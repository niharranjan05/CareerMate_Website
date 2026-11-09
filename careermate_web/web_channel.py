"""web_channel.py  -  CareerMate WEB CHAT APP (a second front door to the same bot)

The bot's 16 commands were written for WhatsApp: every handler sends its answers with `wa.text(phone, ...)`, a few
modules send PDFs/images straight to the WhatsApp Cloud API. This module lets a browser talk to the SAME code:

    browser  --POST /api/web/send-->  process_message()   (the normal WhatsApp message handler, unchanged)
    handlers --wa.text / documents--> web_channel.push_*  (instead of the WhatsApp API, because the "phone" is a web id)
    browser  <--GET /api/web/poll---  per-session outbox  (long polling, ~20 s)

So the web app has exactly the same commands, quizzes, resume review, PDF chat, YouTube notes ... as WhatsApp, the same
database and the same analytics (the admin panel shows web users like any other user).

How a web user is identified
    The browser creates a random secret `sid` (stored in localStorage). The server turns it into a "phone" that can never
    be a real number:  "000" + 12 digits (E.164 numbers never start with 0).  `is_web(phone)` is therefore a prefix check, and
    WhatsApp users are never affected.

Endpoints  (all under /api/web)
    GET  /config                      -> {"enabled": true, "access_code": false, "max_upload_mb": 10, ...}
    POST /send    {sid, text}         -> queues the message for the bot
    POST /upload  (multipart: sid, file[, caption])   -> a resume / PDF for /send, /ats, /chat_pdf
    GET  /poll?sid=..&after=N&wait=20 -> {"items": [...], "busy": bool, "last": N}   (long poll)
    GET  /file/{id}                   -> a PDF / image the bot produced (unguessable id, deleted after 2 h)
  and the app itself:  GET /app  (+ /app/manifest.webmanifest, /app/sw.js, icons)

Environment variables (all optional)
    WEB_CHAT_ENABLED      1 (default). Set 0 to switch the web app off.
    WEB_CHAT_ACCESS_CODE  if set, the app asks for this code before the first message (keeps strangers from spending your AI quota).
    WEB_CHAT_PER_MIN      messages per minute per browser session (default 20)
    WEB_CHAT_PER_IP_MIN   messages per minute per IP address (default 60)
    WEB_CHAT_PER_DAY      messages per day per browser session (default 300)
    WEB_MAX_UPLOAD_MB     largest accepted upload (default 10)
    WEB_MAX_FILE_MB       largest file the bot may hand to the browser (default 40)
    WEB_FILE_TTL_MIN      how long files stay downloadable (default 120)
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import os
import re
import secrets
import tempfile
import threading
import time
from collections import deque
from pathlib import Path

from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse, Response

log = logging.getLogger("career_bot.web")

ENABLED = os.getenv("WEB_CHAT_ENABLED", "1").strip().lower() not in ("0", "false", "no", "off")
ACCESS_CODE = os.getenv("WEB_CHAT_ACCESS_CODE", "").strip()
PER_MIN = int(os.getenv("WEB_CHAT_PER_MIN", "20"))
PER_IP_MIN = int(os.getenv("WEB_CHAT_PER_IP_MIN", "60"))
PER_DAY = int(os.getenv("WEB_CHAT_PER_DAY", "300"))
MAX_UPLOAD = int(float(os.getenv("WEB_MAX_UPLOAD_MB", "10")) * 1024 * 1024)
MAX_FILE = int(float(os.getenv("WEB_MAX_FILE_MB", "40")) * 1024 * 1024)
FILE_TTL = float(os.getenv("WEB_FILE_TTL_MIN", "120")) * 60
SESSION_TTL = 24 * 3600
MAX_TEXT = 2000
MAX_ITEMS = 300                    # outbox length kept per session
ALLOWED_UPLOAD = (".pdf", ".docx", ".txt")

APP_DIR = Path(__file__).resolve().parent / "web_app"
FILES_DIR = Path(tempfile.gettempdir()) / "careermate_web_files"
FILES_DIR.mkdir(parents=True, exist_ok=True)

router = APIRouter()

_loop: asyncio.AbstractEventLoop | None = None
_processor = None                                   # main.process_message
_lock = threading.RLock()
_sessions: dict[str, "Session"] = {}
_files: dict[str, dict] = {}                        # file id -> {path, name, mime, size, phone, exp}
_hits_sid: dict[str, deque] = {}
_hits_ip: dict[str, deque] = {}
_day: dict[str, tuple[str, int]] = {}
_tasks: set[asyncio.Task] = set()
_janitor_started = False


# ------------------------------------------------------------------ identity
def is_web(phone) -> bool:
    """True for ids made by this module ("000" + 12 digits). Real WhatsApp numbers never start with 0."""
    p = str(phone or "")
    return len(p) == 15 and p.startswith("000") and p.isdigit()


def phone_for(sid: str) -> str:
    h = hashlib.sha256(("careermate-web:" + sid).encode()).hexdigest()
    return "000" + str(int(h[:16], 16)).zfill(12)[-12:]


_SID_RE = re.compile(r"^[A-Za-z0-9_-]{24,64}$")


def _phone(sid: str) -> str:
    if not _SID_RE.match(sid or ""):
        raise HTTPException(status_code=400, detail="bad session")
    return phone_for(sid)


# ------------------------------------------------------------------ session / outbox
class Session:
    def __init__(self, phone: str):
        self.phone = phone
        self.items: list[dict] = []
        self.seq = 0
        self.pending = 0                            # messages being processed right now
        self.done = 0                               # messages finished so far (lets a poll end as soon as the bot is idle again)
        self.event = asyncio.Event()
        self.seen = time.time()


def _session(phone: str) -> Session:
    with _lock:
        s = _sessions.get(phone)
        if s is None:
            s = _sessions[phone] = Session(phone)
        s.seen = time.time()
        return s


def _wake(s: Session):
    s.event.set()


def _push(phone: str, item: dict) -> None:
    """Thread-safe: the PDF senders run in worker threads (asyncio.to_thread)."""
    s = _session(phone)
    with _lock:
        s.seq += 1
        item = {"id": s.seq, "ts": int(time.time() * 1000), **item}
        s.items.append(item)
        if len(s.items) > MAX_ITEMS:
            del s.items[: len(s.items) - MAX_ITEMS]
    if _loop is not None:
        try:
            _loop.call_soon_threadsafe(_wake, s)
        except RuntimeError:
            pass


def push_text(phone: str, body: str) -> None:
    _push(phone, {"type": "text", "text": str(body)})


def _store_file(phone: str, data: bytes, name: str, mime: str) -> str | None:
    if not data or len(data) > MAX_FILE:
        return None
    fid = secrets.token_urlsafe(18)
    path = FILES_DIR / fid
    path.write_bytes(data)
    with _lock:
        _files[fid] = {"path": str(path), "name": name or "file", "mime": mime or "application/octet-stream",
                       "size": len(data), "phone": phone, "exp": time.time() + FILE_TTL}
    return fid


def push_document(phone: str, data: bytes, filename: str, mime: str = "application/pdf", caption: str = "") -> None:
    fid = _store_file(phone, data, filename, mime)
    if not fid:
        push_text(phone, f"⚠️ *{filename}* is too large to send in the web app. Try the WhatsApp bot for this one.")
        return
    _push(phone, {"type": "document", "file": fid, "name": filename, "mime": mime, "size": len(data), "caption": (caption or "")[:1000]})


def push_image(phone: str, data: bytes | None = None, mime: str = "image/jpeg", caption: str = "", link: str = "") -> None:
    if data:
        fid = _store_file(phone, data, "image", mime)
        if fid:
            _push(phone, {"type": "image", "file": fid, "caption": (caption or "")[:1000]})
            return
    if link:
        _push(phone, {"type": "image", "url": link, "caption": (caption or "")[:1000]})
    else:
        push_text(phone, caption)


def read_upload(media_id: str) -> bytes:
    """`wa.download("web:<id>")` -> the bytes the browser uploaded."""
    fid = media_id.split(":", 1)[1] if ":" in media_id else media_id
    meta = _files.get(fid)
    if not meta:
        raise FileNotFoundError("upload expired")
    return Path(meta["path"]).read_bytes()


class OkResponse:
    """What the WhatsApp class returns for a web recipient, so callers that look at `.status_code` keep working."""
    status_code = 200
    text = "ok"

    def json(self):
        return {"web": True}


# ------------------------------------------------------------------ housekeeping
def _cleanup():
    now = time.time()
    with _lock:
        for fid, m in list(_files.items()):
            if m["exp"] < now:
                _files.pop(fid, None)
                try:
                    os.remove(m["path"])
                except OSError:
                    pass
        for ph, s in list(_sessions.items()):
            if now - s.seen > SESSION_TTL and s.pending == 0:
                _sessions.pop(ph, None)
        for d in (_hits_sid, _hits_ip):
            for k, q in list(d.items()):
                while q and now - q[0] > 60:
                    q.popleft()
                if not q:
                    d.pop(k, None)


async def _janitor():
    while True:
        await asyncio.sleep(600)
        try:
            _cleanup()
        except Exception:
            log.exception("web cleanup failed")


def init(processor, loop: asyncio.AbstractEventLoop) -> None:
    """Called once from main.py's lifespan."""
    global _loop, _processor, _janitor_started
    _loop, _processor = loop, processor
    if not _janitor_started:
        _janitor_started = True
        loop.create_task(_janitor())
    for old in FILES_DIR.glob("*"):                 # leftovers of a previous run
        try:
            old.unlink()
        except OSError:
            pass
    log.info("Web chat app is %s (open /app)%s", "ON" if ENABLED else "OFF", " - access code required" if ACCESS_CODE else "")


# ------------------------------------------------------------------ guards
def _client_ip(request: Request) -> str:
    fwd = request.headers.get("x-forwarded-for", "")
    return (fwd.split(",")[0].strip() if fwd else (request.client.host if request.client else "?")) or "?"


def _hit(table: dict, key: str, limit: int) -> bool:
    now = time.time()
    with _lock:
        q = table.setdefault(key, deque())
        while q and now - q[0] > 60:
            q.popleft()
        if len(q) >= limit:
            return False
        q.append(now)
        return True


def _guard(request: Request, phone: str, sid: str):
    if not ENABLED:
        raise HTTPException(status_code=404, detail="web chat is switched off")
    if ACCESS_CODE:
        given = request.headers.get("x-access-code", "")
        if not hmac.compare_digest(given.encode(), ACCESS_CODE.encode()):
            raise HTTPException(status_code=401, detail="access code required")
    if not _hit(_hits_sid, sid, PER_MIN) or not _hit(_hits_ip, _client_ip(request), PER_IP_MIN):
        raise HTTPException(status_code=429, detail="You are sending messages too fast. Please wait a moment.")
    today = time.strftime("%Y-%m-%d")
    with _lock:
        d, n = _day.get(sid, (today, 0))
        if d != today:
            n = 0
        if n >= PER_DAY:
            raise HTTPException(status_code=429, detail="Daily message limit reached for this browser. Please come back tomorrow.")
        _day[sid] = (today, n + 1)


async def _run(phone: str, message: dict):
    s = _session(phone)
    try:
        await _processor(message)
    except Exception:
        log.exception("web message failed")
        push_text(phone, "⚠️ Something went wrong processing that. Please try again.")
    finally:
        s.pending = max(0, s.pending - 1)
        s.done += 1
        _wake(s)


def _submit(phone: str, message: dict):
    _session(phone).pending += 1                    # counted before the task starts, so the browser never sees "idle" too early
    t = asyncio.create_task(_run(phone, message))
    _tasks.add(t)
    t.add_done_callback(_tasks.discard)


# ------------------------------------------------------------------ API
@router.get("/api/web/config")
async def config():
    return {"enabled": ENABLED, "access_code": bool(ACCESS_CODE), "max_upload_mb": MAX_UPLOAD // (1024 * 1024),
            "max_text": MAX_TEXT, "uploads": list(ALLOWED_UPLOAD)}


@router.post("/api/web/send")
async def send(request: Request):
    body = await request.json()
    sid = str(body.get("sid", ""))
    phone = _phone(sid)
    text = str(body.get("text", "")).strip()
    if not text:
        raise HTTPException(status_code=400, detail="empty message")
    if len(text) > MAX_TEXT:
        raise HTTPException(status_code=413, detail=f"Message too long (max {MAX_TEXT} characters).")
    _guard(request, phone, sid)
    mid = "web." + secrets.token_hex(10)
    _session(phone)
    _submit(phone, {"from": phone, "id": mid, "type": "text", "text": {"body": text}})
    return {"ok": True, "id": mid}


@router.post("/api/web/upload")
async def upload(request: Request, sid: str = Form(...), file: UploadFile = File(...), caption: str = Form("")):
    phone = _phone(sid)
    name = re.sub(r"[^\w .()-]+", "_", (file.filename or "document.pdf"))[:120]
    if not name.lower().endswith(ALLOWED_UPLOAD):
        raise HTTPException(status_code=415, detail="Please upload a PDF, DOCX or TXT file.")
    _guard(request, phone, sid)
    data = await file.read(MAX_UPLOAD + 1)
    if len(data) > MAX_UPLOAD:
        raise HTTPException(status_code=413, detail=f"File too large (max {MAX_UPLOAD // (1024 * 1024)} MB).")
    if not data:
        raise HTTPException(status_code=400, detail="empty file")
    fid = _store_file(phone, data, name, file.content_type or "application/octet-stream")
    mid = "web." + secrets.token_hex(10)
    _session(phone)
    _submit(phone, {"from": phone, "id": mid, "type": "document",
                    "document": {"id": f"web:{fid}", "filename": name, "caption": caption[:500] or None}})
    return {"ok": True, "id": mid, "name": name, "size": len(data)}


@router.get("/api/web/poll")
async def poll(sid: str, after: int = 0, wait: float = 20.0):
    phone = _phone(sid)
    s = _session(phone)
    deadline = time.monotonic() + max(0.0, min(wait, 25.0))
    done0 = s.done
    while True:
        s.event.clear()
        with _lock:
            items = [i for i in s.items if i["id"] > after]
            last = s.seq
        if items or time.monotonic() >= deadline or (s.done != done0 and s.pending == 0):
            break
        try:
            await asyncio.wait_for(s.event.wait(), max(0.05, deadline - time.monotonic()))
        except asyncio.TimeoutError:
            break
    if items:                                       # let a burst of messages finish landing so the browser gets them together
        await asyncio.sleep(0.15)
        with _lock:
            items = [i for i in s.items if i["id"] > after]
            last = s.seq
    return JSONResponse({"items": items, "busy": s.pending > 0, "last": last}, headers={"Cache-Control": "no-store"})


@router.get("/api/web/file/{fid}")
async def get_file(fid: str, dl: int = 0):
    m = _files.get(fid)
    if not m or not Path(m["path"]).exists():
        raise HTTPException(status_code=404, detail="This file has expired. Please run the command again.")
    disp = "attachment" if dl else "inline"
    safe = re.sub(r'[^\w .()-]+', "_", m["name"])
    return FileResponse(m["path"], media_type=m["mime"], headers={"Content-Disposition": f'{disp}; filename="{safe}"', "Cache-Control": "private, max-age=3600",
                                                                   "X-Content-Type-Options": "nosniff"})


# ------------------------------------------------------------------ the app itself (static files)
_TYPES = {".html": "text/html; charset=utf-8", ".js": "application/javascript", ".webmanifest": "application/manifest+json",
          ".png": "image/png", ".svg": "image/svg+xml", ".ico": "image/x-icon", ".css": "text/css"}


def _static(name: str):
    if not ENABLED:
        raise HTTPException(status_code=404)
    p = (APP_DIR / (name or "index.html")).resolve()
    if APP_DIR.resolve() not in p.parents and p != APP_DIR.resolve() or not p.is_file():
        raise HTTPException(status_code=404)
    headers = {"Cache-Control": "no-cache" if p.suffix in (".html", ".js", ".webmanifest") else "public, max-age=86400"}
    if p.name == "sw.js":
        headers["Service-Worker-Allowed"] = "/app/"
    return FileResponse(str(p), media_type=_TYPES.get(p.suffix, "application/octet-stream"), headers=headers)


@router.get("/app")
async def app_index_noslash():
    return Response(status_code=307, headers={"Location": "/app/"})


@router.get("/app/")
async def app_index():
    return _static("index.html")


@router.get("/app/{name:path}")
async def app_file(name: str):
    return _static(name)
