# CareerMate Web Chat App – setup

1. Copy into your backend repo (same folder as the existing files, overwrite):
   web_channel.py (new), main.py, whatsapp.py, news_service.py, PYQ.py, books.py, notes_utube.py
   and the folder web_app/ (new, next to main.py).
2. Add `python-multipart` to requirements.txt (needed for file upload).
3. Optional env vars on Render:
   - WEB_CHAT_ACCESS_CODE  -> if set, visitors must enter this code
   - (rate limits have safe defaults; see top of web_channel.py)
4. Deploy, then open  https://<your-render-url>/app/
   On phone: browser menu -> "Add to Home screen" (installable PWA).

Notes
- All 16 commands work through the web app; WhatsApp behaviour is unchanged.
- Web users appear in the admin panel as ids starting with 000.
- Files/images are held in memory with a short TTL (fine on Render free tier; lost on restart).
- External services (news, Unstop, roadmap.sh, YouTube, LLM keys) were not testable offline; they use your existing keys.
