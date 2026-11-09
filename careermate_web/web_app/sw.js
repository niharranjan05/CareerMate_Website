// CareerMate web app: tiny service worker so the app can be installed and its shell opens offline.
// API calls (/api/...) are never cached.
const CACHE = "careermate-shell-v1";
const SHELL = ["/app/", "/app/logo.png", "/app/manifest.webmanifest", "/app/favicon.png"];
self.addEventListener("install", (e) => { e.waitUntil(caches.open(CACHE).then((c) => c.addAll(SHELL)).then(() => self.skipWaiting())); });
self.addEventListener("activate", (e) => { e.waitUntil(caches.keys().then((ks) => Promise.all(ks.filter((k) => k !== CACHE).map((k) => caches.delete(k)))).then(() => self.clients.claim())); });
self.addEventListener("fetch", (e) => {
  const u = new URL(e.request.url);
  if (e.request.method !== "GET" || u.origin !== location.origin || !u.pathname.startsWith("/app/")) return;
  // network first (so updates show up), fall back to the cached shell when offline
  e.respondWith(fetch(e.request).then((r) => { const copy = r.clone(); caches.open(CACHE).then((c) => c.put(e.request, copy)); return r; }).catch(() => caches.match(e.request).then((m) => m || caches.match("/app/"))));
});
