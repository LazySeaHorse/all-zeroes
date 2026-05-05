// Service worker — exists only for PWA installability.
// No caching of API responses; the app always fetches live data.

const SHELL = ['/', '/index.html', '/app.js', '/icon.svg', '/manifest.json'];

self.addEventListener('install', (e) => {
  e.waitUntil(
    caches.open('az-shell-v1').then((c) => c.addAll(SHELL)).then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', (e) => {
  e.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => k !== 'az-shell-v1').map((k) => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (e) => {
  const url = new URL(e.request.url);
  // Only serve shell files from cache; let API requests pass through.
  if (url.origin === self.location.origin && !url.pathname.startsWith('/api/')) {
    e.respondWith(
      caches.match(e.request).then((cached) => cached || fetch(e.request))
    );
  }
});
