// Minimal service worker — required by browsers for "Install app" to be offered.
// This dashboard is always meant to show live data, so we deliberately don't
// cache API responses; this just satisfies the installability requirement.
self.addEventListener('install', (event) => {
  self.skipWaiting();
});
self.addEventListener('activate', (event) => {
  self.clients.claim();
});
self.addEventListener('fetch', (event) => {
  event.respondWith(fetch(event.request));
});
