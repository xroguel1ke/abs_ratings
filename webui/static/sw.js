// Minimal service worker: makes the web UI installable as an app (no offline cache - the data is always live).
self.addEventListener('install', e => self.skipWaiting());
self.addEventListener('activate', e => self.clients.claim());
self.addEventListener('fetch', () => {});
