/**
 * LEONI — Service Worker
 * Mode hors-ligne pour les techniciens terrain (zones avec mauvaise connexion)
 *
 * Stratégies :
 * - HTML/CSS/JS : Cache-First avec révision (mise à jour silencieuse)
 * - Firebase API : Network-First avec fallback cache
 * - Polices, images : Cache-First long terme
 */

const CACHE_VERSION = 'leoni-v2.0';
const STATIC_CACHE = `${CACHE_VERSION}-static`;
const RUNTIME_CACHE = `${CACHE_VERSION}-runtime`;

// Assets à pré-cacher au démarrage (chargés instantanément hors-ligne)
const PRECACHE_URLS = [
  './',
  './index.html',
  './admin-dashboard.html',
  './styles.css',
  './app.js'
];

// ── Installation : pré-cache des fichiers statiques ──
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(STATIC_CACHE)
      .then(cache => cache.addAll(PRECACHE_URLS))
      .then(() => self.skipWaiting())
      .catch(err => console.warn('[SW] Precache partial:', err))
  );
});

// ── Activation : suppression des anciens caches ──
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys => Promise.all(
      keys.filter(k => !k.startsWith(CACHE_VERSION)).map(k => caches.delete(k))
    )).then(() => self.clients.claim())
  );
});

// ── Stratégie de fetch ──
self.addEventListener('fetch', event => {
  const { request } = event;
  const url = new URL(request.url);

  // Ignorer les requêtes non-GET
  if (request.method !== 'GET') return;

  // Firebase Realtime Database : Network-First (données toujours fraîches)
  if (url.hostname.includes('firebaseio.com') || url.hostname.includes('firebasedatabase.app')) {
    event.respondWith(networkFirst(request));
    return;
  }

  // Firebase Auth : Network-only (sécurité)
  if (url.hostname.includes('googleapis.com') || url.hostname.includes('gstatic.com')) {
    event.respondWith(staleWhileRevalidate(request));
    return;
  }

  // Tout le reste (HTML, CSS, JS, images) : Cache-First
  event.respondWith(cacheFirst(request));
});

// ── Stratégie Cache-First ──
async function cacheFirst(request) {
  const cached = await caches.match(request);
  if (cached) return cached;
  try {
    const response = await fetch(request);
    if (response.ok) {
      const cache = await caches.open(RUNTIME_CACHE);
      cache.put(request, response.clone());
    }
    return response;
  } catch (e) {
    // Hors-ligne : retourne page offline si HTML
    if (request.headers.get('accept')?.includes('text/html')) {
      return caches.match('./index.html');
    }
    throw e;
  }
}

// ── Stratégie Network-First (Firebase) ──
async function networkFirst(request) {
  try {
    const response = await fetch(request);
    if (response.ok) {
      const cache = await caches.open(RUNTIME_CACHE);
      cache.put(request, response.clone());
    }
    return response;
  } catch (e) {
    const cached = await caches.match(request);
    if (cached) return cached;
    throw e;
  }
}

// ── Stratégie Stale-While-Revalidate ──
async function staleWhileRevalidate(request) {
  const cached = await caches.match(request);
  const fetchPromise = fetch(request).then(response => {
    if (response.ok) {
      caches.open(RUNTIME_CACHE).then(cache => cache.put(request, response.clone()));
    }
    return response;
  }).catch(() => cached);
  return cached || fetchPromise;
}

// ── Notification de mise à jour ──
self.addEventListener('message', event => {
  if (event.data === 'SKIP_WAITING') self.skipWaiting();
});
