/**
 * LEONI Crimping Lab — Service Worker v2.1
 * Mode hors-ligne pour techniciens terrain
 *
 * RÈGLE CRITIQUE : toujours response.clone() AVANT toute lecture/return,
 * sinon "Response body already used" car une Response ne peut être lue qu'une fois.
 */

const CACHE_VERSION = 'leoni-v2-1';
const STATIC_CACHE  = `${CACHE_VERSION}-static`;
const RUNTIME_CACHE = `${CACHE_VERSION}-runtime`;

const PRECACHE_URLS = [
  './',
  './index.html',
  './admin-dashboard.html',
  './styles.css',
  './app.js'
];

// ── Installation ──
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(STATIC_CACHE)
      .then(cache => cache.addAll(PRECACHE_URLS))
      .then(() => self.skipWaiting())
      .catch(() => self.skipWaiting()) // ne pas bloquer l'install si un fichier manque
  );
});

// ── Activation ──
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys()
      .then(keys => Promise.all(
        keys.filter(k => !k.startsWith(CACHE_VERSION)).map(k => caches.delete(k))
      ))
      .then(() => self.clients.claim())
  );
});

// ── Fetch ──
self.addEventListener('fetch', event => {
  const { request } = event;
  const url = new URL(request.url);

  // Ignorer non-GET et chrome-extension
  if (request.method !== 'GET') return;
  if (url.protocol !== 'http:' && url.protocol !== 'https:') return;

  // Firebase Realtime Database → Network-First
  if (url.hostname.includes('firebaseio.com') || url.hostname.includes('firebasedatabase.app')) {
    event.respondWith(networkFirst(request));
    return;
  }

  // Auth Firebase / gstatic → Stale-While-Revalidate
  if (url.hostname.includes('googleapis.com') || url.hostname.includes('gstatic.com')) {
    event.respondWith(staleWhileRevalidate(request));
    return;
  }

  // Reste (HTML, CSS, JS, images, polices) → Cache-First
  event.respondWith(cacheFirst(request));
});

/* ─────────────────────────────────────────────────────────────
   STRATÉGIES — chaque fonction CLONE la response avant
   de la mettre en cache, et retourne la response originale.
   ───────────────────────────────────────────────────────────── */

async function cacheFirst(request) {
  // 1. Tente le cache
  const cached = await caches.match(request);
  if (cached) return cached;

  // 2. Sinon réseau + put en cache (clone d'abord)
  try {
    const response = await fetch(request);
    if (response && response.ok && response.type !== 'opaque') {
      const responseClone = response.clone();
      caches.open(RUNTIME_CACHE).then(cache => {
        cache.put(request, responseClone).catch(() => {});
      });
    }
    return response;
  } catch (e) {
    // Hors-ligne : page de fallback
    if (request.headers.get('accept')?.includes('text/html')) {
      const fallback = await caches.match('./admin-dashboard.html');
      if (fallback) return fallback;
    }
    return new Response('Hors-ligne', { status: 503, statusText: 'Offline' });
  }
}

async function networkFirst(request) {
  try {
    const response = await fetch(request);
    if (response && response.ok) {
      const responseClone = response.clone();
      caches.open(RUNTIME_CACHE).then(cache => {
        cache.put(request, responseClone).catch(() => {});
      });
    }
    return response;
  } catch (e) {
    const cached = await caches.match(request);
    if (cached) return cached;
    return new Response(JSON.stringify({ offline: true }), {
      status: 503, headers: { 'Content-Type': 'application/json' }
    });
  }
}

async function staleWhileRevalidate(request) {
  const cached = await caches.match(request);

  // Lance le fetch en arrière-plan (clone immédiat)
  const fetchPromise = fetch(request).then(response => {
    if (response && response.ok && response.type !== 'opaque') {
      const responseClone = response.clone();
      caches.open(RUNTIME_CACHE).then(cache => {
        cache.put(request, responseClone).catch(() => {});
      });
    }
    return response;
  }).catch(() => null);

  // Retourne le cache immédiatement si dispo, sinon attend le réseau
  return cached || (await fetchPromise) || new Response('Indisponible', { status: 503 });
}

// ── Skip waiting sur message ──
self.addEventListener('message', event => {
  if (event.data === 'SKIP_WAITING') self.skipWaiting();
});
