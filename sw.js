/**
 * LEONI Crimping Lab — Service Worker v4.0
 * Stratégie: Network-First pour JS/CSS/HTML (toujours fraîches),
 * Cache-First pour le reste (polices, images, librairies CDN)
 *
 * Évite les problèmes "ancien code en cache" en privilégiant toujours
 * la version réseau pour les fichiers de l'app.
 */

const CACHE_NAME = 'leoni-v4-fresh';

// Installation : prendre le contrôle immédiatement
self.addEventListener('install', event => {
  self.skipWaiting();
});

// Activation : nettoyer TOUS les anciens caches
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys()
      .then(keys => Promise.all(keys.map(k => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', event => {
  const { request } = event;

  if (request.method !== 'GET') return;
  const url = new URL(request.url);
  if (url.protocol !== 'http:' && url.protocol !== 'https:') return;

  // Ignorer Firebase RTDB (toujours direct)
  if (url.hostname.includes('firebaseio.com') ||
      url.hostname.includes('firebasedatabase.app')) {
    return;
  }

  // ═══ Network-First pour les fichiers de l'app (HTML/JS/CSS) ═══
  // Garantit que les utilisateurs ont TOUJOURS la dernière version
  const isAppFile = url.hostname === self.location.hostname &&
                    (url.pathname.endsWith('.html') ||
                     url.pathname.endsWith('.js') ||
                     url.pathname.endsWith('.css') ||
                     url.pathname.endsWith('/'));

  if (isAppFile) {
    event.respondWith(networkFirst(request));
    return;
  }

  // ═══ Cache-First pour le reste (polices, images, CDN) ═══
  event.respondWith(cacheFirst(request));
});

async function networkFirst(request) {
  try {
    const response = await fetch(request, { cache: 'no-store' });
    if (response && response.ok && response.type === 'basic') {
      const responseClone = response.clone();
      caches.open(CACHE_NAME)
        .then(cache => cache.put(request, responseClone))
        .catch(() => {});
    }
    return response;
  } catch (e) {
    // Hors-ligne : retourner depuis le cache si disponible
    const cached = await caches.match(request);
    if (cached) return cached;
    return new Response('Offline', { status: 503 });
  }
}

async function cacheFirst(request) {
  const cached = await caches.match(request);
  if (cached) return cached;
  try {
    const response = await fetch(request);
    if (response && response.ok && response.type === 'basic') {
      const responseClone = response.clone();
      caches.open(CACHE_NAME)
        .then(cache => cache.put(request, responseClone))
        .catch(() => {});
    }
    return response;
  } catch (e) {
    return new Response('Offline', { status: 503 });
  }
}

self.addEventListener('message', event => {
  if (event.data === 'SKIP_WAITING') self.skipWaiting();
});
