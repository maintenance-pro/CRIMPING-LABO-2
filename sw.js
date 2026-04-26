/**
 * LEONI Crimping Lab — Service Worker v3.0 (Ultra-light)
 * Aucun pré-cache au démarrage → pas de violation 'message handler'
 * Cache uniquement les fichiers déjà chargés
 */

const CACHE_NAME = 'leoni-v3';

// Installation : skip immédiat sans pré-cache (évite la violation)
self.addEventListener('install', event => {
  self.skipWaiting();
});

// Activation : nettoyer les anciens caches
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys()
      .then(keys => Promise.all(keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

// Fetch : cache passif (uniquement les requêtes réussies)
self.addEventListener('fetch', event => {
  const { request } = event;
  
  // Ignorer non-GET et URLs spéciales
  if (request.method !== 'GET') return;
  const url = new URL(request.url);
  if (url.protocol !== 'http:' && url.protocol !== 'https:') return;
  
  // Ignorer Firebase RTDB (toujours direct)
  if (url.hostname.includes('firebaseio.com') || url.hostname.includes('firebasedatabase.app')) {
    return; // Le navigateur fait sa requête normalement
  }
  
  // Pour les fichiers statiques : Stale-While-Revalidate ultra simple
  event.respondWith(
    caches.match(request).then(cached => {
      const networkPromise = fetch(request)
        .then(response => {
          // Cache en arrière-plan SI réussi (clone IMMÉDIATEMENT)
          if (response && response.ok && response.type === 'basic') {
            const responseClone = response.clone();
            caches.open(CACHE_NAME)
              .then(cache => cache.put(request, responseClone))
              .catch(() => {});
          }
          return response;
        })
        .catch(() => null);
      
      // Retourne le cache immédiatement si disponible (rapide)
      // Sinon attend le réseau
      return cached || networkPromise || new Response('Offline', { status: 503 });
    })
  );
});

self.addEventListener('message', event => {
  if (event.data === 'SKIP_WAITING') self.skipWaiting();
});
