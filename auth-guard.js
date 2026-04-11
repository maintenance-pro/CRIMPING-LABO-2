// =============================================================================
// auth-guard.js — LEONI Crimping-Laboratoire
// Script de protection de route — À inclure en PREMIER dans chaque dashboard
//
// Usage :
//   <script type="module">
//     import { guardRoute } from "./auth-guard.js";
//     guardRoute("admin");   // ou "crimp" ou "labo"
//   </script>
// =============================================================================

import { auth, db }              from "./firebase-init.js";
import { onAuthStateChanged,
         signOut }               from "https://www.gstatic.com/firebasejs/10.12.0/firebase-auth.js";
import { doc, getDoc }           from "https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js";

// ---------------------------------------------------------------------------
// CONSTANTES
// ---------------------------------------------------------------------------

const LOGIN_PAGE   = "index.html";   // page de login
const ROLE_ROUTES  = {
  admin : "admin-dashboard.html",
  crimp : "crimp-dashboard.html",
  labo  : "labo-dashboard.html",
};

// ---------------------------------------------------------------------------
// UTILITAIRES INTERNES
// ---------------------------------------------------------------------------

/** Redirige vers la page de login et arrête le script */
function redirectToLogin(reason = "") {
  if (reason) console.warn("[AuthGuard]", reason);
  sessionStorage.removeItem("userProfile");
  window.location.replace(LOGIN_PAGE);
}

/** Redirige vers le dashboard autorisé de l'utilisateur */
function redirectToAuthorizedDashboard(userRole) {
  const route = ROLE_ROUTES[userRole];
  if (route) {
    console.warn(`[AuthGuard] Accès non autorisé → redirection vers ${route}`);
    window.location.replace(route);
  } else {
    redirectToLogin("Rôle inconnu.");
  }
}

/** Récupère le profil depuis Firestore (source de vérité) */
async function fetchAndVerifyProfile(uid) {
  try {
    const snap = await getDoc(doc(db, "users", uid));
    if (!snap.exists()) return null;
    return snap.data();
  } catch (err) {
    console.error("[AuthGuard] Erreur Firestore :", err);
    return null;
  }
}

/** Affiche ou masque le contenu de la page */
function setPageVisible(visible) {
  document.documentElement.style.visibility = visible ? "visible" : "hidden";
}

// ---------------------------------------------------------------------------
// EXPORT PRINCIPAL : guardRoute(requiredRole)
// ---------------------------------------------------------------------------

/**
 * Protège une page en vérifiant que l'utilisateur connecté a le rôle requis.
 *
 * @param {string} requiredRole  - Rôle attendu : "admin" | "crimp" | "labo"
 * @param {object} options
 * @param {boolean} options.strictFirestore - Si true, vérifie toujours Firestore
 *                                            (plus sûr, mais une requête de plus).
 *                                            Par défaut : false (utilise sessionStorage).
 */
export function guardRoute(requiredRole, { strictFirestore = false } = {}) {
  if (!ROLE_ROUTES[requiredRole]) {
    console.error(`[AuthGuard] Rôle "${requiredRole}" invalide.`);
    return;
  }

  // Cacher immédiatement la page pendant la vérification
  setPageVisible(false);

  onAuthStateChanged(auth, async (user) => {

    // ── 1. Pas de session Firebase → login
    if (!user) {
      redirectToLogin("Aucune session active.");
      return;
    }

    let profile = null;

    // ── 2a. Mode rapide : utiliser le sessionStorage (1 requête Firestore évitée)
    if (!strictFirestore) {
      const cached = sessionStorage.getItem("userProfile");
      if (cached) {
        try { profile = JSON.parse(cached); } catch (_) { profile = null; }
      }
    }

    // ── 2b. Vérification Firestore (si strict ou cache absent)
    if (!profile || strictFirestore) {
      profile = await fetchAndVerifyProfile(user.uid);

      if (!profile) {
        await signOut(auth);
        redirectToLogin("Profil introuvable dans Firestore.");
        return;
      }

      // Mettre à jour le cache
      sessionStorage.setItem("userProfile", JSON.stringify({
        uid         : user.uid,
        displayName : profile.displayName || user.email,
        email       : user.email,
        role        : profile.role,
        matricule   : profile.matricule || null,
      }));
    }

    // ── 3. Vérifier compte actif
    if (profile.active === false) {
      await signOut(auth);
      redirectToLogin("Compte désactivé.");
      return;
    }

    // ── 4. Vérifier le rôle
    if (profile.role !== requiredRole) {
      // Cas spécial : admin peut accéder à toutes les pages
      if (profile.role === "admin") {
        console.info(`[AuthGuard] Admin accède à la page "${requiredRole}" — autorisé.`);
        setPageVisible(true);
        exposeUserProfile(profile);
        return;
      }
      // Mauvais rôle → rediriger vers son propre dashboard
      redirectToAuthorizedDashboard(profile.role);
      return;
    }

    // ── 5. ✅ Accès autorisé
    setPageVisible(true);
    exposeUserProfile(profile);
  });
}

// ---------------------------------------------------------------------------
// EXPOSE LE PROFIL AU RESTE DE LA PAGE
// ---------------------------------------------------------------------------

/**
 * Rend le profil utilisateur accessible via window.__leoniUser
 * et déclenche l'événement "userReady" sur document.
 */
function exposeUserProfile(profile) {
  window.__leoniUser = profile;
  document.dispatchEvent(new CustomEvent("userReady", { detail: profile }));
}

// ---------------------------------------------------------------------------
// HELPER : DÉCONNEXION (réutilisable dans les dashboards)
// ---------------------------------------------------------------------------

export async function logout() {
  try {
    await signOut(auth);
  } finally {
    sessionStorage.removeItem("userProfile");
    window.location.replace(LOGIN_PAGE);
  }
}

// ---------------------------------------------------------------------------
// HELPER : RÉCUPÉRER LE PROFIL COURANT (depuis le cache)
// ---------------------------------------------------------------------------

export function getCurrentUser() {
  try {
    const cached = sessionStorage.getItem("userProfile");
    return cached ? JSON.parse(cached) : null;
  } catch (_) {
    return null;
  }
}
