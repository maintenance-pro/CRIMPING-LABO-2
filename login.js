// =============================================================================
// login.js — LEONI Crimping-Laboratoire
// Authentification Firebase + Redirection selon rôle RBAC
// =============================================================================

import { auth, db }                        from "./firebase-init.js";
import { signInWithEmailAndPassword,
         onAuthStateChanged,
         setPersistence,
         browserLocalPersistence,
         browserSessionPersistence }        from "https://www.gstatic.com/firebasejs/10.12.0/firebase-auth.js";
import { doc, getDoc, serverTimestamp,
         setDoc }                           from "https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js";

// ---------------------------------------------------------------------------
// CONSTANTES
// ---------------------------------------------------------------------------
const ROLE_ROUTES = {
  admin : "admin-dashboard.html",
  crimp : "crimp-dashboard.html",
  labo  : "labo-dashboard.html",
};

const ERROR_MESSAGES = {
  "auth/invalid-email"          : "Adresse e-mail invalide.",
  "auth/user-disabled"          : "Ce compte est désactivé. Contactez l'administrateur.",
  "auth/user-not-found"         : "Aucun compte trouvé avec cet identifiant.",
  "auth/wrong-password"         : "Mot de passe incorrect.",
  "auth/invalid-credential"     : "Identifiants invalides.",
  "auth/too-many-requests"      : "Trop de tentatives. Réessayez dans quelques minutes.",
  "auth/network-request-failed" : "Erreur réseau. Vérifiez votre connexion.",
  "role/not-found"              : "Rôle utilisateur introuvable. Contactez l'administrateur.",
  "role/inactive"               : "Compte inactif. Contactez l'administrateur.",
  "role/unknown"                : "Rôle non reconnu. Contactez l'administrateur.",
};

// ---------------------------------------------------------------------------
// UTILITAIRES
// ---------------------------------------------------------------------------

/** Affiche un message d'erreur dans l'UI */
function showError(message) {
  const el = document.getElementById("login-error");
  if (!el) return;
  el.textContent = message;
  el.hidden = false;
  el.setAttribute("role", "alert");
}

/** Cache le message d'erreur */
function clearError() {
  const el = document.getElementById("login-error");
  if (el) { el.textContent = ""; el.hidden = true; }
}

/** Active / désactive l'état de chargement du bouton */
function setLoading(isLoading) {
  const btn  = document.getElementById("btn-login");
  const spin = document.getElementById("login-spinner");
  if (!btn) return;
  btn.disabled = isLoading;
  btn.textContent = isLoading ? "Connexion…" : "Se connecter";
  if (spin) spin.hidden = !isLoading;
}

/** Récupère le profil utilisateur depuis Firestore */
async function fetchUserProfile(uid) {
  const ref  = doc(db, "users", uid);
  const snap = await getDoc(ref);
  if (!snap.exists()) return null;
  return snap.data();
}

/** Met à jour la date de dernière connexion */
async function updateLastLogin(uid) {
  try {
    await setDoc(doc(db, "users", uid), { lastLoginAt: serverTimestamp() }, { merge: true });
  } catch (_) { /* non-bloquant */ }
}

/** Redirige vers le dashboard correspondant au rôle */
function redirectByRole(role) {
  const route = ROLE_ROUTES[role];
  if (!route) {
    showError(ERROR_MESSAGES["role/unknown"]);
    return false;
  }
  window.location.replace(route);
  return true;
}

// ---------------------------------------------------------------------------
// LOGIQUE PRINCIPALE DE CONNEXION
// ---------------------------------------------------------------------------

async function handleLogin(event) {
  event.preventDefault();
  clearError();
  setLoading(true);

  const email      = document.getElementById("login-email")?.value.trim()    ?? "";
  const password   = document.getElementById("login-password")?.value         ?? "";
  const rememberMe = document.getElementById("login-remember")?.checked       ?? false;

  if (!email || !password) {
    showError("Veuillez remplir tous les champs.");
    setLoading(false);
    return;
  }

  try {
    // 1. Persistance selon "Se souvenir de moi"
    const persistence = rememberMe ? browserLocalPersistence : browserSessionPersistence;
    await setPersistence(auth, persistence);

    // 2. Authentification Firebase
    const credential = await signInWithEmailAndPassword(auth, email, password);
    const user       = credential.user;

    // 3. Récupérer le profil + rôle depuis Firestore
    const profile = await fetchUserProfile(user.uid);

    if (!profile) {
      await auth.signOut();
      showError(ERROR_MESSAGES["role/not-found"]);
      setLoading(false);
      return;
    }

    if (profile.active === false) {
      await auth.signOut();
      showError(ERROR_MESSAGES["role/inactive"]);
      setLoading(false);
      return;
    }

    const role = profile.role;

    if (!ROLE_ROUTES[role]) {
      await auth.signOut();
      showError(ERROR_MESSAGES["role/unknown"]);
      setLoading(false);
      return;
    }

    // 4. Mettre à jour lastLoginAt (non-bloquant)
    updateLastLogin(user.uid);

    // 5. Sauvegarder le profil en sessionStorage pour les guards
    sessionStorage.setItem("userProfile", JSON.stringify({
      uid         : user.uid,
      displayName : profile.displayName || user.email,
      email       : user.email,
      role        : role,
      matricule   : profile.matricule || null,
    }));

    // 6. Redirection
    redirectByRole(role);

  } catch (err) {
    console.error("[Login] Erreur :", err.code, err.message);
    const message = ERROR_MESSAGES[err.code] ?? `Erreur inattendue (${err.code})`;
    showError(message);
    setLoading(false);
  }
}

// ---------------------------------------------------------------------------
// VÉRIFICATION SI DÉJÀ CONNECTÉ (évite d'afficher le login inutilement)
// ---------------------------------------------------------------------------

function checkAlreadyLoggedIn() {
  const loader = document.getElementById("login-preloader");
  if (loader) loader.hidden = false;

  onAuthStateChanged(auth, async (user) => {
    if (user) {
      // Utilisateur déjà connecté → récupère son rôle et redirige
      const profile = await fetchUserProfile(user.uid);
      if (profile?.active !== false && ROLE_ROUTES[profile?.role]) {
        redirectByRole(profile.role);
        return;
      }
    }
    // Non connecté → afficher le formulaire
    if (loader) loader.hidden = true;
    const card = document.getElementById("login-card");
    if (card) card.hidden = false;
  });
}

// ---------------------------------------------------------------------------
// INITIALISATION DES ÉVÉNEMENTS
// ---------------------------------------------------------------------------

document.addEventListener("DOMContentLoaded", () => {
  // Vérifier si déjà connecté
  checkAlreadyLoggedIn();

  // Formulaire de connexion
  const form = document.getElementById("form-login");
  if (form) form.addEventListener("submit", handleLogin);

  // Toggle visibilité mot de passe
  const toggleBtn = document.getElementById("btn-toggle-pass");
  const passInput = document.getElementById("login-password");
  if (toggleBtn && passInput) {
    toggleBtn.addEventListener("click", () => {
      const isVisible    = passInput.type === "text";
      passInput.type     = isVisible ? "password" : "text";
      toggleBtn.textContent = isVisible ? "👁" : "🙈";
      toggleBtn.setAttribute("aria-label", isVisible ? "Afficher le mot de passe" : "Masquer le mot de passe");
    });
  }
});
