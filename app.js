/* ============================================================================
   LEONI · BON D'INTERVENTION SERTISSAGE
   app.js — Logique principale (Firebase Auth + Realtime DB + Storage)
   ============================================================================ */

(function () {
  'use strict';

  /* ==========================================================================
     1. CONFIGURATION FIREBASE
     ──────────────────────────────────────────────────────────────────────────
     ✅ COMMENT OBTENIR TES VALEURS :
        1. Va sur https://console.firebase.google.com
        2. Sélectionne ton projet (ou crée-le : "leoni-sertissage-lab")
        3. Clique sur ⚙️ Paramètres du projet → onglet "Général"
        4. Descends jusqu'à "Tes applications" → clique sur l'icône </> (Web)
        5. Copie-colle les 7 valeurs ci-dessous

     ⚠️ NE PAS oublier d'activer dans Firebase Console :
        • Authentication → Fournisseurs → Email/Mot de passe ✅
        • Realtime Database → Créer une base de données ✅
        • Storage → Commencer ✅
     ========================================================================== */
  const FIREBASE_CONFIG = {
    apiKey:            "AIzaSyBIW4I1DRqyqcmNEdajXbptQ5-RWFIG1V4",
    authDomain:        "leoni-sertissage-labo.firebaseapp.com",
    databaseURL:       "https://leoni-sertissage-labo-default-rtdb.europe-west1.firebasedatabase.app",
    projectId:         "leoni-sertissage-labo",
    storageBucket:     "leoni-sertissage-labo.firebasestorage.app",
    messagingSenderId: "432527114405",
    appId:             "1:432527114405:web:b141f6ae74f11b1cbb0d4c",
    measurementId:     "G-5YSFB1HGC8"
  };

  // Tolérances par défaut pour le calcul Cm/Cmk (à ajuster par outil/produit)
  const DEFAULT_TOLERANCES = {
    hauteurIsolant: { min: 1.40, max: 1.70 },
    effort:         { min: 40,   max: 70   }
  };

  /* ==========================================================================
     2. ÉTAT GLOBAL
     ========================================================================== */
  const state = {
    user: null,           // Firebase user
    profile: null,        // /users/{uid}
    role: 'viewer',
    tools: {},            // catalogue outils {id: {...}}
    interventions: {},    // tous les bons en cache
    currentInterventionId: null,
    currentView: 'login',
    filters: {
      tool: '', from: '', to: '', type: '', status: '', tech: ''
    },
    pagination: { page: 1, pageSize: 50 },
    sort: { field: 'numBon', dir: 'desc' },
    listeners: [],        // unsubscribe functions
    stock: {},            // stock items
    stockMovements: [],   // movements log
  };

  /* ==========================================================================
     SESSION TIMEOUT — déconnexion auto après 30 min d'inactivité
     ========================================================================== */
  const SESSION_TIMEOUT_MS = 30 * 60 * 1000;
  const SESSION_WARN_MS    = 28 * 60 * 1000;
  let _sessionTimer, _sessionWarnTimer;

  function resetSessionTimer() {
    clearTimeout(_sessionTimer);
    clearTimeout(_sessionWarnTimer);
    const warnModal = document.getElementById('modal-session-warn');
    if (warnModal) warnModal.hidden = true;
    _sessionWarnTimer = setTimeout(() => {
      const m = document.getElementById('modal-session-warn');
      if (m) { m.hidden = false; let s = 120; const el = document.getElementById('session-countdown');
        const iv = setInterval(() => { s--; if (el) el.textContent = s; if (s <= 0) clearInterval(iv); }, 1000); }
    }, SESSION_WARN_MS);
    _sessionTimer = setTimeout(async () => {
      const m = document.getElementById('modal-session-warn');
      if (m) m.hidden = true;
      if (typeof fbAuth !== 'undefined' && state.user) {
        await fbAuth.signOut(); ui.toast('Session expirée. Reconnectez-vous.', 'warn');
      }
    }, SESSION_TIMEOUT_MS);
  }

  function initSessionTimeout() {
    if (!state.user) return;
    ['click','keydown','mousemove','touchstart'].forEach(e =>
      document.addEventListener(e, resetSessionTimer, { passive: true }));
    resetSessionTimer();
  }

  function clearSessionTimeout() {
    clearTimeout(_sessionTimer); clearTimeout(_sessionWarnTimer);
  }


  /* ==========================================================================
     3. UTILS
     ========================================================================== */
  const $  = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

  const fmt = {
    date: (ts) => {
      if (!ts) return '—';
      const d = new Date(ts);
      return d.toLocaleDateString('fr-FR', { day: '2-digit', month: '2-digit', year: 'numeric' });
    },
    dateTime: (ts) => {
      if (!ts) return '—';
      const d = new Date(ts);
      return d.toLocaleString('fr-FR', { dateStyle: 'short', timeStyle: 'short' });
    },
    number: (n, decimals = 0) => {
      if (n == null || isNaN(n)) return '—';
      return Number(n).toLocaleString('fr-FR', { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
    },
    duration: (ms) => {
      if (!ms) return '—';
      const h = Math.floor(ms / 3600000);
      const m = Math.floor((ms % 3600000) / 60000);
      return h > 0 ? `${h}h ${m}min` : `${m}min`;
    },
    statusLabel: (s) => ({
      draft:     'Brouillon',
      submitted: 'En attente labo',
      validated: 'Validé',
      rejected:  'Refusé',
      cancelled: 'Annulé'
    }[s] || s)
  };

  const debounce = (fn, wait = 300) => {
    let t;
    return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), wait); };
  };

  /* ==========================================================================
     4. FIREBASE INIT
     ========================================================================== */
  let fbApp, fbAuth, fbDb, fbStorage;

  function initFirebase() {
    if (typeof firebase === 'undefined') {
      console.error('Firebase SDK manquant. Vérifie les <script> dans index.html.');
      ui.toast('Erreur : Firebase non chargé', 'danger');
      return;
    }
    fbApp     = firebase.initializeApp(FIREBASE_CONFIG);
    fbAuth    = firebase.auth();
    fbDb      = firebase.database();
    fbStorage = firebase.storage();
    console.log('🔥 Firebase initialisé');
  }

  /* ==========================================================================
     5. UI HELPERS (toast, modal, loader)
     ========================================================================== */
  const ui = {
    toast(message, type = 'info', duration = 4000) {
      const container = $('#toast-container');
      if (!container) return;
      const t = document.createElement('div');
      t.className = `toast toast--${type}`;
      t.innerHTML = `<span>${this.escape(message)}</span>`;
      container.appendChild(t);
      setTimeout(() => {
        t.style.animation = 'toastIn 200ms reverse';
        setTimeout(() => t.remove(), 200);
      }, duration);
    },

    confirm(title, message) {
      return new Promise((resolve) => {
        const modal = $('#modal-confirm');
        $('#confirm-title').textContent = title;
        $('#confirm-message').textContent = message;
        modal.hidden = false;
        const ok = $('#confirm-ok');
        const close = (val) => {
          modal.hidden = true;
          ok.removeEventListener('click', onOk);
          modal.querySelectorAll('[data-close]').forEach(el => el.removeEventListener('click', onCancel));
          resolve(val);
        };
        const onOk = () => close(true);
        const onCancel = () => close(false);
        ok.addEventListener('click', onOk);
        modal.querySelectorAll('[data-close]').forEach(el => el.addEventListener('click', onCancel));
      });
    },

    showLoader(text = 'Chargement…') {
      $('#loader-text').textContent = text;
      $('#loader').hidden = false;
    },
    hideLoader() { $('#loader').hidden = true; },

    escape(str) {
      const div = document.createElement('div');
      div.textContent = String(str ?? '');
      return div.innerHTML;
    },

    showView(viewName) {
      $$('[data-view]').forEach(el => { el.hidden = (el.dataset.view !== viewName); });
      $$('.sidebar__link').forEach(el => {
        el.classList.toggle('sidebar__link--active', el.dataset.nav === viewName);
      });
      state.currentView = viewName;
      window.location.hash = viewName;
    },

    applyRoleVisibility() {
      $$('[data-requires-role]').forEach(el => {
        const allowed = el.dataset.requiresRole.split(',').map(s => s.trim());
        el.hidden = !allowed.includes(state.role);
      });
    }
  };

  /* ==========================================================================
     6. AUTH MODULE
     ========================================================================== */
  const auth = {
    init() {
      $('#form-login').addEventListener('submit', this.handleLogin.bind(this));
      $('#btn-toggle-pass').addEventListener('click', () => {
        const inp = $('#login-password');
        inp.type = inp.type === 'password' ? 'text' : 'password';
      });
      fbAuth.onAuthStateChanged(this.handleAuthChange.bind(this));
    },

    async handleLogin(e) {
      e.preventDefault();
      const email = $('#login-email').value.trim();
      const password = $('#login-password').value;
      const errEl = $('#login-error');
      errEl.hidden = true;
      ui.showLoader('Connexion…');
      try {
        await fbAuth.signInWithEmailAndPassword(email, password);
      } catch (err) {
        errEl.textContent = this.translateError(err.code);
        errEl.hidden = false;
        ui.hideLoader();
      }
    },

    async handleAuthChange(user) {
      if (user) {
        state.user = user;
        // Charger profil
        const snap = await fbDb.ref(`users/${user.uid}`).once('value');
        let profile = snap.val();
        // Si profil inexistant, créer un profil minimal (premier login)
        if (!profile) {
          // Aucun profil → déconnecter et afficher erreur
          await fbAuth.signOut();
          const errEl = document.getElementById('login-error');
          if (errEl) {
            errEl.textContent = 'Compte non configuré. Contactez l\'administrateur.';
            errEl.hidden = false;
          }
          ui.hideLoader();
          return;
        }
        if (!profile.active) {
          ui.toast('Compte désactivé. Contacte l\'administrateur.', 'danger');
          await fbAuth.signOut();
          return;
        }
        state.profile = profile;
        state.role = profile.role || 'viewer';

        // Maj dernière connexion
        fbDb.ref(`users/${user.uid}/lastLoginAt`).set(Date.now());

        // UI
        $('#view-login').hidden = true;
        $('#shell').hidden = false;
        this.updateUserBadge();
        ui.applyRoleVisibility();
        await app.loadInitialData();
        router.handleHash();
        ui.hideLoader();
        ui.toast(`Bienvenue ${profile.displayName}`, 'success');
        initSessionTimeout();
      } else {
        state.user = null;
        state.profile = null;
        state.role = 'viewer';
        app.detachListeners();
        $('#view-login').hidden = false;
        $('#shell').hidden = true;
      }
    },

    updateUserBadge() {
      const p = state.profile;
      $('#user-name').textContent = p.displayName || '—';
      $('#user-role').textContent = p.role || '—';
      $('#user-avatar').textContent = (p.displayName || '?').charAt(0).toUpperCase();
      $('#hub-greeting').textContent = `Bienvenue ${p.displayName} — ${this.roleLabel(p.role)}`;
    },

    roleLabel(role) {
      return ({
        admin:            'Administrateur',
        super_admin:      'Super Admin',
        responsable:      'Responsable Maintenance',
        crimp:            'Technicien Crimping',
        crimping:         'Technicien Crimping',
        labo:             'Technicien Labo',
        viewer:           'Visiteur'
      }[role] || role);
    },

    translateError(code) {
      return ({
        'auth/invalid-email':       'Email invalide',
        'auth/user-disabled':       'Compte désactivé',
        'auth/user-not-found':      'Utilisateur introuvable',
        'auth/wrong-password':      'Mot de passe incorrect',
        'auth/invalid-credential':  'Identifiants invalides',
        'auth/too-many-requests':   'Trop de tentatives. Réessaie plus tard.',
        'auth/network-request-failed': 'Erreur réseau'
      }[code] || `Erreur : ${code}`);
    },

    async logout() {
      const ok = await ui.confirm('Déconnexion', 'Veux-tu vraiment te déconnecter ?');
      if (ok) {
        await fbAuth.signOut();
        ui.toast('Déconnecté', 'info');
      }
    },

    can(action) {
      const perms = {
        admin:       ['*'],
        super_admin: ['*'],
        responsable: ['intervention.create','intervention.edit','intervention.delete','intervention.validate','intervention.reject','catalog.edit','user.read'],
        crimp:       ['intervention.create','intervention.edit'],
        crimping:    ['intervention.create','intervention.edit'],
        labo:        ['intervention.validate','intervention.reject','intervention.editLab'],
        magasinier:  ['stock.read','stock.write','stock.import','intervention.read','perf.read'],
        viewer:      []
      };
      const userPerms = perms[state.role] || [];
      return userPerms.includes('*') || userPerms.includes(action);
    }
  };

  /* ==========================================================================
     7. DATABASE MODULE (CRUD)
     ========================================================================== */
  const db = {
    /* --- counters --- */
    async getNextReg() {
      const ref = fbDb.ref('counters/registre');
      const result = await ref.transaction(current => (current || 0) + 1);
      return result.snapshot.val();
    },

    /* --- tools --- */
    listenTools() {
      const ref = fbDb.ref('tools');
      const handler = (snap) => {
        state.tools = snap.val() || {};
        views.intervention.refreshToolDatalist();
        if (state.currentView === 'catalog') views.catalog.render();
      };
      ref.on('value', handler);
      state.listeners.push(() => ref.off('value', handler));
    },

    async saveTool(tool) {
      const id = tool.id || (tool.outilId || '').toLowerCase().replace(/[^a-z0-9]/g, '-');
      tool.id = id;
      tool.updatedAt = Date.now();
      tool.createdAt = tool.createdAt || Date.now();
      await fbDb.ref(`tools/${id}`).set(tool);
      return id;
    },

    async deleteTool(id) {
      await fbDb.ref(`tools/${id}`).remove();
    },

    /* --- interventions --- */
    listenInterventions() {
      const ref = fbDb.ref('interventions').orderByChild('numBon');
      const handler = (snap) => {
        state.interventions = snap.val() || {};
        views.hub.render();
        if (state.currentView === 'history') views.history.render();
        if (state.currentView === 'queue')   views.queue.render();
        if (state.currentView === 'performance') views.performance.render();
      };
      ref.on('value', handler);
      state.listeners.push(() => ref.off('value', handler));
    },

    /* --- stock --- */
    listenStock() {
      const ref = fbDb.ref('stock');
      const handler = (snap) => {
        state.stock = snap.val() || {};
        if (state.currentView === 'magasin') views.magasin.render();
        views.magasin.updateStockBadge();
      };
      ref.on('value', handler, () => {});
      state.listeners.push(() => ref.off('value', handler));

      // Movements
      const movRef = fbDb.ref('stockMovements').orderByChild('timestamp').limitToLast(50);
      const movHandler = (snap) => {
        state.stockMovements = [];
        snap.forEach(c => state.stockMovements.unshift({ id:c.key, ...c.val() }));
        if (state.currentView === 'magasin') views.magasin.renderMovements();
      };
      movRef.on('value', movHandler, () => {});
      state.listeners.push(() => movRef.off('value', movHandler));
    },

    async createIntervention(data) {
      const reg = await this.getNextReg();
      const now = Date.now();
      const intervention = {
        numBon: reg,
        qrCode: `LEONI-INT-${reg}`,
        status: 'draft',
        statusHistory: [{
          status: 'draft', at: now,
          by: state.user.uid, byName: state.profile.displayName
        }],
        tool: data.tool || {},
        crimping: {
          filledAt: now,
          filledBy: { uid: state.user.uid, name: state.profile.displayName, matricule: state.profile.matricule || '' },
          date: data.date || now,
          cycles: data.cycles || 0,
          type: data.type || 'preventive',
          piecesChanged: data.piecesChanged || {},
          observation: data.observation || '',
          cyclesPhotoUrl: data.cyclesPhotoUrl || null,
          signatureUrl: data.signatureUrl || null
        },
        lab: null,
        sla: { submittedAt: null, decidedAt: null, durationMs: null },
        locked: false,
        createdAt: now,
        updatedAt: now,
        _indexes: {
          yearMonth: new Date(now).toISOString().slice(0, 7),
          toolId: (data.tool && data.tool.toolId) || '',
          status: 'draft'
        }
      };
      await fbDb.ref(`interventions/${reg}`).set(intervention);
      this.audit('intervention.create', reg);
      return reg;
    },

    async updateIntervention(reg, partial) {
      partial.updatedAt = Date.now();
      await fbDb.ref(`interventions/${reg}`).update(partial);
      this.audit('intervention.update', reg);
    },

    async submitToLab(reg) {
      const now = Date.now();
      const ref = fbDb.ref(`interventions/${reg}`);
      const snap = await ref.once('value');
      const data = snap.val();
      if (!data) throw new Error('Bon introuvable');
      const history = data.statusHistory || [];
      history.push({ status: 'submitted', at: now, by: state.user.uid, byName: state.profile.displayName });
      await ref.update({
        status: 'submitted',
        statusHistory: history,
        'sla/submittedAt': now,
        '_indexes/status': 'submitted',
        updatedAt: now
      });
      this.audit('intervention.submit', reg);
      this.notifyLab(reg);
    },

    async validateIntervention(reg, labData) {
      const now = Date.now();
      const ref = fbDb.ref(`interventions/${reg}`);
      const snap = await ref.once('value');
      const data = snap.val();
      const submittedAt = (data.sla && data.sla.submittedAt) || now;
      const history = data.statusHistory || [];
      history.push({ status: 'validated', at: now, by: state.user.uid, byName: state.profile.displayName });
      await ref.update({
        status: 'validated',
        statusHistory: history,
        lab: {
          ...labData,
          filledAt: now,
          filledBy: { uid: state.user.uid, name: state.profile.displayName, matricule: state.profile.matricule || '' },
          decision: 'validated'
        },
        'sla/decidedAt': now,
        'sla/durationMs': now - submittedAt,
        '_indexes/status': 'validated',
        locked: true,
        updatedAt: now
      });
      this.audit('intervention.validate', reg);
      this.notifyCrimping(reg, 'validated');
    },

    async rejectIntervention(reg, reason) {
      const now = Date.now();
      const ref = fbDb.ref(`interventions/${reg}`);
      const snap = await ref.once('value');
      const data = snap.val();
      const submittedAt = (data.sla && data.sla.submittedAt) || now;
      const history = data.statusHistory || [];
      history.push({ status: 'rejected', at: now, by: state.user.uid, byName: state.profile.displayName, reason });
      await ref.update({
        status: 'rejected',
        statusHistory: history,
        'lab/decision': 'rejected',
        'lab/rejectReason': reason,
        'lab/filledBy': { uid: state.user.uid, name: state.profile.displayName },
        'sla/decidedAt': now,
        'sla/durationMs': now - submittedAt,
        '_indexes/status': 'rejected',
        updatedAt: now
      });
      this.audit('intervention.reject', reg);
      this.notifyCrimping(reg, 'rejected', reason);
    },

    async deleteIntervention(reg) {
      await fbDb.ref(`interventions/${reg}`).remove();
      this.audit('intervention.delete', reg);
    },

    /* --- audit & notifications --- */
    audit(action, entityId, extra = {}) {
      const log = {
        timestamp: Date.now(),
        uid: state.user.uid,
        userName: state.profile.displayName,
        action,
        entity: `interventions/${entityId}`,
        ...extra
      };
      fbDb.ref('auditLog').push(log);
    },

    async notifyLab(reg) {
      const usersSnap = await fbDb.ref('users').orderByChild('role').equalTo('labo').once('value');
      const labUsers = usersSnap.val() || {};
      Object.keys(labUsers).forEach(uid => {
        fbDb.ref(`notifications/${uid}`).push({
          type: 'intervention_submitted',
          message: `Nouveau bon N°${reg} à valider`,
          interventionId: reg,
          createdAt: Date.now(),
          read: false
        });
      });
    },

    async notifyCrimping(reg, decision, reason = null) {
      const intSnap = await fbDb.ref(`interventions/${reg}/crimping/filledBy/uid`).once('value');
      const uid = intSnap.val();
      if (!uid) return;
      fbDb.ref(`notifications/${uid}`).push({
        type: `intervention_${decision}`,
        message: decision === 'validated'
          ? `Ton bon N°${reg} a été validé ✅`
          : `Ton bon N°${reg} a été refusé : ${reason}`,
        interventionId: reg,
        createdAt: Date.now(),
        read: false
      });
    },

    listenNotifications() {
      if (!state.user) return;
      const ref = fbDb.ref(`notifications/${state.user.uid}`).orderByChild('createdAt').limitToLast(20);
      const handler = (snap) => {
        const notifs = [];
        snap.forEach(child => notifs.push({ id: child.key, ...child.val() }));
        notifs.reverse();
        const unread = notifs.filter(n => !n.read).length;
        const badge = $('#notif-badge');
        badge.textContent = unread;
        badge.hidden = unread === 0;
        views.notifications.render(notifs);
      };
      ref.on('value', handler);
      state.listeners.push(() => ref.off('value', handler));
    }
  };

  /* ==========================================================================
     8. STORAGE MODULE
     ========================================================================== */
  const storage = {
    async upload(path, file) {
      const ref = fbStorage.ref(path);
      const snap = await ref.put(file);
      const url = await snap.ref.getDownloadURL();
      return { path, url };
    },
    async uploadCoupe(reg, file) {
      const year = new Date().getFullYear();
      const ext = file.name.split('.').pop();
      return this.upload(`coupes/${year}/${reg}.${ext}`, file);
    },
    async uploadCyclesPhoto(reg, file) {
      return this.upload(`cycles/${reg}.jpg`, file);
    },
    async uploadSignature(reg, kind, dataUrl) {
      const blob = await (await fetch(dataUrl)).blob();
      return this.upload(`signatures/${kind}_${reg}.png`, blob);
    }
  };

  /* ==========================================================================
     9. ROUTER
     ========================================================================== */
  const router = {
    init() {
      window.addEventListener('hashchange', () => this.handleHash());
      $$('[data-nav]').forEach(el => {
        el.addEventListener('click', (e) => {
          e.preventDefault();
          this.go(el.dataset.nav);
        });
      });
    },
    go(view) {
      window.location.hash = view;
    },
    handleHash() {
      const hash = window.location.hash.replace('#', '') || 'hub';
      const valid = ['hub','intervention','history','queue','catalog','users','stats','auditlog','magasin','performance'];
      let view = valid.includes(hash) ? hash : 'hub';

      // ── Restrictions par rôle ──
      // Les techniciens LABO ne peuvent pas voir le catalogue ni créer un nouveau bon
      if (state.role === 'labo') {
        if (view === 'catalog' || view === 'users') {
          view = 'queue';
          window.location.hash = 'queue';
          if (typeof ui !== 'undefined' && ui.toast) ui.toast('Accès non autorisé pour votre rôle', 'warn');
          return;
        }
      }

      ui.showView(view);
      if (views[view] && views[view].render) views[view].render();
    }
  };

  /* ==========================================================================
     10. CAPABILITY CALCULATIONS (Cm/Cmk)
     ========================================================================== */
  const capa = {
    mean(arr) {
      const valid = arr.filter(x => !isNaN(x) && x !== null);
      if (!valid.length) return null;
      return valid.reduce((a, b) => a + b, 0) / valid.length;
    },
    stdDev(arr) {
      const m = this.mean(arr);
      if (m == null) return null;
      const valid = arr.filter(x => !isNaN(x) && x !== null);
      if (valid.length < 2) return null;
      const variance = valid.reduce((sum, x) => sum + Math.pow(x - m, 2), 0) / (valid.length - 1);
      return Math.sqrt(variance);
    },
    cm(arr, tol) {
      const sigma = this.stdDev(arr);
      if (!sigma || sigma === 0) return null;
      return (tol.max - tol.min) / (6 * sigma);
    },
    cmk(arr, tol) {
      const m = this.mean(arr);
      const sigma = this.stdDev(arr);
      if (!sigma || sigma === 0 || m == null) return null;
      return Math.min((m - tol.min), (tol.max - m)) / (3 * sigma);
    },
    compute(measures, tolerances = DEFAULT_TOLERANCES) {
      const hi = (measures.hauteurIsolant || []).map(Number);
      const ef = (measures.effort || []).map(Number);
      return {
        hauteurAmeMoyenne: this.mean(hi),
        cmAme:    this.cm(hi, tolerances.hauteurIsolant),
        cmkAme:   this.cmk(hi, tolerances.hauteurIsolant),
        cmEffort: this.cm(ef, tolerances.effort)
      };
    }
  };

  /* ==========================================================================
     11. SIGNATURE PAD
     ========================================================================== */
  function initSignaturePad(canvas) {
    const ctx = canvas.getContext('2d');
    let drawing = false;
    let last = null;
    const dpr = window.devicePixelRatio || 1;
    const resize = () => {
      const rect = canvas.getBoundingClientRect();
      canvas.width = rect.width * dpr;
      canvas.height = rect.height * dpr;
      ctx.scale(dpr, dpr);
      ctx.lineWidth = 2;
      ctx.lineCap = 'round';
      ctx.strokeStyle = '#0a0f1c';
    };
    resize();
    const pos = (e) => {
      const rect = canvas.getBoundingClientRect();
      const t = e.touches ? e.touches[0] : e;
      return { x: t.clientX - rect.left, y: t.clientY - rect.top };
    };
    const start = (e) => { drawing = true; last = pos(e); e.preventDefault(); };
    const move = (e) => {
      if (!drawing) return;
      const p = pos(e);
      ctx.beginPath();
      ctx.moveTo(last.x, last.y);
      ctx.lineTo(p.x, p.y);
      ctx.stroke();
      last = p;
      e.preventDefault();
    };
    const end = () => { drawing = false; };
    canvas.addEventListener('mousedown', start);
    canvas.addEventListener('mousemove', move);
    canvas.addEventListener('mouseup', end);
    canvas.addEventListener('mouseleave', end);
    canvas.addEventListener('touchstart', start, { passive: false });
    canvas.addEventListener('touchmove', move, { passive: false });
    canvas.addEventListener('touchend', end);
    return {
      clear: () => ctx.clearRect(0, 0, canvas.width, canvas.height),
      isEmpty: () => {
        if (!canvas.width || !canvas.height) return true;
        try {
          const data = ctx.getImageData(0, 0, canvas.width, canvas.height).data;
          return !data.some(v => v !== 0);
        } catch (_) { return true; }
      },
      toDataURL: () => (!canvas.width || !canvas.height) ? null : canvas.toDataURL('image/png')
    };
  }

  /* ==========================================================================
     12. QR CODE (minimal — texte centré sur canvas)
     ========================================================================== */
  function drawQrPlaceholder(canvas, text) {
    const ctx = canvas.getContext('2d');
    canvas.width = 80; canvas.height = 80;
    ctx.fillStyle = '#fff'; ctx.fillRect(0, 0, 80, 80);
    ctx.fillStyle = '#0a0f1c';
    ctx.font = 'bold 9px monospace';
    ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
    ctx.fillText(text, 40, 40);
    // Pour un vrai QR : intégrer la lib qrcode-generator depuis CDN
  }

  /* ==========================================================================
     13. VIEWS
     ========================================================================== */
  const views = {

    /* ========== HUB ========== */
    hub: {
      render() {
        const list = Object.values(state.interventions);
        const now = new Date();
        const monthStart = new Date(now.getFullYear(), now.getMonth(), 1).getTime();
        const monthList = list.filter(i => (i.createdAt || 0) >= monthStart);
        const pending = list.filter(i => i.status === 'submitted');
        const validated = monthList.filter(i => i.status === 'validated');
        const rejected = monthList.filter(i => i.status === 'rejected');
        const slas = list.filter(i => i.sla && i.sla.durationMs).map(i => i.sla.durationMs);
        const avgSla = slas.length ? slas.reduce((a, b) => a + b, 0) / slas.length : 0;

        $('#kpi-month').textContent = monthList.length;
        $('#kpi-pending').textContent = pending.length;
        $('#kpi-validated').textContent = validated.length;
        $('#kpi-rejected').textContent = rejected.length;
        $('#kpi-sla').textContent = fmt.duration(avgSla);

        const validRate = monthList.length ? Math.round(validated.length / monthList.length * 100) : 0;
        $('#kpi-validated-rate').textContent = `${validRate}% du mois`;

        // Sidebar pills
        $('#nav-history-count').textContent = list.length;
        $('#nav-queue-count').textContent = pending.length;

        // Queue urgente
        const queueList = $('#queue-list');
        queueList.innerHTML = pending.slice(0, 5).map(i => `
          <li data-reg="${i.numBon}">
            <span class="status-pill status-pill--submitted"></span>
            <strong>N°${i.numBon}</strong>
            <span>${ui.escape(i.tool && i.tool.outilId || '—')}</span>
            <span style="margin-left:auto;color:var(--text-muted);font-size:var(--fs-xs)">${fmt.dateTime(i.createdAt)}</span>
          </li>
        `).join('') || '<li style="color:var(--text-muted)">Aucun bon en attente 🎉</li>';

        queueList.querySelectorAll('li[data-reg]').forEach(li => {
          li.addEventListener('click', () => views.intervention.open(li.dataset.reg));
        });

        // Top outils
        const toolStats = {};
        list.forEach(i => {
          const k = (i.tool && i.tool.outilId) || 'Inconnu';
          toolStats[k] = (toolStats[k] || 0) + 1;
        });
        const top = Object.entries(toolStats).sort((a, b) => b[1] - a[1]).slice(0, 5);
        $('#top-tools-list').innerHTML = top.map(([k, v]) => `
          <li><strong>${ui.escape(k)}</strong><span style="margin-left:auto">${v} interventions</span></li>
        `).join('') || '<li style="color:var(--text-muted)">Pas de données</li>';

        // Activity feed (5 dernières)
        const recent = list.sort((a, b) => (b.updatedAt || 0) - (a.updatedAt || 0)).slice(0, 5);
        $('#activity-feed').innerHTML = recent.map(i => `
          <li>
            <span class="status-pill status-pill--${i.status}"></span>
            <span>N°${i.numBon} · ${ui.escape(i.tool && i.tool.outilId || '—')}</span>
            <span style="margin-left:auto;color:var(--text-muted);font-size:var(--fs-xs)">${fmt.dateTime(i.updatedAt)}</span>
          </li>
        `).join('') || '<li style="color:var(--text-muted)">Aucune activité</li>';

        // ── Health Score ──
        this.renderHealthScore(list);

        // ── Chart Hub ──
        this.renderWeeklyChart(list);
      },

      renderHealthScore(list) {
        const hsEl = document.getElementById('hub-health-score');
        if (!hsEl) return;
        const total = list.length;
        if (!total) { hsEl.innerHTML = '<span style="color:var(--text-muted);font-size:.85rem">Pas de données</span>'; return; }

        const monthStart = new Date(); monthStart.setDate(1); monthStart.setHours(0,0,0,0);
        const monthList = list.filter(i => (i.createdAt||0) >= monthStart.getTime());
        const validated = monthList.filter(i => i.status === 'validated').length;
        const rejected  = monthList.filter(i => i.status === 'rejected').length;
        const pending   = list.filter(i => i.status === 'submitted').length;
        const now = Date.now();
        const overdue = list.filter(i => i.status === 'submitted' && i.sla && i.sla.submittedAt && (now - i.sla.submittedAt) > 48*3600000).length;

        const mTotal = monthList.length || 1;
        const validRate  = validated / mTotal;
        const rejectRate = rejected / mTotal;
        const overdueScore = Math.max(0, 1 - (overdue / Math.max(pending, 1)));

        const score = Math.round((validRate * 40) + ((1 - rejectRate) * 30) + (overdueScore * 30));
        const color = score >= 80 ? '#10b981' : score >= 50 ? '#f59e0b' : '#ef4444';
        const label = score >= 80 ? 'Excellent' : score >= 50 ? 'Correct' : 'Attention';

        const dash = Math.round(score * 2.51);
        hsEl.innerHTML = `
          <div style="display:flex;align-items:center;gap:1.25rem;flex-wrap:wrap">
            <div style="position:relative;width:80px;height:80px;flex-shrink:0">
              <svg viewBox="0 0 36 36" width="80" height="80" style="transform:rotate(-90deg)">
                <circle cx="18" cy="18" r="15.9" fill="none" stroke="var(--border-base)" stroke-width="3"/>
                <circle cx="18" cy="18" r="15.9" fill="none" stroke="${color}" stroke-width="3"
                  stroke-dasharray="${dash} ${251 - dash}" stroke-linecap="round" style="transition:stroke-dasharray .8s ease"/>
              </svg>
              <div style="position:absolute;inset:0;display:flex;flex-direction:column;align-items:center;justify-content:center">
                <span style="font-size:1.1rem;font-weight:800;color:${color};line-height:1">${score}</span>
                <span style="font-size:.55rem;color:var(--text-muted)">/ 100</span>
              </div>
            </div>
            <div>
              <div style="font-size:1rem;font-weight:700;color:${color}">${label}</div>
              <div style="font-size:.78rem;color:var(--text-secondary);margin-top:.2rem">
                Taux validation : ${Math.round(validRate*100)}%<br>
                Bons en retard : ${overdue}<br>
                Refus ce mois : ${rejected}
              </div>
            </div>
          </div>
          ${overdue > 3 ? '<div style="margin-top:.75rem;padding:.5rem .75rem;background:rgba(239,68,68,.1);border:1px solid rgba(239,68,68,.3);border-radius:7px;font-size:.8rem;color:#fca5a5">⚠️ ' + overdue + ' bon(s) en attente depuis plus de 48h</div>' : ''}
        `;
      },

      renderWeeklyChart(list) {
        const canvas = document.getElementById('chart-weekly');
        if (!canvas) return;
        const loadChart = () => new Promise((res, rej) => {
          if (window.Chart) return res();
          const s = document.createElement('script');
          s.src = 'https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js';
          s.onload = res; s.onerror = rej;
          document.head.appendChild(s);
        });
        loadChart().then(() => {
          const weeks = 12;
          const labels = [], counts = [];
          for (let w = weeks - 1; w >= 0; w--) {
            const end = new Date(); end.setDate(end.getDate() - w * 7);
            const start = new Date(end); start.setDate(start.getDate() - 7);
            const label = start.toLocaleDateString('fr-FR', { day:'2-digit', month:'2-digit' });
            labels.push(label);
            counts.push(list.filter(i => i.createdAt >= start.getTime() && i.createdAt < end.getTime()).length);
          }
          if (canvas._chart) canvas._chart.destroy();
          canvas._chart = new Chart(canvas, {
            type: 'bar',
            data: { labels, datasets: [{ label: 'Bons', data: counts,
              backgroundColor: 'rgba(59,130,246,.5)', borderColor: '#3b82f6',
              borderWidth: 1, borderRadius: 4 }] },
            options: { responsive: true, maintainAspectRatio: false,
              plugins: { legend: { display: false } },
              scales: { x: { ticks: { color: '#8895b3', font: { size: 11 } }, grid: { color: 'rgba(255,255,255,.04)' } },
                y: { ticks: { color: '#8895b3', font: { size: 11 }, stepSize: 1 }, grid: { color: 'rgba(255,255,255,.04)' }, beginAtZero: true } } }
          });
        }).catch(() => {});
      }
    },

    /* ========== INTERVENTION ========== */
    intervention: {
      crimpPad: null,
      labPad: null,
      currentToolKey: null,

      init() {
        // Init signature pads
        this.crimpPad = initSignaturePad($('#sign-crimp-pad'));
        this.labPad = initSignaturePad($('#sign-lab-pad'));
        $('#btn-sign-crimp-clear').addEventListener('click', () => this.crimpPad.clear());
        $('#btn-sign-lab-clear').addEventListener('click', () => this.labPad.clear());

        // Tool autocomplete
        $('#int-tool-search').addEventListener('input', debounce(() => this.onToolSelected(), 200));

        // Auto-calc capabilité
        const measureInputs = $$('.measure-table__input');
        measureInputs.forEach(inp => inp.addEventListener('input', debounce(() => this.recalcCapa(), 200)));

        // Counter observation
        const obs = $('#int-observation');
        obs.addEventListener('input', () => {
          $('#obs-counter').textContent = `${obs.value.length} / 1000`;
        });

        // Boutons d'action
        $('#btn-int-save').addEventListener('click', () => this.save('draft'));
        $('#form-intervention').addEventListener('submit', (e) => { e.preventDefault(); this.save('submit'); });
        $('#btn-validate').addEventListener('click', () => this.validate());
        $('#btn-reject').addEventListener('click', () => this.reject());
        $('#btn-int-delete').addEventListener('click', () => this.delete());
        $('#btn-int-back').addEventListener('click', () => router.go('history'));
        $('#btn-int-print').addEventListener('click', () => window.print());

        // PDF Export
        const btnPdf = document.getElementById('btn-int-export');
        if (btnPdf) btnPdf.addEventListener('click', () => this.exportPDF());

        // Photo cycles
        $('#upload-cycles .upload__btn').addEventListener('click', () => $('#file-cycles').click());
        $('#upload-coupe .upload__btn').addEventListener('click', () => $('#file-coupe').click());

        // Quick new
        $('#btn-quick-new').addEventListener('click', () => this.newBlank());
      },

      async exportPDF() {
        const reg = state.currentInterventionId;
        if (!reg) return ui.toast('Ouvrez un bon d\'intervention pour l\'exporter', 'warn');
        const data = state.interventions[reg];
        if (!data) return;

        const loadjsPDF = () => new Promise((res,rej) => {
          if (window.jspdf) return res();
          const s = document.createElement('script');
          s.src = 'https://cdnjs.cloudflare.com/ajax/libs/jspdf/2.5.1/jspdf.umd.min.js';
          s.onload = res; s.onerror = rej; document.head.appendChild(s);
        });

        ui.showLoader('Génération du PDF…');
        try {
          await loadjsPDF();
          const { jsPDF } = window.jspdf;
          const doc = new jsPDF({ orientation: 'portrait', unit: 'mm', format: 'a4' });
          const W = 210; const margin = 18;

          // Header LEONI
          doc.setFillColor(10, 14, 26);
          doc.rect(0, 0, W, 28, 'F');
          doc.setTextColor(255,255,255);
          doc.setFont('helvetica','bold');
          doc.setFontSize(18); doc.text('LEONI', margin, 12);
          doc.setFontSize(10); doc.setFont('helvetica','normal');
          doc.text('Wiring Systems — Bon d\'Intervention Sertissage', margin, 20);
          doc.setFontSize(9); doc.text('N° ' + data.numBon, W - margin, 12, {align:'right'});
          doc.text(new Date(data.createdAt).toLocaleDateString('fr-FR'), W - margin, 20, {align:'right'});

          let y = 38;
          const drawSection = (title, color) => {
            doc.setFillColor(...color); doc.rect(margin, y, W - margin*2, 7, 'F');
            doc.setTextColor(255,255,255); doc.setFont('helvetica','bold'); doc.setFontSize(9);
            doc.text(title, margin + 2, y + 5);
            doc.setTextColor(30,30,30); doc.setFont('helvetica','normal');
            y += 10;
          };
          const drawRow = (label, value, x2) => {
            doc.setFontSize(8.5); doc.setFont('helvetica','bold'); doc.setTextColor(80,80,80);
            doc.text(label, margin + 2, y + 4);
            doc.setFont('helvetica','normal'); doc.setTextColor(30,30,30);
            doc.text(String(value || '—'), (x2 || margin + 50), y + 4);
            y += 7;
          };

          // Statut badge
          const statusColors = { validated:[16,185,129], submitted:[245,158,11], rejected:[239,68,68], draft:[107,115,136] };
          const sc = statusColors[data.status] || statusColors.draft;
          doc.setFillColor(...sc); doc.roundedRect(margin, y, 40, 7, 2, 2, 'F');
          doc.setTextColor(255,255,255); doc.setFont('helvetica','bold'); doc.setFontSize(8);
          doc.text(fmt.statusLabel(data.status).toUpperCase(), margin + 20, y + 5, {align:'center'});
          y += 13;

          // Section outil
          drawSection('IDENTIFICATION DE L\'OUTIL', [30, 58, 138]);
          if (data.tool) {
            drawRow('Réf. outil:', data.tool.refOutil); drawRow('Outil ID:', data.tool.outilId);
            drawRow('Fabricant:', data.tool.fabricant); drawRow('Réf. fabricant:', data.tool.refFabricant);
          }
          y += 3;

          // Section crimping
          drawSection('PARTIE CRIMPING — TECHNICIEN MAINTENANCE', [15, 118, 110]);
          if (data.crimping) {
            drawRow('Technicien:', data.crimping.filledBy && data.crimping.filledBy.name);
            drawRow('Matricule:', data.crimping.filledBy && data.crimping.filledBy.matricule);
            drawRow('Date:', data.crimping.date ? new Date(data.crimping.date).toLocaleDateString('fr-FR') : '—');
            drawRow('Cycles compteur:', data.crimping.cycles);
            drawRow('Type:', data.crimping.type ? {preventive:'Préventive',curative:'Curative',requalification:'Requalification'}[data.crimping.type] : '—');
            const pieces = Object.entries(data.crimping.piecesChanged||{}).filter(([,v])=>v).map(([k])=>k).join(', ') || 'Aucune';
            drawRow('Pièces changées:', pieces);
            if (data.crimping.observation) drawRow('Observation:', data.crimping.observation.slice(0,60));
          }
          y += 3;

          // Section labo
          if (data.lab) {
            drawSection('PARTIE LABORATOIRE — VALIDATION', [59, 130, 246]);
            drawRow('Technicien labo:', data.lab.filledBy && data.lab.filledBy.name);
            drawRow('Réf. connexion:', data.lab.connexion && data.lab.connexion.refConnexion);
            drawRow('Section câble:', data.lab.connexion && data.lab.connexion.sectionCable);
            if (data.lab.capabilite) {
              const c = data.lab.capabilite;
              drawRow('Cm âme:', c.cmAme ? c.cmAme.toFixed(2) : '—');
              drawRow('Cmk âme:', c.cmkAme ? c.cmkAme.toFixed(2) : '—');
              drawRow('Cm effort:', c.cmEffort ? c.cmEffort.toFixed(2) : '—');
            }
            const decLabel = data.lab.decision === 'validated' ? '✓ VALIDÉ' : '✗ REFUSÉ';
            const decColor = data.lab.decision === 'validated' ? [16,185,129] : [239,68,68];
            doc.setFillColor(...decColor); doc.roundedRect(margin, y, 45, 8, 2, 2, 'F');
            doc.setTextColor(255,255,255); doc.setFont('helvetica','bold'); doc.setFontSize(9);
            doc.text(decLabel, margin + 22, y + 6, {align:'center'});
            y += 14;
          }

          // Footer
          doc.setDrawColor(200,200,200); doc.setLineWidth(0.3);
          doc.line(margin, 282, W - margin, 282);
          doc.setTextColor(150,150,150); doc.setFont('helvetica','normal'); doc.setFontSize(7.5);
          doc.text('LEONI Wiring Systems · Document généré le ' + new Date().toLocaleString('fr-FR'), W/2, 287, {align:'center'});

          doc.save('BON-LEONI-N' + reg + '-' + new Date().toISOString().slice(0,10) + '.pdf');
          ui.toast('PDF généré ✅', 'success');
        } catch(err) {
          console.error(err); ui.toast('Erreur PDF: ' + err.message, 'danger');
        } finally { ui.hideLoader(); }
      },

      newBlank() {
        state.currentInterventionId = null;
        $('#form-intervention').reset();
        $('#int-num').textContent = '— (nouveau)';
        $('#int-created').textContent = fmt.dateTime(Date.now());
        $('#int-date').valueAsDate = new Date();
        this.setStatus('draft');
        this.crimpPad.clear();
        this.labPad.clear();
        this.clearToolCard();
        $('#int-timeline').innerHTML = '';
        router.go('intervention');
        ui.applyRoleVisibility();
        // Active la section labo selon le rôle (visible mais disabled tant que pas submitted)
        $('#section-lab').disabled = true;
        // LABO ne peut pas créer de bons crimping
        if (state.role === 'labo') {
          ui.toast('Le rôle Labo ne peut pas créer de bons. Ouvrez un bon existant depuis la File d’attente.', 'warn');
          router.go('queue');
          return;
        }
        // Pré-remplir le technicien crimping avec le profil connecté
        if ($('#crimp-tech-name') && state.profile) {
          if ((state.role === 'crimp' || state.role === 'crimping') && !$('#crimp-tech-name').value) {
            $('#crimp-tech-name').value = state.profile.displayName || '';
            $('#crimp-tech-mat').value  = state.profile.matricule   || '';
          }
        }
        // Pré-remplir le technicien labo avec le profil connecté
        if ($('#labo-tech-name') && state.profile) {
          if (state.role === 'labo' && !$('#labo-tech-name').value) {
            $('#labo-tech-name').value = state.profile.displayName || '';
            $('#labo-tech-mat').value  = state.profile.matricule   || '';
          }
        }
      },

      open(reg) {
        const data = state.interventions[reg];
        if (!data) { ui.toast('Bon introuvable', 'danger'); return; }
        state.currentInterventionId = reg;

        $('#int-num').textContent = data.numBon;
        $('#int-created').textContent = fmt.dateTime(data.createdAt);
        this.setStatus(data.status);
        drawQrPlaceholder($('#int-qrcode'), `#${data.numBon}`);

        // Outil
        if (data.tool) {
          $('#int-tool-search').value = data.tool.refOutil || '';
          $('#int-outil-id').value = data.tool.outilId || '';
          $('#int-fabricant').value = data.tool.fabricant || '';
          this.fillToolCard(data.tool);
        }

        // ── Verrouillage Outil + Crimping pour le rôle LABO ──
        const isLabo = state.role === 'labo';
        const toolSection = $('.intervention__section--tool');
        if (toolSection) toolSection.disabled = isLabo;

        // Disable outil inputs individually (fieldset disabled n'affecte pas les readonly)
        ['int-tool-search','int-outil-id','int-fabricant','int-outil-id'].forEach(id => {
          const el = document.getElementById(id);
          if (el) {
            el.readOnly  = isLabo;
            el.style.opacity = isLabo ? '0.6' : '';
            el.style.cursor  = isLabo ? 'not-allowed' : '';
            el.style.pointerEvents = isLabo ? 'none' : '';
          }
        });

        // Crimping
        if (data.crimping) {
          $('#int-date').value = data.crimping.date ? new Date(data.crimping.date).toISOString().slice(0, 10) : '';
          $('#int-cycles').value = data.crimping.cycles || '';
          $('#int-type').value = data.crimping.type || '';
          $('#int-observation').value = data.crimping.observation || '';
          $('#obs-counter').textContent = `${(data.crimping.observation || '').length} / 1000`;
          // Load duration
          const durEl = $('#int-duration');
          if (durEl) {
            durEl.value = data.crimping.duration || '';
            const display = document.getElementById('int-duration-display');
            if (display) {
              if (data.crimping.duration) {
                const h = Math.floor(data.crimping.duration/60), m = data.crimping.duration%60;
                display.textContent = h > 0 ? `≈ ${h}h ${m}min` : `≈ ${m}min`;
              } else display.textContent = '—';
            }
            // Hide duration field for labo (read-only on this section)
            const fieldDur = document.getElementById('field-duration');
            if (fieldDur) {
              fieldDur.style.display = (state.role === 'magasinier') ? 'none' : 'block';
            }
          }
          $$('input[name="piece"]').forEach(cb => {
            cb.checked = !!(data.crimping.piecesChanged && data.crimping.piecesChanged[cb.value]);
          });
          // Technicien crimping
          if ($('#crimp-tech-name')) $('#crimp-tech-name').value = data.crimping.technicienNom || (data.crimping.filledBy && data.crimping.filledBy.name) || '';
          if ($('#crimp-tech-mat'))  $('#crimp-tech-mat').value  = data.crimping.technicienMatricule || (data.crimping.filledBy && data.crimping.filledBy.matricule) || '';
        }

        // ── Verrouillage section CRIMPING pour le rôle LABO ──
        const crimpSection = document.getElementById('section-crimping');
        if (crimpSection) {
          if (state.role === 'labo') {
            crimpSection.disabled = true;
            crimpSection.style.opacity = '0.65';
            crimpSection.style.pointerEvents = 'none';
            // Ajouter badge "Lecture seule" si pas déjà là
            if (!document.getElementById('crimp-readonly-badge')) {
              const badge = document.createElement('span');
              badge.id = 'crimp-readonly-badge';
              badge.style.cssText = 'display:inline-flex;align-items:center;gap:.3rem;font-size:.72rem;font-weight:600;padding:.18rem .55rem;border-radius:5px;background:rgba(99,102,241,.12);color:#a5b4fc;border:1px solid rgba(99,102,241,.25);margin-left:.75rem';
              badge.textContent = '🔒 Lecture seule';
              const legend = crimpSection.querySelector('.intervention__legend');
              if (legend) legend.appendChild(badge);
            }
          } else {
            crimpSection.disabled = false;
            crimpSection.style.opacity = '';
            crimpSection.style.pointerEvents = '';
            document.getElementById('crimp-readonly-badge')?.remove();
          }
        }

        // Lab
        const lab = data.lab || {};
        const labSection = $('#section-lab');
        // Débloquer la section labo si on est au moins submitted ET utilisateur labo/admin
        const canEditLab = ['submitted'].includes(data.status) && auth.can('intervention.editLab');
        labSection.disabled = !canEditLab && data.status !== 'validated' && data.status !== 'rejected';

        // Technicien labo
        if ($('#labo-tech-name')) $('#labo-tech-name').value = lab.technicienNom || (lab.filledBy && lab.filledBy.name) || '';
        if ($('#labo-tech-mat'))  $('#labo-tech-mat').value  = lab.technicienMatricule || (lab.filledBy && lab.filledBy.matricule) || '';

        if (lab.connexion) {
          $('#lab-ref-connexion').value = lab.connexion.refConnexion || '';
          $('#lab-section-cable').value = lab.connexion.sectionCable || '';
          $('#lab-indice').value = lab.connexion.indiceParametrique || '';
        }
        if (lab.mesures) {
          $('#lab-largeur-ame').value = lab.mesures.largeurAme || '';
          $('#lab-largeur-isolant').value = lab.mesures.largeurIsolant || '';
          (lab.mesures.hauteurIsolant || []).forEach((v, i) => {
            const inp = $(`[name="hi${i+1}"]`); if (inp) inp.value = v;
          });
          (lab.mesures.effort || []).forEach((v, i) => {
            const inp = $(`[name="ef${i+1}"]`); if (inp) inp.value = v;
          });
        }
        if (lab.conformites) {
          $$('input[name="conf"]').forEach(cb => { cb.checked = !!lab.conformites[cb.value]; });
        }
        $('#lab-chute-tension').value = (lab.capabilite && lab.capabilite.chuteTension) || '';
        this.recalcCapa();

        // Coupe metallo link
        if (lab.coupeMetallo && lab.coupeMetallo.downloadUrl) {
          const link = $('#link-coupe');
          link.href = lab.coupeMetallo.downloadUrl;
          link.hidden = false;
          $('#preview-coupe').textContent = lab.coupeMetallo.fileName || 'Fichier joint';
        }

        // Timeline
        this.renderTimeline(data.statusHistory || []);

        router.go('intervention');
      },

      fillToolCard(tool) {
        $('#tc-ref-fab').textContent = tool.refFabricant || '—';
        $('#tc-par').textContent = (tool.pieces && tool.pieces.pAr) || '—';
        $('#tc-pav').textContent = (tool.pieces && tool.pieces.pAv) || '—';
        $('#tc-ear').textContent = (tool.pieces && tool.pieces.eAr) || '—';
        $('#tc-eav').textContent = (tool.pieces && tool.pieces.eAv) || '—';
        $('#tool-cycle').textContent = `${fmt.number(tool.frequenceCycle || 0)} cycles`;
      },
      clearToolCard() {
        ['tc-ref-fab','tc-par','tc-pav','tc-ear','tc-eav'].forEach(id => $('#'+id).textContent = '—');
        $('#tool-cycle').textContent = '— cycles';
      },

      onToolSelected() {
        const val = $('#int-tool-search').value.trim().toLowerCase();
        if (!val) return this.clearToolCard();
        const found = Object.values(state.tools).find(t =>
          (t.refOutil || '').toLowerCase() === val ||
          (t.outilId || '').toLowerCase() === val
        );
        if (found) {
          $('#int-outil-id').value = found.outilId || '';
          $('#int-fabricant').value = found.fabricant || '';
          this.fillToolCard(found);
          this.currentToolKey = found.id;
        }
      },

      refreshToolDatalist() {
        const dl = $('#datalist-tools');
        if (!dl) return;
        dl.innerHTML = Object.values(state.tools).map(t =>
          `<option value="${ui.escape(t.refOutil || '')}">${ui.escape(t.outilId || '')}</option>`
        ).join('');
      },

      recalcCapa() {
        const hi = [1,2,3,4,5].map(i => parseFloat($(`[name="hi${i}"]`).value));
        const ef = [1,2,3,4,5].map(i => parseFloat($(`[name="ef${i}"]`).value));
        const result = capa.compute({ hauteurIsolant: hi, effort: ef });
        $('#capa-ame-moy').textContent = result.hauteurAmeMoyenne != null ? result.hauteurAmeMoyenne.toFixed(3) : '—';
        $('#capa-cm').textContent      = result.cmAme    != null ? result.cmAme.toFixed(2) : '—';
        $('#capa-cmk').textContent     = result.cmkAme   != null ? result.cmkAme.toFixed(2) : '—';
        $('#capa-cm-effort').textContent = result.cmEffort != null ? result.cmEffort.toFixed(2) : '—';
      },

      setStatus(status) {
        $$('#int-status .status-pill').forEach(p => { p.hidden = (p.dataset.status !== status); });
        // Stepper
        const order = { draft: 1, submitted: 2, validated: 4, rejected: 4, cancelled: 4 };
        const step = order[status] || 1;
        $$('.stepper__step').forEach((el, idx) => {
          el.classList.toggle('stepper__step--active', (idx + 1) <= step);
        });
      },

      renderTimeline(history) {
        $('#int-timeline').innerHTML = history.map(h => `
          <li>
            <strong>${fmt.statusLabel(h.status)}</strong>
            par ${ui.escape(h.byName || '—')} —
            <span style="color:var(--text-muted)">${fmt.dateTime(h.at)}</span>
            ${h.reason ? `<br><em style="color:var(--danger-500)">Motif: ${ui.escape(h.reason)}</em>` : ''}
          </li>
        `).join('');
      },

      collectCrimpingData() {
        const pieces = {};
        $$('input[name="piece"]').forEach(cb => { pieces[cb.value] = cb.checked; });
        // Technicien crimping : champs manuels OU profil connecté
        const crimpName = $('#crimp-tech-name')?.value.trim() || state.profile?.displayName || '';
        const crimpMat  = $('#crimp-tech-mat')?.value.trim()  || state.profile?.matricule   || '';
        return {
          tool: {
            toolId: this.currentToolKey || '',
            refOutil: $('#int-tool-search').value,
            outilId: $('#int-outil-id').value,
            fabricant: $('#int-fabricant').value,
            refFabricant: $('#tc-ref-fab').textContent
          },
          date: $('#int-date').value ? new Date($('#int-date').value).getTime() : Date.now(),
          cycles: parseInt($('#int-cycles').value) || 0,
          type: $('#int-type').value,
          piecesChanged: pieces,
          observation: $('#int-observation').value,
          technicienNom:       crimpName,
          technicienMatricule: crimpMat
        };
      },

      collectLabData() {
        const conf = {};
        $$('input[name="conf"]').forEach(cb => { conf[cb.value] = cb.checked; });
        const hi = [1,2,3,4,5].map(i => parseFloat($(`[name="hi${i}"]`).value) || null);
        const ef = [1,2,3,4,5].map(i => parseFloat($(`[name="ef${i}"]`).value) || null);
        const computed = capa.compute({ hauteurIsolant: hi, effort: ef });
        // Technicien labo : champs manuels OU profil connecté
        const laboName = $('#labo-tech-name')?.value.trim() || state.profile?.displayName || '';
        const laboMat  = $('#labo-tech-mat')?.value.trim()  || state.profile?.matricule   || '';
        return {
          technicienNom:       laboName,
          technicienMatricule: laboMat,
          connexion: {
            refConnexion: $('#lab-ref-connexion').value,
            sectionCable: $('#lab-section-cable').value,
            indiceParametrique: $('#lab-indice').value
          },
          mesures: {
            largeurAme: parseFloat($('#lab-largeur-ame').value) || null,
            largeurIsolant: parseFloat($('#lab-largeur-isolant').value) || null,
            hauteurIsolant: hi,
            effort: ef
          },
          capabilite: {
            ...computed,
            chuteTension: parseFloat($('#lab-chute-tension').value) || null
          },
          conformites: conf
        };
      },

      async save(mode) {
        if (!auth.can('intervention.create') && !auth.can('intervention.edit')) {
          return ui.toast('Accès refusé', 'danger');
        }
        ui.showLoader('Enregistrement…');
        try {
          const data = this.collectCrimpingData();
          if (!data.tool.refOutil || !data.tool.outilId) {
            ui.hideLoader();
            return ui.toast('Référence outil obligatoire', 'warn');
          }
          let reg = state.currentInterventionId;
          if (!reg) {
            reg = await db.createIntervention(data);
            state.currentInterventionId = reg;
          } else {
            await db.updateIntervention(reg, {
              tool: data.tool,
              'crimping/date': data.date,
              'crimping/cycles': data.cycles,
              'crimping/type': data.type,
              'crimping/piecesChanged': data.piecesChanged,
              'crimping/observation': data.observation
            });
          }
          // Upload signature crimping si présente
          const crimpDataUrl = this.crimpPad.toDataURL();
          if (!this.crimpPad.isEmpty() && crimpDataUrl) {
            try {
              const sig = await storage.uploadSignature(reg, 'crimp', crimpDataUrl);
              await db.updateIntervention(reg, { 'crimping/signatureUrl': sig.url });
            } catch(e) { console.warn('Signature upload skipped:', e.message); }
          }
          // Upload photo cycles si présente
          const cycleFile = $('#file-cycles').files[0];
          if (cycleFile) {
            const ph = await storage.uploadCyclesPhoto(reg, cycleFile);
            await db.updateIntervention(reg, { 'crimping/cyclesPhotoUrl': ph.url });
          }
          if (mode === 'submit') {
            await db.submitToLab(reg);
            ui.toast(`Bon N°${reg} soumis au labo ✅`, 'success');
            this.setStatus('submitted');
          } else {
            ui.toast(`Brouillon N°${reg} enregistré`, 'success');
          }
          $('#int-num').textContent = reg;
        } catch (err) {
          console.error(err);
          ui.toast('Erreur : ' + err.message, 'danger');
        } finally {
          ui.hideLoader();
        }
      },

      async validate() {
        if (!auth.can('intervention.validate')) return ui.toast('Accès refusé', 'danger');
        const reg = state.currentInterventionId;
        if (!reg) return ui.toast('Aucun bon ouvert', 'warn');
        ui.showLoader('Validation…');
        try {
          const labData = this.collectLabData();
          // Upload coupe metallo si fichier
          const coupeFile = $('#file-coupe').files[0];
          if (coupeFile) {
            const up = await storage.uploadCoupe(reg, coupeFile);
            labData.coupeMetallo = {
              fileName: coupeFile.name,
              storagePath: up.path,
              downloadUrl: up.url,
              uploadedAt: Date.now()
            };
          }
          // Upload signature labo
          const labDataUrl = this.labPad.toDataURL();
          if (!this.labPad.isEmpty() && labDataUrl) {
            try {
              const sig = await storage.uploadSignature(reg, 'lab', labDataUrl);
              labData.signatureUrl = sig.url;
            } catch(e) { console.warn('Lab signature upload skipped:', e.message); }
          }
          await db.validateIntervention(reg, labData);
          ui.toast(`Bon N°${reg} validé ✅`, 'success');
          this.setStatus('validated');
        } catch (err) {
          console.error(err);
          ui.toast('Erreur : ' + err.message, 'danger');
        } finally {
          ui.hideLoader();
        }
      },

      async reject() {
        if (!auth.can('intervention.reject')) return ui.toast('Accès refusé', 'danger');
        const reg = state.currentInterventionId;
        if (!reg) return;
        // Ouvre modal refus
        const modal = $('#modal-reject');
        modal.hidden = false;
        const onConfirm = async () => {
          const reason = $('#reject-reason').value.trim();
          if (!reason) return ui.toast('Le motif est obligatoire', 'warn');
          modal.hidden = true;
          $('#reject-reason').value = '';
          ui.showLoader('Refus en cours…');
          try {
            await db.rejectIntervention(reg, reason);
            ui.toast(`Bon N°${reg} refusé`, 'warn');
            this.setStatus('rejected');
          } catch (err) {
            ui.toast('Erreur : ' + err.message, 'danger');
          } finally {
            ui.hideLoader();
          }
          $('#btn-confirm-reject').removeEventListener('click', onConfirm);
        };
        $('#btn-confirm-reject').addEventListener('click', onConfirm);
        modal.querySelectorAll('[data-close]').forEach(el => {
          el.addEventListener('click', () => { modal.hidden = true; }, { once: true });
        });
      },

      async delete() {
        if (!auth.can('intervention.delete')) return ui.toast('Accès refusé', 'danger');
        const reg = state.currentInterventionId;
        if (!reg) return;
        const ok = await ui.confirm('Supprimer le bon', `Le bon N°${reg} sera supprimé définitivement. Continuer ?`);
        if (!ok) return;
        ui.showLoader('Suppression…');
        try {
          await db.deleteIntervention(reg);
          ui.toast('Bon supprimé', 'info');
          state.currentInterventionId = null;
          router.go('history');
        } catch (err) {
          ui.toast('Erreur : ' + err.message, 'danger');
        } finally {
          ui.hideLoader();
        }
      }
    },

    /* ========== HISTORY ========== */
    history: {
      init() {
        const filterIds = ['filter-tool','filter-from','filter-to','filter-type','filter-status','filter-tech'];
        filterIds.forEach(id => {
          $('#'+id).addEventListener('input', debounce(() => this.applyFilters(), 300));
        });
        $('#btn-filter-reset').addEventListener('click', () => this.resetFilters());
        $('#btn-filter-export').addEventListener('click', () => this.exportExcel());
        $('#btn-filter-print').addEventListener('click', () => window.print());
        $('#page-prev').addEventListener('click', () => this.changePage(-1));
        $('#page-next').addEventListener('click', () => this.changePage(1));
        $('#page-size').addEventListener('change', (e) => {
          state.pagination.pageSize = parseInt(e.target.value);
          state.pagination.page = 1;
          this.render();
        });
        $$('#history-table thead th[data-sort]').forEach(th => {
          th.addEventListener('click', () => {
            const f = th.dataset.sort;
            state.sort = { field: f, dir: state.sort.field === f && state.sort.dir === 'asc' ? 'desc' : 'asc' };
            this.render();
          });
        });
      },

      applyFilters() {
        state.filters = {
          tool:   $('#filter-tool').value.trim().toLowerCase(),
          from:   $('#filter-from').value,
          to:     $('#filter-to').value,
          type:   $('#filter-type').value,
          status: $('#filter-status').value,
          tech:   $('#filter-tech').value.trim().toLowerCase()
        };
        state.pagination.page = 1;
        this.render();
      },

      resetFilters() {
        ['filter-tool','filter-from','filter-to','filter-type','filter-status','filter-tech'].forEach(id => {
          $('#'+id).value = '';
        });
        this.applyFilters();
      },

      changePage(delta) {
        state.pagination.page += delta;
        this.render();
      },

      getFiltered() {
        let list = Object.values(state.interventions);
        const f = state.filters;
        if (f.tool)   list = list.filter(i => ((i.tool && i.tool.outilId) || '').toLowerCase().includes(f.tool));
        if (f.from)   { const ts = new Date(f.from).getTime(); list = list.filter(i => (i.crimping && i.crimping.date) >= ts); }
        if (f.to)     { const ts = new Date(f.to).getTime() + 86400000; list = list.filter(i => (i.crimping && i.crimping.date) <= ts); }
        if (f.type)   list = list.filter(i => i.crimping && i.crimping.type === f.type);
        if (f.status) list = list.filter(i => i.status === f.status);
        if (f.tech)   list = list.filter(i => ((i.crimping && i.crimping.filledBy && i.crimping.filledBy.name) || '').toLowerCase().includes(f.tech));

        // Sort
        const { field, dir } = state.sort;
        list.sort((a, b) => {
          let va, vb;
          if (field === 'numBon') { va = a.numBon; vb = b.numBon; }
          else if (field === 'outilId') { va = (a.tool && a.tool.outilId) || ''; vb = (b.tool && b.tool.outilId) || ''; }
          else if (field === 'date')    { va = (a.crimping && a.crimping.date) || 0; vb = (b.crimping && b.crimping.date) || 0; }
          else if (field === 'cycles')  { va = (a.crimping && a.crimping.cycles) || 0; vb = (b.crimping && b.crimping.cycles) || 0; }
          else if (field === 'type')    { va = (a.crimping && a.crimping.type) || ''; vb = (b.crimping && b.crimping.type) || ''; }
          else if (field === 'status')  { va = a.status; vb = b.status; }
          else { va = 0; vb = 0; }
          if (va < vb) return dir === 'asc' ? -1 : 1;
          if (va > vb) return dir === 'asc' ? 1 : -1;
          return 0;
        });
        return list;
      },

      render() {
        const list = this.getFiltered();
        const { page, pageSize } = state.pagination;
        const totalPages = Math.max(1, Math.ceil(list.length / pageSize));
        if (state.pagination.page > totalPages) state.pagination.page = totalPages;
        const start = (state.pagination.page - 1) * pageSize;
        const slice = list.slice(start, start + pageSize);

        const tbody = $('#history-body');
        tbody.innerHTML = slice.map(i => {
          const c = i.crimping || {};
          const lab = i.lab || {};
          const coupe = lab.coupeMetallo;
          return `
            <tr data-reg="${i.numBon}">
              <td><input type="checkbox" data-reg="${i.numBon}" /></td>
              <td><strong>${i.numBon}</strong></td>
              <td>${ui.escape((i.tool && i.tool.outilId) || '—')}</td>
              <td>${fmt.date(c.date)}</td>
              <td>${fmt.number(c.cycles)}</td>
              <td>${ui.escape(c.type || '—')}</td>
              <td style="max-width:240px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${ui.escape(c.observation || '')}">${ui.escape(c.observation || '—')}</td>
              <td>${coupe && coupe.downloadUrl ? `<a href="${coupe.downloadUrl}" target="_blank" rel="noopener">📎 ${ui.escape(coupe.fileName || 'Fichier')}</a>` : '—'}</td>
              <td><span class="status-pill status-pill--${i.status}">${fmt.statusLabel(i.status)}</span></td>
              <td>
                <button class="btn btn--ghost" data-action="open" data-reg="${i.numBon}">Ouvrir</button>
              </td>
            </tr>
          `;
        }).join('');

        tbody.querySelectorAll('[data-action="open"]').forEach(btn => {
          btn.addEventListener('click', (e) => {
            e.stopPropagation();
            views.intervention.open(btn.dataset.reg);
          });
        });
        tbody.querySelectorAll('tr[data-reg]').forEach(tr => {
          tr.addEventListener('click', () => views.intervention.open(tr.dataset.reg));
        });

        $('#history-total').textContent = `${list.length} résultat${list.length > 1 ? 's' : ''}`;
        $('#page-info').textContent = `${state.pagination.page} / ${totalPages}`;
      },

      exportExcel() {
        const list = this.getFiltered();
        const headers = ['N°Bon','OUTIL','Date','Cycles','Type','Observation','Statut','Technicien Crimping','Technicien Labo','Cm','Cmk'];
        const rows = list.map(i => {
          const c = i.crimping || {};
          const lab = i.lab || {};
          const cap = lab.capabilite || {};
          return [
            i.numBon,
            (i.tool && i.tool.outilId) || '',
            fmt.date(c.date),
            c.cycles || '',
            c.type || '',
            (c.observation || '').replace(/[\n\r;]/g, ' '),
            fmt.statusLabel(i.status),
            (c.filledBy && c.filledBy.name) || '',
            (lab.filledBy && lab.filledBy.name) || '',
            cap.cmAme != null ? cap.cmAme.toFixed(2) : '',
            cap.cmkAme != null ? cap.cmkAme.toFixed(2) : ''
          ];
        });
        const csv = [headers, ...rows].map(r => r.map(v => `"${v}"`).join(';')).join('\n');
        const blob = new Blob(['\ufeff' + csv], { type: 'text/csv;charset=utf-8' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `leoni_sertissage_${new Date().toISOString().slice(0,10)}.csv`;
        a.click();
        URL.revokeObjectURL(url);
        ui.toast('Export CSV téléchargé', 'success');
      }
    },

    /* ========== QUEUE ========== */
    queue: {
      currentTab: 'all',
      init() {
        $$('.queue-view__tabs .tab').forEach(t => {
          t.addEventListener('click', () => {
            $$('.queue-view__tabs .tab').forEach(x => x.classList.remove('tab--active'));
            t.classList.add('tab--active');
            this.currentTab = t.dataset.tab;
            this.render();
          });
        });
      },
      render() {
        let list = Object.values(state.interventions);
        if (this.currentTab === 'mine') {
          list = list.filter(i => (i.crimping && i.crimping.filledBy && i.crimping.filledBy.uid) === state.user.uid);
        } else if (this.currentTab === 'todo') {
          list = list.filter(i => i.status === 'submitted');
        } else if (this.currentTab === 'rejected') {
          list = list.filter(i => i.status === 'rejected');
        }
        list.sort((a, b) => (b.updatedAt || 0) - (a.updatedAt || 0));
        const grid = $('#queue-cards');
        grid.innerHTML = list.map(i => `
          <article class="card" style="padding:var(--sp-5);cursor:pointer" data-reg="${i.numBon}">
            <header style="display:flex;justify-content:space-between;align-items:center;margin-bottom:var(--sp-3)">
              <strong style="font-size:var(--fs-lg)">N°${i.numBon}</strong>
              <span class="status-pill status-pill--${i.status}">${fmt.statusLabel(i.status)}</span>
            </header>
            <div style="color:var(--text-secondary);font-size:var(--fs-sm)">
              <div>🛠 ${ui.escape((i.tool && i.tool.outilId) || '—')}</div>
              <div>📅 ${fmt.date(i.crimping && i.crimping.date)}</div>
              <div>👤 ${ui.escape((i.crimping && i.crimping.filledBy && i.crimping.filledBy.name) || '—')}</div>
            </div>
          </article>
        `).join('') || '<p style="color:var(--text-muted)">Aucun bon dans cette catégorie</p>';
        grid.querySelectorAll('[data-reg]').forEach(el => {
          el.addEventListener('click', () => views.intervention.open(el.dataset.reg));
        });
      }
    },

    /* ========== CATALOG ========== */
    catalog: {
      init() {
        $('#catalog-search').addEventListener('input', debounce(() => this.render(), 200));
        $('#btn-catalog-add').addEventListener('click', () => this.openModal());

        // ── Import Excel ──
        const btnImport = $('#btn-catalog-import');
        if (btnImport) {
          btnImport.addEventListener('click', () => this.importExcel());
        }

        // ── Export Excel ──
        const btnExport = $('#btn-catalog-export');
        if (btnExport) {
          btnExport.addEventListener('click', () => this.exportExcel());
        }
      },

      /* ── Export Catalogue vers Excel ── */
      async exportExcel() {
        const list = Object.values(state.tools);
        if (!list.length) return ui.toast('Catalogue vide — rien à exporter', 'warn');

        const loadXLSX = () => new Promise((resolve, reject) => {
          if (window.XLSX) return resolve();
          const s = document.createElement('script');
          s.src = 'https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js';
          s.onload = resolve;
          s.onerror = () => reject(new Error('Impossible de charger SheetJS'));
          document.head.appendChild(s);
        });

        ui.showLoader('Export Excel en cours…');
        try {
          await loadXLSX();

          const rows = list.map(t => ({
            'Réf Outil'       : t.refOutil        || '',
            'Outil ID'        : t.outilId         || '',
            'Fabricant'       : t.fabricant       || '',
            'Réf Fabricant'   : t.refFabricant    || '',
            'P-AR'            : t.pAr             || '',
            'P-AV'            : t.pAv             || '',
            'E-AR'            : t.eAr             || '',
            'E-AV'            : t.eAv             || '',
            'Fréquence Cycle' : t.frequenceCycle  || 0,
          }));

          const wb = XLSX.utils.book_new();
          const ws = XLSX.utils.json_to_sheet(rows);

          // Style colonnes largeur auto
          ws['!cols'] = [
            {wch:12},{wch:16},{wch:14},{wch:16},
            {wch:8},{wch:8},{wch:8},{wch:8},{wch:16}
          ];

          XLSX.utils.book_append_sheet(wb, ws, 'Catalogue Outils');

          const date = new Date().toLocaleDateString('fr-FR').replace(/\//g, '-');
          XLSX.writeFile(wb, 'LEONI_Catalogue_Outils_' + date + '.xlsx');

          ui.hideLoader();
          ui.toast(list.length + ' outil(s) exporté(s) ✅', 'success');
        } catch (err) {
          ui.hideLoader();
          ui.toast('Erreur export : ' + err.message, 'danger');
        }
      },

      /* ── Import Excel avec SheetJS ── */
      importExcel() {
        // Charger SheetJS si pas encore chargé
        const loadXLSX = () => new Promise((resolve, reject) => {
          if (window.XLSX) return resolve();
          const s = document.createElement('script');
          s.src = 'https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js';
          s.onload = resolve;
          s.onerror = () => reject(new Error('Impossible de charger SheetJS'));
          document.head.appendChild(s);
        });

        const input = document.createElement('input');
        input.type = 'file';
        input.accept = '.xlsx,.xls,.csv';
        input.style.display = 'none';
        document.body.appendChild(input);

        input.addEventListener('change', async () => {
          const file = input.files[0];
          input.remove();
          if (!file) return;

          ui.showLoader('Lecture du fichier Excel…');
          try {
            await loadXLSX();

            const buffer = await file.arrayBuffer();
            const wb = XLSX.read(buffer, { type: 'array' });
            const ws = wb.Sheets[wb.SheetNames[0]];
            const rows = XLSX.utils.sheet_to_json(ws, { defval: '' });

            if (!rows.length) {
              ui.hideLoader();
              return ui.toast('Fichier vide ou format non reconnu', 'warn');
            }

            // Mapping colonnes → champs (insensible à la casse)
            const MAP = {
              'ref outil'        : 'refOutil',   'réf outil'       : 'refOutil',
              'ref'              : 'refOutil',   'reference'       : 'refOutil',
              'outil id'         : 'outilId',    'outilid'         : 'outilId',
              'id'               : 'outilId',    'outil'           : 'outilId',
              'fabricant'        : 'fabricant',  'manufacturer'    : 'fabricant',
              'ref fabricant'    : 'refFabricant','réf fabricant'  : 'refFabricant',
              'p-ar'             : 'pAr',        'par'             : 'pAr',
              'p-av'             : 'pAv',        'pav'             : 'pAv',
              'e-ar'             : 'eAr',        'ear'             : 'eAr',
              'e-av'             : 'eAv',        'eav'             : 'eAv',
              'frequence cycle'  : 'frequenceCycle', 'fréquence cycle': 'frequenceCycle',
              'cycles'           : 'frequenceCycle', 'frequence'    : 'frequenceCycle',
            };

            const normalize = (k) => (k || '').toString().toLowerCase().trim();

            let imported = 0, skipped = 0;
            for (const row of rows) {
              const tool = {};
              for (const [col, val] of Object.entries(row)) {
                const field = MAP[normalize(col)];
                if (field) tool[field] = val;
              }

              // Fallback: si colonnes non reconnues, essayer par position
              const vals = Object.values(row);
              if (!tool.refOutil && vals[0]) tool.refOutil = String(vals[0]).trim();
              if (!tool.outilId  && vals[1]) tool.outilId  = String(vals[1]).trim();
              if (!tool.fabricant&& vals[2]) tool.fabricant= String(vals[2]).trim();

              if (!tool.refOutil && !tool.outilId) { skipped++; continue; }

              const id = (tool.outilId || tool.refOutil || '')
                .toLowerCase().replace(/[^a-z0-9]/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '');
              tool.id = id || ('tool-' + Date.now() + '-' + imported);
              tool.frequenceCycle = parseInt(tool.frequenceCycle) || 0;
              tool.createdAt = Date.now();
              tool.updatedAt = Date.now();

              await fbDb.ref('tools/' + tool.id).set(tool);
              imported++;
            }

            ui.hideLoader();
            ui.toast(imported + ' outil(s) importé(s) ✅' + (skipped ? ' (' + skipped + ' ignoré(s))' : ''), 'success');
            this.render();

          } catch (err) {
            ui.hideLoader();
            ui.toast('Erreur import : ' + err.message, 'danger');
            console.error('[Import Excel]', err);
          }
        });

        input.click();
      },
      render() {
        const q = $('#catalog-search').value.trim().toLowerCase();
        let list = Object.values(state.tools);
        if (q) list = list.filter(t =>
          (t.refOutil || '').toLowerCase().includes(q) ||
          (t.outilId || '').toLowerCase().includes(q) ||
          (t.fabricant || '').toLowerCase().includes(q)
        );
        const grid = $('#catalog-grid');
        grid.innerHTML = list.slice(0, 100).map(t => `
          <article class="card" style="padding:var(--sp-5)">
            <header style="display:flex;justify-content:space-between;align-items:center;margin-bottom:var(--sp-3)">
              <strong>${ui.escape(t.outilId || t.id)}</strong>
              <span class="badge badge--auto">${ui.escape(t.fabricant || '')}</span>
            </header>
            <div style="color:var(--text-secondary);font-size:var(--fs-sm);font-family:var(--font-mono)">
              <div>Réf: ${ui.escape(t.refOutil || '—')}</div>
              <div>Fab: ${ui.escape(t.refFabricant || '—')}</div>
              <div>Cycles: ${fmt.number(t.frequenceCycle || 0)}</div>
            </div>
          </article>
        `).join('') || '<p style="color:var(--text-muted)">Aucun outil</p>';
      },
      openModal(tool = null) {
        const modal = $('#modal-tool');
        modal.hidden = false;
        $('#tool-modal-title').textContent = tool ? 'Modifier outil' : 'Nouvel outil';
        const form = $('#form-tool');
        form.reset();
        if (tool) {
          Object.entries(tool).forEach(([k, v]) => {
            const inp = form.querySelector(`[name="${k}"]`);
            if (inp) inp.value = v;
          });
        }
        modal.querySelectorAll('[data-close]').forEach(el => {
          el.addEventListener('click', () => { modal.hidden = true; }, { once: true });
        });
        $('#btn-tool-save').onclick = async () => {
          const data = {};
          new FormData(form).forEach((v, k) => { data[k] = v; });
          data.pieces = { pAr: data.pAr, pAv: data.pAv, eAr: data.eAr, eAv: data.eAv };
          await db.saveTool(data);
          modal.hidden = true;
          ui.toast('Outil enregistré', 'success');
        };
      }
    },

    /* ========== USERS (admin) ========== */
    users: {
      _allUsers: {},

      init() {
        $('#btn-user-add').addEventListener('click', () => this.openCreateModal());

        // ── Event delegation pour les boutons du tableau ──
        document.getElementById('users-body').addEventListener('click', (e) => {
          const btn = e.target.closest('button[data-action]');
          if (!btn) return;
          const action = btn.dataset.action;
          const uid    = btn.dataset.uid;
          const email  = btn.dataset.email;
          const active = btn.dataset.active === 'true';
          if (action === 'edit')          this.openEditModal(uid);
          if (action === 'reset-pass')    this.sendPasswordReset(email);
          if (action === 'toggle-active') this.toggleActive(uid, active);
          if (action === 'toggle-pass') {
            const spanId  = btn.dataset.id;
            const passVal = btn.dataset.pass;
            const span    = document.getElementById(spanId);
            if (!span) return;
            const isHidden = span.textContent.includes('•');
            span.textContent = isHidden ? passVal : '••••••••';
            btn.textContent  = isHidden ? '🙈' : '👁';
          }
        });
      },

      async render() {
        if (state.role !== 'admin' && state.role !== 'super_admin' && !auth.can('user.read')) return;
        try {
          const snap = await fbDb.ref('users').once('value');
          this._allUsers = snap.val() || {};
        } catch (e) {
          ui.toast('Erreur chargement utilisateurs : ' + e.message, 'danger');
          return;
        }
        const list = Object.values(this._allUsers);

        if (!list.length) {
          document.getElementById('users-body').innerHTML =
            '<tr><td colspan="8" style="text-align:center;padding:2rem;color:var(--text-muted)">Aucun utilisateur</td></tr>';
          return;
        }

        document.getElementById('users-body').innerHTML = list.map(u => {
          const isActive = u.active !== false;
          const passVal  = u.password ? ui.escape(u.password) : '—';
          const passId   = 'pw-' + (u.uid || '').slice(0, 8);
          return `<tr>
            <td><strong>${ui.escape(u.displayName || '—')}</strong></td>
            <td style="font-family:var(--font-mono);font-size:.8rem">${ui.escape(u.matricule || '—')}</td>
            <td style="font-size:.82rem">${ui.escape(u.email || '—')}</td>
            <td><span class="badge badge--auto">${auth.roleLabel(u.role)}</span></td>
            <td>
              <div style="display:flex;align-items:center;gap:.4rem">
                <span id="${passId}" style="font-family:var(--font-mono);font-size:.78rem;
                  letter-spacing:.06em;background:rgba(255,255,255,.05);padding:.15rem .5rem;
                  border-radius:5px;border:1px solid var(--border-soft)">
                  ••••••••
                </span>
                <button class="btn btn--ghost" style="font-size:.7rem;padding:.18rem .45rem"
                  data-action="toggle-pass" data-pass="${passVal}" data-id="${passId}">👁</button>
              </div>
            </td>
            <td>
              <span style="font-size:.78rem;padding:.2rem .55rem;border-radius:5px;font-weight:600;
                background:${isActive ? 'rgba(16,185,129,.12)' : 'rgba(239,68,68,.12)'};
                color:${isActive ? 'var(--success-500)' : 'var(--danger-500)'}">
                ${isActive ? '✅ Actif' : '⛔ Inactif'}
              </span>
            </td>
            <td style="font-size:.78rem;color:var(--text-muted)">${fmt.dateTime(u.lastLoginAt)}</td>
            <td>
              <div style="display:flex;gap:.35rem;flex-wrap:wrap">
                <button class="btn btn--ghost" style="font-size:.75rem;padding:.28rem .6rem"
                  data-action="edit" data-uid="${ui.escape(u.uid || '')}">✏️ Modifier</button>
                <button class="btn btn--ghost" style="font-size:.75rem;padding:.28rem .6rem;color:var(--warn-500)"
                  data-action="reset-pass" data-email="${ui.escape(u.email || '')}">🔑 Reset MDP</button>
                <button class="btn btn--ghost" style="font-size:.75rem;padding:.28rem .6rem;color:${isActive ? 'var(--danger-500)' : 'var(--success-500)'}"
                  data-action="toggle-active" data-uid="${ui.escape(u.uid || '')}" data-active="${isActive}">
                  ${isActive ? '⛔ Désactiver' : '✅ Activer'}
                </button>
              </div>
            </td>
          </tr>`;
        }).join('');
      },

      /* ── Réinitialisation mot de passe ── */
      async sendPasswordReset(email) {
        if (!email) return;
        const ok = await ui.confirm('Réinitialiser le mot de passe',
          'Envoyer un email de réinitialisation à ' + email + ' ?');
        if (!ok) return;
        try {
          await fbAuth.sendPasswordResetEmail(email);
          ui.toast('Email envoyé à ' + email + ' ✅', 'success');
        } catch (err) {
          ui.toast('Erreur : ' + err.message, 'danger');
        }
      },

      /* ── Activer / Désactiver ── */
      async toggleActive(uid, isCurrentlyActive) {
        if (!uid) return;
        const action = isCurrentlyActive ? 'Désactiver' : 'Activer';
        const ok = await ui.confirm(action + ' le compte', action + ' cet utilisateur ?');
        if (!ok) return;
        try {
          await fbDb.ref('users/' + uid + '/active').set(!isCurrentlyActive);
          ui.toast('Compte ' + (isCurrentlyActive ? 'désactivé ⛔' : 'activé ✅'), 'success');
          this.render();
        } catch (err) {
          ui.toast('Erreur : ' + err.message, 'danger');
        }
      },

      /* ── Modal Modifier ── */
      openEditModal(uid) {
        if (!uid) return;
        const u = this._allUsers[uid];
        if (!u) { ui.toast('Utilisateur introuvable', 'warn'); return; }

        const existing = document.getElementById('modal-user-edit');
        if (existing) existing.remove();

        const div = document.createElement('div');
        div.id = 'modal-user-edit';
        div.className = 'modal';
        div.setAttribute('role', 'dialog');
        div.setAttribute('aria-modal', 'true');
        div.innerHTML = `
          <div class="modal__backdrop"></div>
          <div class="modal__panel">
            <header class="modal__header">
              <h2 class="modal__title">✏️ Modifier — ${ui.escape(u.displayName || u.email)}</h2>
              <button class="modal__close" id="btn-edit-close" aria-label="Fermer">×</button>
            </header>
            <div class="modal__body" style="display:flex;flex-direction:column;gap:.85rem;padding:1.25rem">
              <div class="field">
                <label class="field__label">Nom complet</label>
                <input class="field__input" id="edit-name" value="${ui.escape(u.displayName || '')}" />
              </div>
              <div class="field">
                <label class="field__label">Matricule</label>
                <input class="field__input" id="edit-mat" value="${ui.escape(u.matricule || '')}" />
              </div>
              <div class="field">
                <label class="field__label">Email (lecture seule)</label>
                <input class="field__input" value="${ui.escape(u.email || '')}" disabled
                  style="opacity:.5;cursor:not-allowed" />
              </div>
              <div class="field">
                <label class="field__label">Rôle</label>
                <select class="field__input" id="edit-role">
                  <option value="admin"      ${u.role === 'admin'      ? 'selected' : ''}>🔴 Administrateur</option>
                  <option value="labo"       ${u.role === 'labo'       ? 'selected' : ''}>🔵 Technicien Labo</option>
                  <option value="magasinier" ${u.role === 'magasinier' ? 'selected' : ''}>📦 Magasinier</option>
                  <option value="crimp"      ${u.role === 'crimp'      ? 'selected' : ''}>🟢 Technicien Crimping</option>
                  <option value="responsable"${u.role === 'responsable'? 'selected' : ''}>🟡 Responsable Maintenance</option>
                </select>
              </div>
              <div style="background:rgba(245,158,11,.07);border:1px solid rgba(245,158,11,.25);
                border-radius:10px;padding:1rem 1.1rem">
                <div style="font-size:.82rem;color:var(--warn-500);font-weight:600;margin-bottom:.4rem">
                  🔑 Réinitialiser le mot de passe
                </div>
                <div style="font-size:.8rem;color:var(--text-secondary);margin-bottom:.75rem">
                  Un email sera envoyé à <strong>${ui.escape(u.email)}</strong> pour choisir un nouveau mot de passe.
                </div>
                <button id="btn-send-reset" class="btn btn--ghost"
                  style="width:100%;font-size:.85rem;color:var(--warn-500);border-color:rgba(245,158,11,.3)">
                  📧 Envoyer l'email de réinitialisation
                </button>
              </div>
            </div>
            <footer class="modal__footer">
              <button class="btn btn--ghost" id="btn-edit-cancel">Annuler</button>
              <button class="btn btn--primary" id="btn-edit-save">💾 Enregistrer</button>
            </footer>
          </div>`;

        document.body.appendChild(div);
        div.hidden = false;

        const close = () => { div.hidden = true; div.remove(); };
        document.getElementById('btn-edit-close').addEventListener('click', close);
        document.getElementById('btn-edit-cancel').addEventListener('click', close);
        div.querySelector('.modal__backdrop').addEventListener('click', close);

        document.getElementById('btn-send-reset').addEventListener('click', async () => {
          try {
            await fbAuth.sendPasswordResetEmail(u.email);
            ui.toast('Email envoyé à ' + u.email + ' ✅', 'success');
          } catch (err) {
            ui.toast('Erreur : ' + err.message, 'danger');
          }
        });

        document.getElementById('btn-edit-save').addEventListener('click', async () => {
          const newName = document.getElementById('edit-name').value.trim();
          const newMat  = document.getElementById('edit-mat').value.trim();
          const newRole = document.getElementById('edit-role').value;
          if (!newName) return ui.toast('Le nom est obligatoire', 'warn');
          try {
            await fbDb.ref('users/' + uid).update({
              displayName : newName,
              matricule   : newMat,
              role        : newRole,
              updatedAt   : Date.now()
            });
            ui.toast('Utilisateur mis à jour ✅', 'success');
            close();
            this.render();
          } catch (err) {
            ui.toast('Erreur : ' + err.message, 'danger');
          }
        });
      },

      /* ── Modal Créer ── */
      openCreateModal() {
        const modal = $('#modal-user');
        modal.hidden = false;
        modal.querySelectorAll('[data-close]').forEach(el => {
          el.addEventListener('click', () => { modal.hidden = true; }, { once: true });
        });
        $('#btn-user-save').onclick = async () => {
          const form = $('#form-user');
          const data = {};
          new FormData(form).forEach((v, k) => { data[k] = v; });
          if (!data.email || !data.password || !data.displayName)
            return ui.toast('Tous les champs sont obligatoires', 'warn');
          try {
            const cred = await fbAuth.createUserWithEmailAndPassword(data.email, data.password);
            await fbDb.ref('users/' + cred.user.uid).set({
              uid         : cred.user.uid,
              displayName : data.displayName,
              matricule   : data.matricule || '',
              email       : data.email,
              role        : data.role,
              active      : true,
              password    : data.password,
              createdAt   : Date.now(),
              lastLoginAt : null
            });
            ui.toast('Utilisateur ' + data.displayName + ' créé ✅', 'success');
            modal.hidden = true;
            form.reset();
            this.render();
          } catch (err) {
            ui.toast('Erreur : ' + err.message, 'danger');
          }
        };
      }
    },

    /* ========== AUDIT LOG ========== */
    auditLog: {
      async render() {
        if (state.role !== 'admin' && state.role !== 'super_admin') return;
        const tbody = document.getElementById('audit-body');
        if (!tbody) return;
        tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;padding:1.5rem;color:var(--text-muted)">Chargement…</td></tr>';
        try {
          const snap = await fbDb.ref('auditLog').orderByChild('timestamp').limitToLast(100).once('value');
          const logs = [];
          snap.forEach(c => logs.unshift({ id: c.key, ...c.val() }));
          if (!logs.length) {
            tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;padding:1.5rem;color:var(--text-muted)">Aucune action enregistrée</td></tr>';
            return;
          }
          tbody.innerHTML = logs.map(l => {
            const actionLabels = {
              'intervention.create':'Création bon','intervention.update':'Modification bon',
              'intervention.submit':'Soumission labo','intervention.validate':'Validation',
              'intervention.reject':'Refus','intervention.delete':'Suppression bon'
            };
            const actionColors = {
              'intervention.create':'var(--accent-500)','intervention.validate':'var(--success-500)',
              'intervention.reject':'var(--danger-500)','intervention.delete':'var(--danger-500)',
              'intervention.submit':'var(--warn-500)','intervention.update':'var(--text-secondary)'
            };
            const label = actionLabels[l.action] || l.action;
            const color = actionColors[l.action] || 'var(--text-secondary)';
            return `<tr>
              <td style="color:var(--text-muted);font-size:.78rem;font-family:var(--font-mono)">${fmt.dateTime(l.timestamp)}</td>
              <td style="font-weight:500">${ui.escape(l.userName||'—')}</td>
              <td><span style="color:${color};font-size:.8rem;font-weight:600">${label}</span></td>
              <td style="font-family:var(--font-mono);font-size:.78rem">${ui.escape(l.entity||'—')}</td>
              <td style="font-size:.78rem;color:var(--text-muted)">${ui.escape(l.uid||'—').slice(0,12)}…</td>
            </tr>`;
          }).join('');
        } catch(e) { tbody.innerHTML = `<tr><td colspan="5" style="text-align:center;padding:1.5rem;color:var(--danger-500)">Erreur: ${e.message}</td></tr>`; }
      },
      init() {}
    },

    /* ==========================================================================
       MAGASIN — Pièces & Stock (port from index.html)
       ========================================================================== */
    magasin: {
      pieces: [],
      piecesFile: null,
      piecesXL: false,
      filtered: [],

      // Mapping des colonnes Excel reconnues
      _COLMAP: {
        "N° DE PIÈCE":"npiece","N° DE PIECE":"npiece","N° PIÈCE":"npiece","NPIECE":"npiece",
        "NOM":"nom","NOM 2":"nom2","DÉSIGNATION":"nom2","DESIGNATION":"nom2","DESCRIPTION":"nom2",
        "N° STOCK":"nstock",
        "QUANTITÉ PHYSIQ.":"qty","QUANTITE PHYSIQ.":"qty","QUANTITÉ PHYSIQ":"qty","QUANTITE PHYSIQ":"qty","QUANTITÉ PHYSIQUE":"qty","QTÉ PHYSIQ":"qty","QUANTITÉ":"qty","QUANTITE":"qty","QTY":"qty",
        "QUANTITÉ MINIMUM":"qmin","QUANTITE MINIMUM":"qmin","QTÉ MINIMUM":"qmin","QTE MINIMUM":"qmin",
        "QUANTITÉ MAXIMUM":"qmax","QUANTITE MAXIMUM":"qmax","QTÉ MAXIMUM":"qmax","QTE MAXIMUM":"qmax",
        "CODE FOURNISSEUR":"codeFour","NOM FOURNISSEUR":"nomFour","FOURNISSEUR":"codeFour",
        "EMPLACEM. MAG. PRINCIPAL":"empl","EMPLACEM MAG PRINCIPAL":"empl","EMPLACEMENT MAG PRINCIPAL":"empl","EMPLACEMENT":"empl","EMPLACEM.":"empl",
        "TYPE PIÈCES":"typePiece","TYPE PIECES":"typePiece","TYPE PIÈCE":"typePiece",
        "GROUPE PIÈCES":"groupePiece","GROUPE PIECES":"groupePiece",
        "CODE À BARRES":"barcode","CODE A BARRES":"barcode","CODE BARRES":"barcode",
        "QTÉ. DISPON.":"qdispo","QTE DISPON":"qdispo",
        "QTÉ. RÉSERVÉE":"qreserv","QTE RESERVEE":"qreserv",
        "QTÉ. COMMANDÉE":"qcommand","QTE COMMANDEE":"qcommand",
        "PRIX MOYEN":"prix","DEVISE":"devise"
      },

      _COLS: [
        { k:'nom',         label:'Nom (Référence)',  w:140 },
        { k:'nom2',        label:'Désignation',      w:200 },
        { k:'npiece',      label:'N° Pièce',         w:130 },
        { k:'nstock',      label:'N° Stock',         w:100 },
        { k:'empl',        label:'Emplacement',      w:140 },
        { k:'qty',         label:'Qté Phys.',        w:80, num:true },
        { k:'qmin',        label:'Qté Min',          w:70, num:true },
        { k:'qmax',        label:'Qté Max',          w:70, num:true },
        { k:'qdispo',      label:'Qté Dispon.',      w:90, num:true },
        { k:'qreserv',     label:'Qté Réservée',     w:90, num:true },
        { k:'qcommand',    label:'Qté Cmdée',        w:90, num:true },
        { k:'codeFour',    label:'Code Fourn.',      w:130 },
        { k:'nomFour',     label:'Nom Fourn.',       w:140 },
        { k:'typePiece',   label:'Type Pièce',       w:130 },
        { k:'groupePiece', label:'Groupe',           w:120 },
        { k:'barcode',     label:'Code Barres',      w:130 },
        { k:'prix',        label:'Prix Moyen',       w:90, num:true },
        { k:'devise',      label:'Devise',           w:70 }
      ],

      mapRow(row) {
        const out = {};
        Object.keys(row).forEach(k => {
          const n = String(k).trim().toUpperCase();
          let mk = this._COLMAP[n];
          if (!mk) {
            const f = Object.keys(this._COLMAP).find(c => n.includes(c) || c.includes(n));
            if (f) mk = this._COLMAP[f];
          }
          if (mk) {
            let v = String(row[k] ?? '').trim();
            if ((mk === 'qty' || mk === 'qmin' || mk === 'qmax') && v) {
              const num = parseFloat(v.replace(',', '.'));
              if (!isNaN(num)) v = String(Math.round(num));
            }
            out[mk] = v;
          }
        });
        return out;
      },

      mergePiecesData(rawData) {
        // Fusion basée sur le NOM (référence) — comme demandé par Bilal
        // Chaque pièce avec le même nom regroupe toutes ses occurrences (multi-stocks/emplacements)
        const groups = new Map();
        rawData.forEach(r => {
          const key = (r.nom || '').trim();
          if (!key) return;
          if (groups.has(key)) {
            const g = groups.get(key);
            // Sommer les quantités de tous les stocks/emplacements
            ['qty','qdispo','qreserv','qcommand'].forEach(k => {
              const a = parseInt(g[k]) || 0, b = parseInt(r[k]) || 0;
              g[k] = String(a + b);
            });
            // Max pour qmax (capacité totale)
            const gmax = parseInt(g.qmax) || 0, rmax = parseInt(r.qmax) || 0;
            g.qmax = String(Math.max(gmax, rmax));
            // Somme des quantités min (seuil min global)
            const gmin = parseInt(g.qmin) || 0, rmin = parseInt(r.qmin) || 0;
            g.qmin = String(gmin + rmin);
            // Fusionner les emplacements (avec /)
            if (r.empl && r.empl.trim()) {
              const existing = (g.empl || '').split(' / ').map(s => s.trim()).filter(Boolean);
              const ne = r.empl.trim();
              if (!existing.includes(ne)) { existing.push(ne); g.empl = existing.join(' / '); }
            }
            // Fusionner les n° stock (différents stocks pour une même pièce)
            if (r.nstock && r.nstock.trim()) {
              const existing = (g.nstock || '').split(' / ').map(s => s.trim()).filter(Boolean);
              const ne = r.nstock.trim();
              if (!existing.includes(ne)) { existing.push(ne); g.nstock = existing.join(' / '); }
            }
            // Garder les autres champs depuis la première occurrence
            Object.keys(r).forEach(k => { if (!g[k] && r[k]) g[k] = r[k]; });
            g._count = (g._count || 1) + 1;
          } else {
            groups.set(key, { ...r, _count: 1 });
          }
        });
        return [...groups.values()];
      },

      async _loadXLSX() {
        return new Promise((res, rej) => {
          if (window.XLSX) return res();
          const s = document.createElement('script');
          s.src = 'https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js';
          s.onload = res; s.onerror = rej; document.head.appendChild(s);
        });
      },

      async importExcel(event) {
        if (!auth.can('stock.write')) {
          ui.toast('Import réservé aux admins/magasiniers', 'danger');
          event.target.value = ''; return;
        }
        const file = event.target.files[0];
        if (!file) return;
        await this._loadXLSX();
        const r = new FileReader();
        r.onload = (e) => {
          try {
            const wb = XLSX.read(new Uint8Array(e.target.result), { type:'array' });
            const sn = wb.SheetNames.find(n => /pièce|piece|stock|rechange|spare/i.test(n)) || wb.SheetNames[0];
            const raw = XLSX.utils.sheet_to_json(wb.Sheets[sn], { defval:'', raw:false });
            if (!raw.length) { ui.toast('Fichier vide', 'warn'); return; }
            const mapped = raw.map(row => this.mapRow(row)).filter(r => r.nom || r.empl || r.qty);
            if (!mapped.length) { ui.toast('Aucune colonne reconnue — vérifiez les en-têtes', 'warn'); return; }
            this.pieces = mapped;
            this.piecesXL = true;
            this.piecesFile = { name:file.name, rows:mapped.length, date:new Date().toLocaleDateString('fr-FR'), sheet:sn };
            this.persist();
            this.render();
            event.target.value = '';
            ui.toast('✅ ' + mapped.length + ' pièces importées', 'success');
            // Push to Firebase for cross-device sync
            this.syncToFirebase();
          } catch(err) {
            ui.toast('Erreur : ' + err.message, 'danger');
          }
        };
        r.readAsArrayBuffer(file);
      },

      async syncToFirebase() {
        try {
          await fbDb.ref('stockPieces').set({
            pieces: this.pieces,
            file: this.piecesFile,
            updatedAt: Date.now(),
            updatedBy: state.profile?.displayName || 'system'
          });
          await fbDb.ref('auditLog').push({
            timestamp: Date.now(), uid: state.user.uid,
            userName: state.profile?.displayName || '',
            action: 'stock.import',
            entity: 'stockPieces',
            meta: { rows: this.pieces.length, file: this.piecesFile?.name }
          });
        } catch(e) { console.warn('[Magasin] Firebase sync failed:', e.message); }
      },

      persist() {
        try {
          localStorage.setItem('leoni-stock-pieces', JSON.stringify({
            pieces: this.pieces, file: this.piecesFile, xl: this.piecesXL
          }));
        } catch(e) {}
      },

      restore() {
        try {
          const saved = localStorage.getItem('leoni-stock-pieces');
          if (saved) {
            const data = JSON.parse(saved);
            this.pieces = data.pieces || [];
            this.piecesFile = data.file || null;
            this.piecesXL = data.xl || false;
          }
        } catch(e) {}
      },

      clear() {
        if (!confirm('Effacer toutes les données stock ? Cette action est irréversible.')) return;
        this.pieces = [];
        this.piecesFile = null;
        this.piecesXL = false;
        this.persist();
        this.render();
        // Remove from Firebase
        fbDb.ref('stockPieces').remove().catch(()=>{});
        ui.toast('Stock effacé', 'warn');
      },

      render() {
        if (state.currentView !== 'magasin') return;
        const mergeEl = document.getElementById('pc-merge-toggle');
        // Fusion par défaut activée — basée sur le NOM (référence)
        const isMerged = mergeEl ? mergeEl.checked : true;
        const rawData = this.pieces;
        const data = isMerged ? this.mergePiecesData(rawData) : rawData;
        const hasData = data.length > 0;

        const empty = document.getElementById('pc-empty');
        const zone  = document.getElementById('pc-data-zone');
        const clrBtn = document.getElementById('pc-clr-btn');
        const injBar = document.getElementById('pc-inj-bar');
        const lbl = document.getElementById('pc-lbl');

        if (empty) empty.style.display = hasData ? 'none' : 'block';
        if (zone)  zone.style.display  = hasData ? 'flex' : 'none';
        if (clrBtn) clrBtn.style.display = (hasData && auth.can('stock.write')) ? 'inline-flex' : 'none';

        // Inject info bar
        if (this.piecesFile && hasData) {
          if (injBar) {
            injBar.style.display = 'flex';
            document.getElementById('pc-inj-info').textContent = this.piecesFile.rows + ' pièces depuis "' + this.piecesFile.name + '"';
            document.getElementById('pc-inj-sheet').textContent = 'Feuille : ' + this.piecesFile.sheet + ' · ' + this.piecesFile.date;
          }
          if (lbl) lbl.textContent = '— ' + this.piecesFile.name + ' · ' + this.piecesFile.rows + ' réf.';
        } else {
          if (injBar) injBar.style.display = 'none';
          if (lbl) lbl.textContent = '— Aucun fichier chargé';
        }

        if (!hasData) {
          ['rk1','rk2','rk3','rk4'].forEach(id => { const el = document.getElementById(id); if (el) el.textContent = '—'; });
          return;
        }

        // KPIs
        const total = data.length;
        const empls = new Set(data.map(r => r.empl).filter(Boolean));
        const enStock = data.filter(r => { const q = parseInt(r.qty); return !isNaN(q) && q > 0; }).length;
        const alerts = data.filter(r => { const q = parseInt(r.qty), m = parseInt(r.qmin); return !isNaN(q) && !isNaN(m) && m > 0 && q <= m; }).length;

        document.getElementById('rk1').textContent = total;
        document.getElementById('rk2').textContent = empls.size;
        document.getElementById('rk3').textContent = enStock;
        document.getElementById('rk4').textContent = alerts;

        // Merge info
        if (isMerged) {
          const before = rawData.length;
          const after = data.length;
          const merged = data.filter(r => r._count > 1).length;
          const infoEl = document.getElementById('pc-merge-info');
          if (infoEl) infoEl.textContent = before + ' lignes → ' + after + ' références (' + merged + ' fusionnées)';
        } else {
          const infoEl = document.getElementById('pc-merge-info');
          if (infoEl) infoEl.textContent = '';
        }

        // Sort by 'nom' (Référence) — items with same Nom stay together
        data = data.slice().sort((a, b) => {
          const na = (a.nom || '').toString().trim();
          const nb = (b.nom || '').toString().trim();
          // Empty values go to the bottom
          if (!na && nb) return 1;
          if (na && !nb) return -1;
          // Compare alphabetically/numerically
          return na.localeCompare(nb, 'fr', { numeric: true, sensitivity: 'base' });
        });

        // Build filter dropdowns
        this.buildFilters(data);
        this.buildEmplCards(data);
        this.buildTable(data);

        this.applyFilters();
      },

      buildFilters(data) {
        const empls = [...new Set(data.map(r => r.empl).filter(Boolean))].sort();
        const fours = [...new Set(data.map(r => r.codeFour || r.nomFour).filter(Boolean))].sort();
        const types = [...new Set(data.map(r => r.typePiece || r.groupePiece).filter(Boolean))].sort();

        const fillSelect = (id, items, label) => {
          const sel = document.getElementById(id);
          if (!sel) return;
          const cur = sel.value;
          sel.innerHTML = '<option value="">' + label + '</option>' +
            items.map(v => `<option value="${ui.escape(v)}" ${v===cur?'selected':''}>${ui.escape(v)}</option>`).join('');
        };
        fillSelect('pc-empl-filter', empls, 'Tous emplacements');
        fillSelect('pc-four-filter', fours, 'Tous fournisseurs');
        fillSelect('pc-type-filter', types, 'Tous types');
      },

      buildEmplCards(data) {
        const empls = {};
        data.forEach(r => {
          if (!r.empl) return;
          empls[r.empl] = (empls[r.empl] || 0) + 1;
        });
        const sorted = Object.entries(empls).sort((a,b) => b[1] - a[1]).slice(0, 10);
        const cards = document.getElementById('pc-empl-cards');
        if (!cards) return;
        cards.innerHTML = sorted.map(([e, n]) => `
          <button onclick="document.getElementById('pc-empl-filter').value='${ui.escape(e)}';views.magasin.applyFilters();"
                  style="background:rgba(0,212,255,.06);border:1px solid var(--border-base);border-radius:7px;padding:.4rem .75rem;font-size:.74rem;color:var(--text-secondary);cursor:pointer;display:flex;align-items:center;gap:.4rem;font-family:var(--font-mono)">
            📍 ${ui.escape(e)} <span style="background:var(--accent-500);color:#000;border-radius:9px;padding:.05rem .4rem;font-weight:700;font-size:.68rem">${n}</span>
          </button>
        `).join('');
      },

      buildTable(data) {
        // Show only columns that have data in at least one row
        const cols = this._COLS.filter(c => data.some(r => r[c.k] && String(r[c.k]).trim() !== ''));
        const thead = document.getElementById('pc-thead');
        if (thead) {
          thead.innerHTML = '<tr>' + cols.map(c => `<th style="${c.num?'text-align:right;':''}min-width:${c.w||100}px;white-space:nowrap">${c.label}</th>`).join('') + '<th style="white-space:nowrap">Statut</th></tr>';
        }
        document.getElementById('pc-col-info').textContent = cols.length + ' / ' + this._COLS.length + ' colonnes affichées';
        this._cols = cols;
      },

      applyFilters() {
        const mergeEl2 = document.getElementById('pc-merge-toggle');
        const isMerged = mergeEl2 ? mergeEl2.checked : true;
        let data = isMerged ? this.mergePiecesData(this.pieces) : this.pieces;

        const q = (document.getElementById('pc-search')?.value || '').toLowerCase().trim();
        const fEmpl = document.getElementById('pc-empl-filter')?.value || '';
        const fFour = document.getElementById('pc-four-filter')?.value || '';
        const fType = document.getElementById('pc-type-filter')?.value || '';
        const fEtat = document.getElementById('pc-etat-filter')?.value || '';

        if (q) {
          data = data.filter(r => Object.values(r).some(v => String(v||'').toLowerCase().includes(q)));
        }
        if (fEmpl) data = data.filter(r => (r.empl||'').includes(fEmpl));
        if (fFour) data = data.filter(r => (r.codeFour||r.nomFour||'') === fFour);
        if (fType) data = data.filter(r => (r.typePiece||r.groupePiece||'') === fType);

        if (fEtat === '__alert') data = data.filter(r => { const q = parseInt(r.qty), m = parseInt(r.qmin); return !isNaN(q) && !isNaN(m) && m > 0 && q <= m; });
        if (fEtat === '__zero')  data = data.filter(r => parseInt(r.qty) === 0);
        if (fEtat === '__ok')    data = data.filter(r => { const q = parseInt(r.qty), m = parseInt(r.qmin); return !isNaN(q) && q > 0 && (isNaN(m) || q > m); });

        // Sort filtered data by 'nom' too
        data = data.slice().sort((a, b) => {
          const na = (a.nom || '').toString().trim();
          const nb = (b.nom || '').toString().trim();
          if (!na && nb) return 1;
          if (na && !nb) return -1;
          return na.localeCompare(nb, 'fr', { numeric: true, sensitivity: 'base' });
        });

        this.filtered = data;
        document.getElementById('pc-sct').textContent = data.length + ' réf.';

        this.renderRows(data);
      },

      renderRows(data) {
        const tbody = document.getElementById('pc-tbody');
        if (!tbody) return;
        const cols = this._cols || this._COLS;
        if (!data.length) {
          tbody.innerHTML = '<tr><td colspan="' + (cols.length+1) + '" style="text-align:center;padding:2rem;color:var(--text-muted)">Aucun résultat</td></tr>';
          return;
        }

        // Count occurrences of each 'nom' to highlight grouped rows
        const nomCounts = {};
        data.forEach(r => { const n = (r.nom||'').trim(); if (n) nomCounts[n] = (nomCounts[n]||0) + 1; });

        let prevNom = null;
        tbody.innerHTML = data.slice(0, 500).map(r => {
          const qty = parseInt(r.qty);
          const qmin = parseInt(r.qmin);
          let status = '';
          // Si fusionné depuis plusieurs stocks/emplacements, afficher un badge "fusionné"
          const mergedBadge = (r._count && r._count > 1)
            ? `<span style="display:inline-block;background:rgba(0,212,255,.12);color:var(--accent-500);font-size:.62rem;font-weight:700;padding:.08rem .35rem;border-radius:4px;margin-right:.3rem;border:1px solid rgba(0,212,255,.2)" title="Fusionné depuis ${r._count} stocks">🔗 ${r._count}×</span>`
            : '';
          if (!isNaN(qty)) {
            if (qty === 0) status = mergedBadge + '<span class="badge" style="background:rgba(255,61,90,.15);color:#ff3d5a;border:1px solid rgba(255,61,90,.3);padding:.18rem .55rem;border-radius:5px;font-size:.7rem;font-weight:700">🔴 RUPTURE</span>';
            else if (!isNaN(qmin) && qmin > 0 && qty <= qmin) status = mergedBadge + '<span class="badge" style="background:rgba(255,181,71,.15);color:#ffb547;border:1px solid rgba(255,181,71,.3);padding:.18rem .55rem;border-radius:5px;font-size:.7rem;font-weight:700">⚠️ FAIBLE</span>';
            else status = mergedBadge + '<span class="badge" style="background:rgba(0,229,160,.13);color:#00e5a0;border:1px solid rgba(0,229,160,.28);padding:.18rem .55rem;border-radius:5px;font-size:.7rem;font-weight:700">✅ OK</span>';
          } else status = '<span style="color:var(--text-muted);font-size:.72rem">—</span>';

          // Visual: same 'nom' rows have a left border accent
          const currentNom = (r.nom||'').trim();
          const isFirstOfGroup = currentNom !== prevNom;
          const isInGroup = currentNom && nomCounts[currentNom] > 1;
          prevNom = currentNom;

          // Border: top accent on first occurrence of grouped item
          const rowStyle = isInGroup ? (isFirstOfGroup
            ? 'border-top:2px solid rgba(0,212,255,.3);'
            : 'background:rgba(0,212,255,.025);') : '';

          return '<tr style="' + rowStyle + '">' + cols.map(c => {
            let v = r[c.k] || '';
            const style = c.num ? 'text-align:right;font-family:var(--font-mono)' : '';
            // Highlight nom column if grouped
            const tdExtra = (c.k === 'nom' && isInGroup)
              ? 'border-left:3px solid var(--accent-500);font-weight:700;color:var(--accent-500);'
              : '';
            return `<td style="${style}${tdExtra}">${ui.escape(v)}</td>`;
          }).join('') + '<td>' + status + '</td></tr>';
        }).join('');

        if (data.length > 500) {
          tbody.innerHTML += '<tr><td colspan="' + (cols.length+1) + '" style="text-align:center;padding:.75rem;color:var(--text-muted);font-size:.78rem;font-style:italic">+ ' + (data.length - 500) + ' lignes supplémentaires (filtrer pour affiner)</td></tr>';
        }
      },

      updateStockBadge() {
        const data = this.pieces;
        const alerts = data.filter(r => {
          const q = parseInt(r.qty), m = parseInt(r.qmin);
          return !isNaN(q) && (q === 0 || (!isNaN(m) && m > 0 && q <= m));
        }).length;
        const badge = document.getElementById('stock-alert-badge');
        if (badge) {
          badge.textContent = alerts;
          badge.hidden = alerts === 0;
        }
      },

      async loadFromFirebase() {
        try {
          const snap = await fbDb.ref('stockPieces').once('value');
          const data = snap.val();
          if (data && data.pieces && Array.isArray(data.pieces)) {
            this.pieces = data.pieces;
            this.piecesFile = data.file;
            this.piecesXL = true;
            this.persist();
            this.updateStockBadge();
            if (state.currentView === 'magasin') this.render();
          }
        } catch(e) { console.warn('[Magasin] Cannot load from Firebase:', e.message); }
      },

      init() {
        this.restore();
        this.loadFromFirebase();

        // Imports
        const i1 = document.getElementById('pc-xl-input');
        const i2 = document.getElementById('pc-xl-input-empty');
        if (i1) i1.addEventListener('change', e => this.importExcel(e));
        if (i2) i2.addEventListener('change', e => this.importExcel(e));

        // Buttons
        document.getElementById('pc-clr-btn')?.addEventListener('click', () => this.clear());
        document.getElementById('pc-refresh-btn')?.addEventListener('click', () => this.render());

        // Filters
        document.getElementById('pc-search')?.addEventListener('input', () => this.applyFilters());
        document.getElementById('pc-empl-filter')?.addEventListener('change', () => this.applyFilters());
        document.getElementById('pc-four-filter')?.addEventListener('change', () => this.applyFilters());
        document.getElementById('pc-type-filter')?.addEventListener('change', () => this.applyFilters());
        document.getElementById('pc-etat-filter')?.addEventListener('change', () => this.applyFilters());

        // Merge toggle
        document.getElementById('pc-merge-toggle')?.addEventListener('change', () => this.render());
      },

      renderMovements() { /* compat */ }
    },

        /* ==========================================================================
       PERFORMANCE — Diagrammes temps techniciens
       ========================================================================== */
    performance: {
      _charts: {},

      render() {
        if (state.currentView !== 'performance') return;
        const period = document.getElementById('perf-period')?.value || 'month';
        const since = this._periodStart(period);
        const list = Object.values(state.interventions || {})
          .filter(i => i.crimping?.duration && (i.createdAt||0) >= since);

        // Group by technician
        const byTech = {};
        list.forEach(i => {
          const name = i.crimping?.filledBy?.name || 'Inconnu';
          const mat  = i.crimping?.filledBy?.matricule || '';
          const key = name + '|' + mat;
          if (!byTech[key]) byTech[key] = { name, matricule: mat, total: 0, count: 0, durations: [] };
          byTech[key].total += i.crimping.duration;
          byTech[key].count++;
          byTech[key].durations.push(i.crimping.duration);
        });
        const techs = Object.values(byTech).sort((a,b) => b.total - a.total);

        // KPIs
        const totalMin = list.reduce((s,i) => s + (i.crimping.duration || 0), 0);
        const avgMin = list.length ? Math.round(totalMin / list.length) : 0;
        const top = techs[0];

        document.getElementById('perf-kpi-total').textContent = this._fmtMin(totalMin);
        document.getElementById('perf-kpi-avg').textContent   = this._fmtMin(avgMin);
        document.getElementById('perf-kpi-techs').textContent = techs.length;
        document.getElementById('perf-kpi-top').textContent   = top ? top.name.split(' ')[0] : '—';
        document.getElementById('perf-kpi-top-h').textContent = top ? this._fmtMin(top.total) : '—';

        // Tech detail table
        const tbody = document.getElementById('perf-tech-body');
        if (tbody) {
          tbody.innerHTML = techs.length ? techs.map(t => {
            const max = Math.max(...t.durations);
            const min = Math.min(...t.durations);
            const avg = Math.round(t.total / t.count);
            return `<tr>
              <td style="font-weight:500">${ui.escape(t.name)}</td>
              <td style="font-family:var(--font-mono);font-size:.78rem;color:var(--text-muted)">${ui.escape(t.matricule||'—')}</td>
              <td style="text-align:right;font-family:var(--font-mono);font-weight:600">${t.count}</td>
              <td style="text-align:right;font-family:var(--font-mono);color:var(--accent-500);font-weight:700">${this._fmtMin(t.total)}</td>
              <td style="text-align:right;font-family:var(--font-mono)">${this._fmtMin(avg)}</td>
              <td style="text-align:right;font-family:var(--font-mono);color:var(--danger-500)">${this._fmtMin(max)}</td>
              <td style="text-align:right;font-family:var(--font-mono);color:var(--success-500)">${this._fmtMin(min)}</td>
            </tr>`;
          }).join('') : '<tr><td colspan="7" style="text-align:center;padding:2rem;color:var(--text-muted)">Aucune donnée — aucun technicien n\'a renseigné de temps d\'intervention sur cette période</td></tr>';
        }

        // Charts
        this._loadChart().then(() => this._renderCharts(techs, list));
      },

      _periodStart(p) {
        const now = Date.now();
        if (p === 'week')    return now - 7*86400000;
        if (p === 'month')   return now - 30*86400000;
        if (p === 'quarter') return now - 90*86400000;
        return 0;
      },

      _fmtMin(min) {
        if (!min) return '0min';
        const h = Math.floor(min/60), m = min%60;
        return h > 0 ? `${h}h${m > 0 ? ' ' + m + 'min' : ''}` : m + 'min';
      },

      _renderCharts(techs, list) {
        const COL = ['#00d4ff','#00e5a0','#ffb547','#ff3d5a','#8b5cf6','#06b6d4','#ec4899','#f59e0b'];
        const labels = techs.map(t => t.name.split(' ')[0]);
        const totals = techs.map(t => Math.round(t.total / 60 * 10) / 10);  // hours
        const avgs   = techs.map(t => Math.round(t.total / t.count));        // minutes

        const baseOpts = {
          responsive: true, maintainAspectRatio: false,
          plugins: { legend: { display: false } },
          scales: {
            x: { ticks: { color: '#7a9bb5', font:{size:11} }, grid: { color: 'rgba(0,212,255,.04)' } },
            y: { ticks: { color: '#7a9bb5', font:{size:11} }, grid: { color: 'rgba(0,212,255,.06)' }, beginAtZero: true }
          }
        };

        // Chart 1: Total hours
        const c1 = document.getElementById('chart-perf-total');
        if (c1) {
          if (this._charts.total) this._charts.total.destroy();
          this._charts.total = new Chart(c1, {
            type: 'bar',
            data: { labels, datasets: [{ label: 'Heures', data: totals,
              backgroundColor: labels.map((_,i) => COL[i%COL.length] + '99'),
              borderColor:    labels.map((_,i) => COL[i%COL.length]),
              borderWidth: 1, borderRadius: 6 }] },
            options: { ...baseOpts, scales: { ...baseOpts.scales, y: { ...baseOpts.scales.y,
              title: { display: true, text: 'Heures', color: '#7a9bb5' } } } }
          });
        }

        // Chart 2: Average per intervention
        const c2 = document.getElementById('chart-perf-avg');
        if (c2) {
          if (this._charts.avg) this._charts.avg.destroy();
          this._charts.avg = new Chart(c2, {
            type: 'bar',
            data: { labels, datasets: [{ label: 'Minutes', data: avgs,
              backgroundColor: 'rgba(0,212,255,.55)',
              borderColor: '#00d4ff', borderWidth: 1, borderRadius: 6 }] },
            options: { ...baseOpts, scales: { ...baseOpts.scales, y: { ...baseOpts.scales.y,
              title: { display: true, text: 'Minutes par bon', color: '#7a9bb5' } } } }
          });
        }

        // Chart 3: Trend by week
        const c3 = document.getElementById('chart-perf-trend');
        if (c3) {
          if (this._charts.trend) this._charts.trend.destroy();
          // Group by week, by tech (top 5)
          const weeks = 12;
          const weekLabels = [];
          for (let w = weeks-1; w >= 0; w--) {
            const e = new Date(); e.setDate(e.getDate() - w*7);
            const s = new Date(e); s.setDate(s.getDate() - 7);
            weekLabels.push(s.toLocaleDateString('fr-FR',{day:'2-digit',month:'2-digit'}));
          }
          const top5 = techs.slice(0, 5);
          const datasets = top5.map((t, idx) => {
            const data = [];
            for (let w = weeks-1; w >= 0; w--) {
              const e = new Date(); e.setDate(e.getDate() - w*7);
              const s = new Date(e); s.setDate(s.getDate() - 7);
              const wMin = list.filter(i =>
                (i.createdAt||0) >= s.getTime() && (i.createdAt||0) < e.getTime() &&
                (i.crimping?.filledBy?.name === t.name)
              ).reduce((sum,i) => sum + (i.crimping.duration||0), 0);
              data.push(Math.round(wMin/60 * 10) / 10);
            }
            return {
              label: t.name.split(' ')[0],
              data, borderColor: COL[idx%COL.length],
              backgroundColor: COL[idx%COL.length] + '22',
              borderWidth: 2, tension: 0.4, fill: false
            };
          });
          this._charts.trend = new Chart(c3, {
            type: 'line',
            data: { labels: weekLabels, datasets },
            options: { responsive: true, maintainAspectRatio: false,
              plugins: { legend: { labels: { color: '#7a9bb5', font:{size:11} } } },
              scales: {
                x: { ticks: { color: '#7a9bb5', font:{size:10} }, grid: { color: 'rgba(0,212,255,.04)' } },
                y: { ticks: { color: '#7a9bb5', font:{size:11} }, grid: { color: 'rgba(0,212,255,.06)' },
                     beginAtZero: true, title: { display: true, text: 'Heures', color: '#7a9bb5' } } }
            }
          });
        }
      },

      _loadChart() {
        return new Promise((res, rej) => {
          if (window.Chart) return res();
          const s = document.createElement('script');
          s.src = 'https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js';
          s.onload = res; s.onerror = rej; document.head.appendChild(s);
        });
      },

      init() {
        $('#perf-period')?.addEventListener('change', () => this.render());
        $('#btn-perf-export')?.addEventListener('click', () => this.exportExcel());
      },

      async exportExcel() {
        const period = document.getElementById('perf-period')?.value || 'month';
        const since = this._periodStart(period);
        const list = Object.values(state.interventions || {})
          .filter(i => i.crimping?.duration && (i.createdAt||0) >= since);
        if (!list.length) return ui.toast('Aucune donnée à exporter', 'warn');

        await views.magasin._loadXLSX();
        const rows = list.map(i => ({
          'N° Bon': i.numBon,
          'Outil': i.tool?.outilId || '',
          'Technicien': i.crimping?.filledBy?.name || '',
          'Matricule': i.crimping?.filledBy?.matricule || '',
          'Date': fmt.date(i.crimping?.date || i.createdAt),
          'Type': i.crimping?.type || '',
          'Durée (min)': i.crimping?.duration || 0,
          'Durée (h)': ((i.crimping?.duration || 0) / 60).toFixed(2),
        }));
        const wb = XLSX.utils.book_new();
        const ws = XLSX.utils.json_to_sheet(rows);
        XLSX.utils.book_append_sheet(wb, ws, 'Performance');
        XLSX.writeFile(wb, 'LEONI_Performance_' + new Date().toISOString().slice(0,10) + '.xlsx');
        ui.toast('Export ✅', 'success');
      }
    },

    /* ========== STATS ========== */
    stats: {
      render() {
        const loadChart = () => new Promise((res,rej) => {
          if (window.Chart) return res();
          const s = document.createElement('script');
          s.src = 'https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js';
          s.onload = res; s.onerror = rej; document.head.appendChild(s);
        });

        loadChart().then(() => {
          const days = parseInt(document.getElementById('stats-period')?.value || 30);
          const since = Date.now() - days * 86400000;
          const list = Object.values(state.interventions).filter(i => (i.createdAt||0) >= since);

          const neutral = 'rgba(255,255,255,.06)';
          const textColor = '#8895b3';
          const gridColor = 'rgba(255,255,255,.04)';

          // Chart 1: Répartition statuts
          const c1 = document.getElementById('chart-status-distribution');
          if (c1) {
            if (c1._chart) c1._chart.destroy();
            const statuses = { validated:0, submitted:0, rejected:0, draft:0 };
            list.forEach(i => { if (statuses[i.status] !== undefined) statuses[i.status]++; });
            c1._chart = new Chart(c1, {
              type: 'doughnut',
              data: { labels: ['Validés','En attente','Refusés','Brouillons'],
                datasets: [{ data: Object.values(statuses),
                  backgroundColor: ['#10b981','#f59e0b','#ef4444','#6b7388'],
                  borderWidth: 0, hoverOffset: 6 }] },
              options: { responsive: true, maintainAspectRatio: false, cutout: '65%',
                plugins: { legend: { position: 'bottom', labels: { color: textColor, padding: 12, font: { size: 12 } } } } }
            });
          }

          // Chart 2: SLA trend (temps moyen par semaine)
          const c2 = document.getElementById('chart-sla-trend');
          if (c2) {
            if (c2._chart) c2._chart.destroy();
            const wks = 8; const wlabels = []; const wavg = [];
            for (let w = wks-1; w >= 0; w--) {
              const wEnd = new Date(); wEnd.setDate(wEnd.getDate() - w*7);
              const wStart = new Date(wEnd); wStart.setDate(wStart.getDate() - 7);
              wlabels.push(wStart.toLocaleDateString('fr-FR',{day:'2-digit',month:'2-digit'}));
              const wItems = list.filter(i => i.sla && i.sla.durationMs && i.createdAt >= wStart.getTime() && i.createdAt < wEnd.getTime());
              const avg = wItems.length ? Math.round(wItems.reduce((s,i) => s + i.sla.durationMs, 0) / wItems.length / 3600000 * 10) / 10 : 0;
              wavg.push(avg);
            }
            c2._chart = new Chart(c2, {
              type: 'line',
              data: { labels: wlabels, datasets: [{ label: 'SLA moyen (h)', data: wavg,
                borderColor: '#3b82f6', backgroundColor: 'rgba(59,130,246,.1)', borderWidth: 2,
                pointBackgroundColor: '#3b82f6', fill: true, tension: 0.4 }] },
              options: { responsive: true, maintainAspectRatio: false,
                plugins: { legend: { labels: { color: textColor, font: { size: 12 } } } },
                scales: { x: { ticks: { color: textColor, font:{size:11} }, grid: { color: gridColor } },
                  y: { ticks: { color: textColor, font:{size:11} }, grid: { color: gridColor }, beginAtZero: true } } }
            });
          }

          // Chart 3: Top outils
          const c3 = document.getElementById('chart-tools-ranking');
          if (c3) {
            if (c3._chart) c3._chart.destroy();
            const toolCounts = {};
            list.forEach(i => { const k = (i.tool && i.tool.outilId) || 'Inconnu'; toolCounts[k] = (toolCounts[k]||0)+1; });
            const sorted = Object.entries(toolCounts).sort((a,b)=>b[1]-a[1]).slice(0,6);
            c3._chart = new Chart(c3, {
              type: 'bar',
              data: { labels: sorted.map(e=>e[0]), datasets: [{ label: 'Interventions', data: sorted.map(e=>e[1]),
                backgroundColor: 'rgba(16,185,129,.5)', borderColor: '#10b981', borderWidth: 1, borderRadius: 4 }] },
              options: { indexAxis:'y', responsive: true, maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: { x: { ticks: { color: textColor, font:{size:11} }, grid: { color: gridColor } },
                  y: { ticks: { color: textColor, font:{size:11} }, grid: { color: gridColor } } } }
            });
          }

          // Chart 4: Performance techniciens
          const c4 = document.getElementById('chart-tech-performance');
          if (c4) {
            if (c4._chart) c4._chart.destroy();
            const techCounts = {};
            list.forEach(i => { const n = (i.crimping && i.crimping.filledBy && i.crimping.filledBy.name) || 'Inconnu'; techCounts[n] = (techCounts[n]||0)+1; });
            const tsorted = Object.entries(techCounts).sort((a,b)=>b[1]-a[1]).slice(0,5);
            c4._chart = new Chart(c4, {
              type: 'bar',
              data: { labels: tsorted.map(e=>e[0].split(' ')[0]), datasets: [{ label: 'Bons créés', data: tsorted.map(e=>e[1]),
                backgroundColor: ['rgba(245,158,11,.5)','rgba(99,102,241,.5)','rgba(16,185,129,.5)','rgba(239,68,68,.5)','rgba(59,130,246,.5)'],
                borderWidth: 0, borderRadius: 4 }] },
              options: { responsive: true, maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: { x: { ticks: { color: textColor, font:{size:11} }, grid: { color: gridColor } },
                  y: { ticks: { color: textColor, font:{size:11}, stepSize:1 }, grid: { color: gridColor }, beginAtZero: true } } }
            });
          }

        }).catch(err => console.error('Chart.js load error:', err));
      }
    },

    /* ========== NOTIFICATIONS PANEL ========== */
    notifications: {
      init() {
        $('#btn-notifications').addEventListener('click', () => {
          $('#notif-panel').hidden = false;
        });
        $('#btn-notif-close').addEventListener('click', () => {
          $('#notif-panel').hidden = true;
        });
        $('#btn-notif-clear').addEventListener('click', async () => {
          if (!state.user) return;
          const ref = fbDb.ref(`notifications/${state.user.uid}`);
          const snap = await ref.once('value');
          const updates = {};
          snap.forEach(c => { updates[`${c.key}/read`] = true; });
          await ref.update(updates);
        });
      },
      render(notifs) {
        $('#notif-list').innerHTML = notifs.map(n => `
          <li style="padding:var(--sp-3);border-bottom:1px solid var(--border-soft);${n.read ? 'opacity:.6' : ''}">
            <div style="font-size:var(--fs-sm)">${ui.escape(n.message)}</div>
            <div style="font-size:var(--fs-xs);color:var(--text-muted);margin-top:var(--sp-1)">${fmt.dateTime(n.createdAt)}</div>
          </li>
        `).join('') || '<li style="padding:var(--sp-4);color:var(--text-muted);text-align:center">Aucune notification</li>';
      }
    }
  };

  /* ==========================================================================
     14. APP CORE
     ========================================================================== */
  const app = {
    async loadInitialData() {
      ui.showLoader('Chargement des données…');
      db.listenTools();
      db.listenInterventions();
      db.listenStock();
      db.listenNotifications();
      ui.hideLoader();
    },

    detachListeners() {
      state.listeners.forEach(off => { try { off(); } catch (e) {} });
      state.listeners = [];
    },

    initUI() {

      // Global search
      const searchInput = document.getElementById('global-search');
      if (searchInput) {
        searchInput.addEventListener('input', debounce((e) => {
          const q = e.target.value.trim().toLowerCase();
          if (!q) { const dd = document.getElementById('search-dropdown'); if (dd) dd.hidden = true; return; }
          const results = [];
          Object.values(state.interventions).forEach(i => {
            if (String(i.numBon).includes(q) ||
                (i.tool && (i.tool.outilId||'').toLowerCase().includes(q)) ||
                (i.crimping && i.crimping.filledBy && (i.crimping.filledBy.name||'').toLowerCase().includes(q))) {
              results.push({ type:'bon', label: `N°${i.numBon} · ${(i.tool&&i.tool.outilId)||'—'} · ${fmt.statusLabel(i.status)}`, id: i.numBon });
            }
          });
          Object.values(state.tools).forEach(t => {
            if ((t.outilId||'').toLowerCase().includes(q) || (t.refOutil||'').toLowerCase().includes(q) || (t.fabricant||'').toLowerCase().includes(q)) {
              results.push({ type:'tool', label: `🛠 ${t.outilId||t.id} — ${t.fabricant||''}`, id: t.id });
            }
          });
          let dd = document.getElementById('search-dropdown');
          if (!dd) {
            dd = document.createElement('ul');
            dd.id = 'search-dropdown';
            dd.style.cssText = 'position:absolute;top:100%;left:0;right:0;background:var(--bg-3);border:1px solid var(--border-base);border-radius:8px;list-style:none;max-height:280px;overflow-y:auto;z-index:9999;box-shadow:0 8px 24px rgba(0,0,0,.4);margin-top:4px;';
            searchInput.parentElement.style.position = 'relative';
            searchInput.parentElement.appendChild(dd);
          }
          dd.innerHTML = results.slice(0,8).map(r => `<li data-type="${r.type}" data-id="${r.id}" style="padding:.6rem 1rem;cursor:pointer;font-size:.85rem;color:var(--text-primary);border-bottom:1px solid var(--border-soft)">${r.label}</li>`).join('') ||
            '<li style="padding:.6rem 1rem;color:var(--text-muted);font-size:.85rem">Aucun résultat</li>';
          dd.hidden = false;
          dd.querySelectorAll('li[data-id]').forEach(li => {
            li.addEventListener('click', () => {
              if (li.dataset.type === 'bon') { views.intervention.open(li.dataset.id); dd.hidden = true; searchInput.value = ''; }
              if (li.dataset.type === 'tool') { router.go('catalog'); dd.hidden = true; searchInput.value = ''; }
            });
            li.addEventListener('mouseenter', () => li.style.background = 'var(--bg-4)');
            li.addEventListener('mouseleave', () => li.style.background = '');
          });
        }, 200));
        document.addEventListener('click', (e) => {
          if (!searchInput.contains(e.target)) {
            const dd = document.getElementById('search-dropdown'); if (dd) dd.hidden = true;
          }
        });
      }


      // User menu dropdown
      $('#btn-user').addEventListener('click', (e) => {
        e.stopPropagation();
        $('#user-dropdown').hidden = !$('#user-dropdown').hidden;
      });
      document.addEventListener('click', () => { $('#user-dropdown').hidden = true; });
      $('#user-dropdown').addEventListener('click', (e) => e.stopPropagation());
      $$('#user-dropdown button').forEach(b => {
        b.addEventListener('click', () => {
          const action = b.dataset.action;
          $('#user-dropdown').hidden = true;
          if (action === 'logout')   auth.logout();
          if (action === 'profile')  userActions.openProfile();
          if (action === 'password') userActions.openPassword();
        });
      });

      // Mobile menu
      $('#btn-menu').addEventListener('click', () => {
        $('#sidebar').classList.toggle('is-open');
        document.getElementById('shell').classList.toggle('menu-open');
      });
      // Close on overlay click
      document.getElementById('shell').addEventListener('click', (e) => {
        if (e.target === document.getElementById('shell') ||
            e.target.classList.contains('shell')) {
          $('#sidebar').classList.remove('is-open');
          document.getElementById('shell').classList.remove('menu-open');
        }
      });

      // Theme toggle — persistent
      const savedTheme = localStorage.getItem('leoni-theme') ||
        (window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
      document.body.dataset.theme = savedTheme;
      document.getElementById('btn-theme').textContent = savedTheme === 'dark' ? '◐' : '●';
      document.getElementById('btn-theme').addEventListener('click', () => {
        const next = document.body.dataset.theme === 'dark' ? 'light' : 'dark';
        document.body.dataset.theme = next;
        localStorage.setItem('leoni-theme', next);
        document.getElementById('btn-theme').textContent = next === 'dark' ? '◐' : '●';
      });
    }
  };


  /* ==========================================================================
     USER ACTIONS — Mon Profil + Changer Mot de Passe
     ========================================================================== */
  const userActions = {

    /* ── PROFIL ── */
    openProfile() {
      const p = state.profile;
      if (!p) return ui.toast('Profil non chargé', 'warn');

      $('#profile-displayname').value    = p.displayName   || '';
      $('#profile-matricule').value      = p.matricule      || '';
      $('#profile-email').value          = p.email          || state.user?.email || '';
      $('#profile-email-display').textContent = p.email    || state.user?.email || '—';
      $('#profile-name-display').textContent  = p.displayName || '—';
      $('#profile-role-display').textContent  = auth.roleLabel(p.role);
      $('#profile-lastlogin').textContent     = fmt.dateTime(p.lastLoginAt) || 'Inconnue';

      const letter = (p.displayName || '?').charAt(0).toUpperCase();
      $('#profile-avatar-big').textContent = letter;
      $('#modal-profile').hidden = false;
    },

    async saveProfile() {
      const btn  = $('#btn-profile-save');
      const name = $('#profile-displayname').value.trim();
      const mat  = $('#profile-matricule').value.trim();
      if (!name) return ui.toast('Le nom ne peut pas être vide', 'warn');

      btn.disabled = true; btn.textContent = 'Enregistrement…';
      try {
        const uid = state.user.uid;
        await fbDb.ref('users/' + uid).update({ displayName: name, matricule: mat });
        // Mettre à jour Firebase Auth displayName
        await fbAuth.currentUser.updateProfile({ displayName: name });
        // Mettre à jour le profil local
        state.profile.displayName = name;
        state.profile.matricule   = mat;
        // Mettre à jour la topbar
        $('#user-name').textContent   = name;
        $('#user-avatar').textContent = name.charAt(0).toUpperCase();
        $('#hub-greeting').textContent = `Bienvenue ${name} — ${auth.roleLabel(state.profile.role)}`;
        $('#modal-profile').hidden = true;
        ui.toast('Profil mis à jour ✅', 'success');
        // Audit log
        await fbDb.ref('auditLog').push({ timestamp: Date.now(), uid, userName: name, action: 'profile.update', entity: 'users/'+uid });
      } catch(e) {
        ui.toast('Erreur : ' + e.message, 'danger');
      } finally {
        btn.disabled = false; btn.textContent = '💾 Enregistrer';
      }
    },

    /* ── MOT DE PASSE ── */
    openPassword() {
      $('#pass-current').value  = '';
      $('#pass-new').value      = '';
      $('#pass-confirm').value  = '';
      $('#password-error').style.display = 'none';
      // Reset force bars
      ['psb1','psb2','psb3','psb4'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.style.background = 'var(--border-soft)';
      });
      document.getElementById('pass-strength-label').textContent = '';
      $('#modal-password').hidden = false;

      // Live strength meter
      $('#pass-new').oninput = () => userActions.checkStrength($('#pass-new').value);
    },

    checkStrength(pw) {
      let score = 0;
      if (pw.length >= 8)  score++;
      if (/[A-Z]/.test(pw)) score++;
      if (/[0-9]/.test(pw)) score++;
      if (/[^A-Za-z0-9]/.test(pw)) score++;

      const colors = ['#ef4444','#f59e0b','#3b82f6','#10b981'];
      const labels = ['Très faible','Faible','Moyen','Fort'];
      ['psb1','psb2','psb3','psb4'].forEach((id, i) => {
        const el = document.getElementById(id);
        if (el) el.style.background = i < score ? colors[score-1] : 'var(--border-soft)';
      });
      const lbl = document.getElementById('pass-strength-label');
      if (lbl) { lbl.textContent = pw ? labels[score-1] || 'Fort' : ''; lbl.style.color = colors[score-1] || '#10b981'; }
    },

    async savePassword() {
      const current = $('#pass-current').value;
      const newPw   = $('#pass-new').value;
      const confirm = $('#pass-confirm').value;
      const errEl   = $('#password-error');

      const showErr = (msg) => { errEl.textContent = msg; errEl.style.display = 'block'; };
      errEl.style.display = 'none';

      if (!current)          return showErr('Veuillez entrer votre mot de passe actuel.');
      if (newPw.length < 8)  return showErr('Le nouveau mot de passe doit faire au moins 8 caractères.');
      if (newPw !== confirm)  return showErr('Les deux nouveaux mots de passe ne correspondent pas.');
      if (current === newPw)  return showErr('Le nouveau mot de passe doit \u00eatre diff\u00e9rent de l\u2019actuel.');

      const btn = $('#btn-password-save');
      btn.disabled = true; btn.textContent = 'Modification…';
      try {
        // Ré-authentifier puis changer
        const user       = fbAuth.currentUser;
        const credential = firebase.auth.EmailAuthProvider.credential(user.email, current);
        await user.reauthenticateWithCredential(credential);
        await user.updatePassword(newPw);

        // Sauvegarder dans RTDB aussi (pour l'admin)
        await fbDb.ref('users/' + user.uid + '/password').set(newPw);

        $('#modal-password').hidden = true;
        ui.toast('Mot de passe modifié avec succès ✅', 'success');
        await fbDb.ref('auditLog').push({ timestamp: Date.now(), uid: user.uid, userName: state.profile?.displayName || '', action: 'password.change', entity: 'auth/'+user.uid });
      } catch(e) {
        const msgs = {
          'auth/wrong-password': 'Mot de passe actuel incorrect.',
          'auth/too-many-requests': 'Trop de tentatives. Réessayez plus tard.',
          'auth/requires-recent-login': 'Session expirée. Reconnectez-vous d’abord.',
        };
        showErr(msgs[e.code] || 'Erreur : ' + e.message);
      } finally {
        btn.disabled = false; btn.textContent = '🔑 Modifier';
      }
    },

    /* ── Fermeture modals ── */
    initModals() {
      // Profile
      $('#btn-profile-close')?.addEventListener('click', () => $('#modal-profile').hidden = true);
      $('#btn-profile-cancel')?.addEventListener('click', () => $('#modal-profile').hidden = true);
      $('#btn-profile-save')?.addEventListener('click', () => userActions.saveProfile());
      $('#modal-profile .modal__backdrop')?.addEventListener('click', () => $('#modal-profile').hidden = true);

      // Password
      $('#btn-password-close')?.addEventListener('click', () => $('#modal-password').hidden = true);
      $('#btn-password-cancel')?.addEventListener('click', () => $('#modal-password').hidden = true);
      $('#btn-password-save')?.addEventListener('click', () => userActions.savePassword());
      $('#modal-password .modal__backdrop')?.addEventListener('click', () => $('#modal-password').hidden = true);
    }
  };

  /* Helper: toggle password visibility */
  function togglePassVis(inputId, btn) {
    const inp = document.getElementById(inputId);
    if (!inp) return;
    inp.type = inp.type === 'password' ? 'text' : 'password';
    btn.textContent = inp.type === 'password' ? '👁' : '🙈';
  }


  /* ==========================================================================
     TIME INTERVENTION FIELD — Saisie du temps par technicien
     ========================================================================== */
  function initDurationField() {
    const btn = document.getElementById('btn-save-duration');
    const input = document.getElementById('int-duration');
    const display = document.getElementById('int-duration-display');

    if (input && display) {
      input.addEventListener('input', () => {
        const min = parseInt(input.value) || 0;
        if (min > 0) {
          const h = Math.floor(min/60);
          const m = min % 60;
          display.textContent = h > 0 ? `≈ ${h}h ${m}min` : `≈ ${m}min`;
        } else { display.textContent = '—'; }
      });
    }

    if (btn) btn.addEventListener('click', async () => {
      const reg = state.currentInterventionId;
      if (!reg) return ui.toast('Ouvrez un bon d\u2019intervention', 'warn');
      const min = parseInt(document.getElementById('int-duration').value);
      if (!min || min < 1) return ui.toast('Saisissez un temps valide en minutes', 'warn');
      try {
        await fbDb.ref('interventions/' + reg + '/crimping/duration').set(min);
        await fbDb.ref('auditLog').push({
          timestamp: Date.now(), uid: state.user.uid, userName: state.profile?.displayName || '',
          action: 'intervention.duration', entity: 'interventions/' + reg, meta: { duration: min }
        });
        ui.toast('Temps enregistré : ' + min + ' min ✅', 'success');
      } catch(e) { ui.toast('Erreur: ' + e.message, 'danger'); }
    });
  }

  /* ==========================================================================
     15. BOOTSTRAP
     ========================================================================== */
  document.addEventListener('DOMContentLoaded', () => {
    initFirebase();
    auth.init();
    app.initUI();
    router.init();
    views.intervention.init();
    views.history.init();
    views.queue.init();
    views.catalog.init();
    views.users.init();
    views.notifications.init();
    views.auditLog.init();
    views.magasin.init();
    views.performance.init();
    userActions.initModals();
    initDurationField();

    // Stats period change triggers re-render
    const statsPeriod = document.getElementById('stats-period');
    if (statsPeriod) statsPeriod.addEventListener('change', () => views.stats.render());

    // Hub chart period change
    const chartPeriod = document.getElementById('chart-period');
    if (chartPeriod) chartPeriod.addEventListener('change', () => views.hub.render());

    // Session warning modal button
    const btnExtend = document.getElementById('btn-session-extend');
    if (btnExtend) btnExtend.addEventListener('click', () => {
      resetSessionTimer();
      document.getElementById('modal-session-warn').hidden = true;
    });

    console.log('✅ LEONI Sertissage Lab v4.0 — App ready');
  });

})();
