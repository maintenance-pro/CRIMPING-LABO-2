// =============================================================================
// firebase-init.js — LEONI Crimping-Laboratoire
// Initialisation Firebase SDK modulaire v9+
// =============================================================================

import { initializeApp }        from "https://www.gstatic.com/firebasejs/10.12.0/firebase-app.js";
import { getAuth }              from "https://www.gstatic.com/firebasejs/10.12.0/firebase-auth.js";
import { getFirestore }         from "https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js";
import { getDatabase }          from "https://www.gstatic.com/firebasejs/10.12.0/firebase-database.js";
import { getStorage }           from "https://www.gstatic.com/firebasejs/10.12.0/firebase-storage.js";

const firebaseConfig = {
  apiKey:            "AIzaSyBIW4I1DRqyqcmNEdajXbptQ5-RWFIG1V4",
  authDomain:        "leoni-sertissage-labo.firebaseapp.com",
  databaseURL:       "https://leoni-sertissage-labo-default-rtdb.europe-west1.firebasedatabase.app",
  projectId:         "leoni-sertissage-labo",
  storageBucket:     "leoni-sertissage-labo.firebasestorage.app",
  messagingSenderId: "432527114405",
  appId:             "1:432527114405:web:b141f6ae74f11b1cbb0d4c",
  measurementId:     "G-5YSFB1HGC8"
};

const app       = initializeApp(firebaseConfig);
const auth      = getAuth(app);
const db        = getFirestore(app);
const rtdb      = getDatabase(app);
const storage   = getStorage(app);

export { app, auth, db, rtdb, storage };
