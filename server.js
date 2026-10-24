require("dotenv").config();

console.log("START SERVER");
console.log("ADMIN_UID:", process.env.ADMIN_UID);
console.log("FIREBASE RAW:", process.env.FIREBASE_SERVICE_ACCOUNT ? process.env.FIREBASE_SERVICE_ACCOUNT.slice(0, 50) : "MISSING");




const express = require("express");
 const admin = require("firebase-admin");
 const cors = require("cors");
 const rateLimit = require("express-rate-limit");
 const bodyParser = require('body-parser'); // ← AJOUTE CECI
 const bcrypt = require("bcryptjs");
 const axios = require("axios");
 const multer = require("multer"); 
const fs = require("fs");
const crypto = require("crypto");
const { pipeline } = require("stream/promises");
 const ffmpeg = require("fluent-ffmpeg");
const { createClient } = require('@supabase/supabase-js');
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);

const app = express();


// ================= MOTEUR DE NOTIFICATIONS PUSH =================
async function sendPushNotification(targetUid, title, body, extraData = {}) {
  try {
    const userDoc = await admin.firestore().collection("users").doc(targetUid).get();
    if (!userDoc.exists) return;

    const token = userDoc.data()?.fcmToken;
    if (!token) {
      console.log(`Pas de token FCM pour l'utilisateur ${targetUid}`);
      return;
    }

    // 🔥 MODIFICATION : Forcer toutes les valeurs de data en String
    const safeData = {};
    if (extraData) {
      for (const key in extraData) {
        if (extraData[key] !== undefined && extraData[key] !== null) {
          safeData[key] = String(extraData[key]);
        }
      }
    }

    const message = {
      token: token,
      notification: { title: title, body: body },
      data: safeData
    };

    const response = await admin.messaging().send(message);
    console.log("Notification Push envoyée :", response);
  } catch (error) {
    console.error("Erreur envoi notification Push :", error);
  }
}
// ================================================================




app.set('trust proxy', 1);


const apiLimiter = rateLimit({
windowMs: 60 * 1000,
max: 100
});

app.use("/api/", apiLimiter);



//  SOLUTION : Utiliser bodyParser au lieu de express.json()
app.use(bodyParser.json({ limit: "1mb" }));
app.use(bodyParser.urlencoded({ extended: true }));



const allowedOrigins = [
  "https://hayticlips.com",
  "https://www.hayticlips.com",
  "https://hayticlips-frontend.vercel.app"
];

app.use(cors({
  origin: function (origin, callback) {
    if (!origin) return callback(null, true); // mobile apps / tests

    if (allowedOrigins.includes(origin)) {
      return callback(null, true);
    } else {
      return callback(new Error("Not allowed by CORS"));
    }
  },
  methods: ["GET", "POST"],
  credentials: true
}));


const path = require("path");
app.use(express.static(path.join(__dirname, "../hayticlips")));

// Middleware de log

app.use((req, res, next) => {
  console.log(`${req.method} ${req.path}`);
  next();
});


const giftLimiter = rateLimit({
windowMs: 10 * 1000,
max: 5
});


const compressLimiter = rateLimit({
windowMs: 60 * 1000,
max: 10
});

const upload = multer({
  dest: "uploads/",
  limits: {
    fileSize: 50 * 1024 * 1024
  }
});


// 👇 NOUVEL OUTIL MÉMOIRE (Pour éviter le bug Render) 👇
const uploadMemory = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 15 * 1024 * 1024 }
});


// FIREBASE ADMIN
let serviceAccount;

try {
  serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
  console.log("Firebase JSON OK");
} catch (e) {
  console.error("Firebase JSON ERROR:", e.message);
  process.exit(1);
}


admin.initializeApp({
credential: admin.credential.cert(serviceAccount),
projectId: "tstx-58474",
databaseURL: "https://tstx-58474.firebaseio.com"
});

const db = admin.firestore();
db.settings({ ignoreUndefinedProperties: true });

// ADMIN UID

const ADMIN_UID = process.env.ADMIN_UID;

if (!ADMIN_UID) {
  throw new Error("ADMIN_UID manquant dans les variables d'environnement");
}



// Accès marchand gratuit.
const MERCHANT_RENEWAL_AMOUNT = 0;
const MERCHANT_RENEWAL_DAYS = 0;

// Conservée temporairement pour les anciennes routes de commande.
// Elle sera remplacée avec le nouveau règlement sécurisé à 3 %.
const MERCHANT_SALE_RATE_BPS = 300;


const MONCASH_MODE = "sandbox"; // change en "live" plus tard

const HOST_REST_API =
  MONCASH_MODE === "live"
    ? "https://moncashbutton.digicelgroup.com/Api"
    : "https://sandbox.moncashbutton.digicelgroup.com/Api";

const GATEWAY_BASE =
  MONCASH_MODE === "live"
    ? "https://moncashbutton.digicelgroup.com/Moncash-middleware"
    : "https://sandbox.moncashbutton.digicelgroup.com/Moncash-middleware";

// ⚠️ remplace par tes vraies infos
const MONCASH_CLIENT_ID = process.env.MONCASH_CLIENT_ID || "TON_CLIENT_ID";
const MONCASH_CLIENT_SECRET = process.env.MONCASH_CLIENT_SECRET || "TON_CLIENT_SECRET";

// ⚠️ remplace par ton vrai domaine backend en production
const APP_BASE_URL = process.env.APP_BASE_URL || "http://172.20.10.2:3000";

async function verifyFirebaseToken(req,res,next){

try{

const authHeader = req.headers.authorization || "";
if(!authHeader.startsWith("Bearer ")){
return res.status(401).json({error:"missing token"});
}


const token = authHeader.replace("Bearer ","").trim();
const decoded = await admin.auth().verifyIdToken(token);

req.user = decoded;

next();

}catch(e){

console.log("TOKEN ERROR:", e);

return res.status(401).json({error:"invalid token"});
}

}// CALCUL CADEAUX
function calculateGift(amount){

const allowed = [25,50,100,250,500,1000];

if(!allowed.includes(amount)){
return null;
}

if(amount === 25) return {creator:20, admin:5};
if(amount === 50) return {creator:43, admin:7};
if(amount === 100) return {creator:90, admin:10};
if(amount === 250) return {creator:235, admin:15};
if(amount === 500) return {creator:475, admin:25};
if(amount === 1000) return {creator:970, admin:30};

return null;
}

async function getMonCashAccessToken() {
  const response = await axios.post(
    `${HOST_REST_API}/oauth/token`,
    "scope=read,write&grant_type=client_credentials",
    {
      headers: {
        Accept: "application/json",
        "Content-Type": "application/x-www-form-urlencoded"
      },
      auth: {
        username: MONCASH_CLIENT_ID,
        password: MONCASH_CLIENT_SECRET
      }
    }
  );

  return response.data.access_token;
}

function makeOrderId(prefix = "HC") {
  return `${prefix}_${Date.now()}_${Math.floor(Math.random() * 100000)}`;
}

function getMoneyState(userData = {}) {
  const wallet = Number(userData.wallet || 0);
  const walletLocked = Number(userData.walletLocked || 0);
  const available = Math.max(0, wallet - walletLocked);

  return { wallet, walletLocked, available };
}

function makeReferralCode() {
  return "RUG" + Math.floor(100000 + Math.random() * 900000);
}

async function generateUniqueReferralCode() {
  let code = "";
  let exists = true;

  while (exists) {
    code = makeReferralCode();
    const snap = await db.collection("users")
      .where("referralCode", "==", code)
      .limit(1)
      .get();

    exists = !snap.empty;
  }

  return code;
}

async function moncashCreatePayment(amount, orderId) {
  const accessToken = await getMonCashAccessToken();

  const response = await axios.post(
    `${HOST_REST_API}/v1/CreatePayment`,
    {
      amount,
      orderId
    },
    {
      headers: {
        accept: "application/json",
        authorization: `Bearer ${accessToken}`,
        "content-type": "application/json"
      }
    }
  );

  const token = response.data?.payment_token?.token;

  if (!token) {
    throw new Error("payment token introuvable");
  }

  return {
    token,
    redirectUrl: `${GATEWAY_BASE}/Payment/Redirect?token=${token}`
  };
}

async function moncashRetrieveOrderPayment(orderId) {
  const accessToken = await getMonCashAccessToken();

  const response = await axios.post(
    `${HOST_REST_API}/v1/RetrieveOrderPayment`,
    {
      orderId
    },
    {
      headers: {
        accept: "application/json",
        authorization: `Bearer ${accessToken}`,
        "content-type": "application/json"
      }
    }
  );

  return response.data;
}

async function moncashTransfer(amount, receiver, desc, reference) {
  const accessToken = await getMonCashAccessToken();

  const response = await axios.post(
    `${HOST_REST_API}/v1/Transfert`,
    {
      amount,
      receiver,
      desc,
      reference
    },
    {
      headers: {
        accept: "application/json",
        authorization: `Bearer ${accessToken}`,
        "content-type": "application/json"
      }
    }
  );

  return response.data;
}

async function moncashPrefundedTransactionStatus(reference) {
  const accessToken = await getMonCashAccessToken();

  const response = await axios.post(
    `${HOST_REST_API}/v1/PrefundedTransactionStatus`,
    {
      reference
    },
    {
      headers: {
        accept: "application/json",
        authorization: `Bearer ${accessToken}`,
        "content-type": "application/json"
      }
    }
  );

  return response.data;
}



// ROUTE RELAIS POUR LES PUSHS DEPUIS LE FRONTEND
app.post("/api/send-push", verifyFirebaseToken, async (req, res) => {
  try {
    const { targetUid, title, body, extraData } = req.body;

    if (!targetUid || !title || !body) {
      return res.status(400).json({ error: "Données manquantes pour le push" });
    }

    // On utilise la fonction sendPushNotification globale que tu as déjà créée sur le serveur
    await sendPushNotification(targetUid, title, body, extraData);

    res.json({ success: true });
  } catch (e) {
    console.error("ERREUR ROUTE SEND-PUSH:", e);
    res.status(500).json({ error: e.message });
  }
});



// ROUTE ENVOI CADEAU
app.post("/api/sendGift", giftLimiter, verifyFirebaseToken, async (req,res)=>{
  try {
    const {videoId, amount} = req.body;
    const giftAmount = Number(amount);

    if(!Number.isInteger(giftAmount)){
      return res.status(400).json({error:"invalid amount"});
    }

    const fromUser = req.user.uid;

    if(!videoId || !giftAmount){
      return res.status(400).json({error:"missing data"});
    }

    const gift = calculateGift(giftAmount);

    if(!gift){
      return res.status(400).json({error:"invalid amount"});
    }

    // VIDEO
    const videoRef = db.collection("videos").doc(videoId);
    const videoSnap = await videoRef.get();

    if(!videoSnap.exists){
      return res.status(404).json({error:"video not found"});
    }

    const videoData = videoSnap.data();

    if(videoData.archived === true){
      return res.status(400).json({error:"video archived"});
    }

    const creatorId = videoData.userId;

    if(!creatorId){
      return res.status(400).json({error:"invalid creator"});
    }

    if(fromUser === creatorId){
      return res.status(400).json({error:"can not send gift to yourself"});
    }

    // USERS
    const senderRef = db.collection("users").doc(fromUser);
    const creatorRef = db.collection("users").doc(creatorId);
    const adminRef = db.collection("users").doc(ADMIN_UID);

    const senderSnap = await senderRef.get();
    const creatorSnap = await creatorRef.get();

    const senderDataTop = senderSnap.data() || {};

    if(senderDataTop.suspended === true){
      return res.status(403).json({error:"account suspended"});
    }

    if(!senderSnap.exists){
      return res.status(404).json({error:"sender not found"});
    }

    if(!creatorSnap.exists){
      return res.status(404).json({error:"creator not found"});
    }

    // TRANSACTION
    await db.runTransaction(async t=>{

      // 1️⃣ --- TOUTES LES LECTURES (READS) ---
      const senderDoc = await t.get(senderRef);
      if(!senderDoc.exists) throw new Error("sender not found");
      
      const creatorDoc = await t.get(creatorRef);
      if(!creatorDoc.exists) throw new Error("creator not found");
      
      const adminDoc = await t.get(adminRef);
      if(!adminDoc.exists) throw new Error("admin not found");
      
      const statsRef = db.collection("adminStats").doc("finance");
      const statsDoc = await t.get(statsRef);

      // 2️⃣ --- PRÉPARATION DES DONNÉES ---
      const senderData = senderDoc.data() || {};
      const { wallet, available } = getMoneyState(senderData);

      if(wallet <= 0) throw new Error("wallet empty");
      if(available < giftAmount) throw new Error("Solde disponible insuffisant");

      const senderWallet = Number(senderData.wallet || 0);
      const creatorData = creatorDoc.data() || {};
      const creatorWallet = Number(creatorData.wallet || 0);
      const creatorEarned = Number(creatorData.walletEarned || 0);
      
      const adminData = adminDoc.data() || {};
      const adminWallet = Number(adminData.wallet || 0);
      const adminEarned = Number(adminData.walletEarned || 0);

      const giftRef = db.collection("giftTransactions").doc();
      const senderTx = db.collection("walletTransactions").doc();
      const receiverTx = db.collection("walletTransactions").doc();
      const adminTx = db.collection("walletTransactions").doc();
      const notifRef = db.collection("notifications").doc();

      const isSenderAdmin = (fromUser === ADMIN_UID);
      const isCreatorAdmin = (creatorId === ADMIN_UID);

      // 3️⃣ --- TOUTES LES ÉCRITURES (WRITES) ---
      if (isCreatorAdmin) {
        const newSenderBalance = senderWallet - giftAmount;
        const totalAdminGain = gift.creator + gift.admin;
        const newAdminBalance = adminWallet + totalAdminGain;
        const newAdminEarned = adminEarned + totalAdminGain;

        t.update(senderRef, { wallet: newSenderBalance });
        t.update(adminRef, { wallet: newAdminBalance, walletEarned: newAdminEarned });

        t.set(receiverTx,{
          userId: ADMIN_UID,
          type: "gift_received",
          amount: gift.creator,
          senderId: fromUser,
          videoId: videoId,
          balanceAfter: newAdminBalance,
          status: "completed",
          createdAt: admin.firestore.FieldValue.serverTimestamp()
        });

        t.set(adminTx,{
          userId: ADMIN_UID,
          type: "gift_commission_received",
          amount: gift.admin,
          senderId: fromUser,
          receiverId: creatorId,
          videoId: videoId,
          balanceAfter: newAdminBalance,
          status: "completed",
          createdAt: admin.firestore.FieldValue.serverTimestamp()
        });

      } else if (isSenderAdmin) {
        const newCreatorBalance = creatorWallet + gift.creator;
        const newCreatorEarned = creatorEarned + gift.creator;
        const newAdminBalance = adminWallet - giftAmount + gift.admin;
        const newAdminEarned = adminEarned + gift.admin;

        t.update(creatorRef, { wallet: newCreatorBalance, walletEarned: newCreatorEarned });
        t.update(adminRef, { wallet: newAdminBalance, walletEarned: newAdminEarned });

        t.set(receiverTx,{
          userId: creatorId,
          type: "gift_received",
          amount: gift.creator,
          senderId: fromUser,
          videoId: videoId,
          balanceAfter: newCreatorBalance,
          status: "completed",
          createdAt: admin.firestore.FieldValue.serverTimestamp()
        });

        t.set(adminTx,{
          userId: ADMIN_UID,
          type: "gift_commission_received",
          amount: gift.admin,
          senderId: fromUser,
          receiverId: creatorId,
          videoId: videoId,
          balanceAfter: newAdminBalance,
          status: "completed",
          createdAt: admin.firestore.FieldValue.serverTimestamp()
        });

      } else {
        const newSenderBalance = senderWallet - giftAmount;
        const newCreatorBalance = creatorWallet + gift.creator;
        const newCreatorEarned = creatorEarned + gift.creator;
        const newAdminBalance = adminWallet + gift.admin;
        const newAdminEarned = adminEarned + gift.admin;

        t.update(senderRef, { wallet: newSenderBalance });
        t.update(creatorRef, { wallet: newCreatorBalance, walletEarned: newCreatorEarned });
        t.update(adminRef, { wallet: newAdminBalance, walletEarned: newAdminEarned });

        t.set(receiverTx,{
          userId: creatorId,
          type: "gift_received",
          amount: gift.creator,
          senderId: fromUser,
          videoId: videoId,
          balanceAfter: newCreatorBalance,
          status: "completed",
          createdAt: admin.firestore.FieldValue.serverTimestamp()
        });

        t.set(adminTx,{
          userId: ADMIN_UID,
          type: "gift_commission_received",
          amount: gift.admin,
          senderId: fromUser,
          receiverId: creatorId,
          videoId: videoId,
          balanceAfter: newAdminBalance,
          status: "completed",
          createdAt: admin.firestore.FieldValue.serverTimestamp()
        });
      }

      t.set(giftRef,{
        from: fromUser,
        to: creatorId,
        videoId: videoId,
        amount: giftAmount,
        creatorEarn: gift.creator,
        adminEarn: gift.admin,
        createdAt: admin.firestore.FieldValue.serverTimestamp()
      });

      let finalSenderBalForTx = senderWallet - giftAmount;
      if(isSenderAdmin) finalSenderBalForTx = adminWallet - giftAmount + gift.admin;

      t.set(senderTx,{
        userId: fromUser,
        type: "gift_sent",
        amount: giftAmount,
        receiverId: creatorId,
        videoId: videoId,
        balanceAfter: finalSenderBalForTx,
        status: "completed",
        createdAt: admin.firestore.FieldValue.serverTimestamp()
      });

      t.set(notifRef,{
        to: creatorId,
        from: fromUser,
        fromUsername: senderData.username || "Utilisateur",
        fromAvatar: senderData.avatar || null,
        type: "gift",
        videoId: videoId,
        amount: giftAmount,
        read: false,
        createdAt: admin.firestore.FieldValue.serverTimestamp()
      });

      let stats = statsDoc.exists ? statsDoc.data() : {
        totalIn: 0,
        totalOut: 0,
        netTotal: 0,
        transactionsCount: 0,
        typeTotals: {}
      };

      const amountValue = gift.admin;
      const typeStat = "gift_commission_received";

      if(!stats.typeTotals[typeStat]){
        stats.typeTotals[typeStat] = { in:0, out:0, count:0 };
      }

      stats.totalIn += amountValue;
      stats.transactionsCount += 1;
      stats.typeTotals[typeStat].in += amountValue;
      stats.typeTotals[typeStat].count += 1;
      stats.netTotal = stats.totalIn - stats.totalOut;

      t.set(statsRef, stats);
    });

    // --- DÉCLENCHEUR PUSH CADEAU (Utilise senderSnap déjà défini plus haut) ---
    const senderUsername = senderSnap.data()?.username || "Un utilisateur";

    await sendPushNotification(
      creatorId,
      "Nouveau Cadeau ! 🎁",
      `${senderUsername} vous a envoyé un cadeau d'une valeur de ${giftAmount} !`,
      { type: "gift", amount: String(giftAmount) }
    );

    res.json({ success: true });

  } catch(e) {
    console.log("GIFT ERROR:", e);
    res.status(500).json({ error: e.message });
  }
});



function getLocalDateString(date = new Date()) {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}




// ROUTE POUR SAUVEGARDER LE TOKEN DU TÉLÉPHONE
app.post("/api/save-fcm-token", verifyFirebaseToken, async (req, res) => {
  try {
    const uid = req.user.uid;
    const { fcmToken } = req.body;

    if (!fcmToken || typeof fcmToken !== "string") {
      return res.status(400).json({ error: "Token FCM invalide ou manquant" });
    }

    await db.collection("users").doc(uid).update({
      fcmToken: fcmToken,
      fcmTokenUpdatedAt: admin.firestore.FieldValue.serverTimestamp()
    });

    res.json({ success: true });
  } catch (e) {
    console.error("Erreur sauvegarde token :", e);
    res.status(500).json({ error: e.message });
  }
});






app.post("/api/check-and-clean-email", async (req, res) => {
  const { email } = req.body;
  if (!email) return res.status(400).json({ error: "Email requis" });

  try {
    // 1. Retrouver l'utilisateur dans Auth grâce à son email
    const userRecord = await admin.auth().getUserByEmail(email);

    // 2. Vérifier si son document Firestore existe
    const userDoc = await admin.firestore().collection("users").doc(userRecord.uid).get();

    if (!userDoc.exists) {
      // Pas de document -> On supprime le compte Auth orphelin
      await admin.auth().deleteUser(userRecord.uid);
      console.log(`Compte orphelin supprimé pour l'email : ${email}`);
      return res.json({ cleaned: true });
    }

    // Le document existe vraiment -> L'email est légitimement pris
    return res.json({ cleaned: false });

  } catch (err) {
    if (err.code === "auth/user-not-found") {
      return res.json({ cleaned: false });
    }
    console.error("Erreur nettoyage email:", err);
    return res.status(500).json({ error: err.message });
  }
});




/* ================= SAVE WITHDRAW NUMBER ================= */


app.post("/api/saveWithdrawNumber", verifyFirebaseToken, async (req,res)=>{
try{

const uid = req.user.uid;


let {number} = req.body;

if(typeof number !== "string" || !number.trim()){
  return res.status(400).json({error:"Numero invalide"});
}

/* nettoyer numero */
let clean = number.replace(/\D/g,"");

if(clean.length === 8){
clean = "509" + clean;
}

/* verifier numero Digicel */

const allowedPrefixes = [
"31","34","36","37","38","39","44","46","47","49"
];

if(
clean.length !== 11 ||
!clean.startsWith("509") ||
!allowedPrefixes.includes(clean.slice(3,5))
){
return res.json({error:"Numero invalide"});
}

/* 🔒 vérifier si numero deja utilisé */

const existing = await db.collection("users")
.where("withdrawNumber","==",clean)
.get();

if(!existing.empty){

for(const doc of existing.docs){

if(doc.id !== uid){

return res.json({
error:"Ce numero est deja utilisé par un autre compte"
});

}

}

}

/* récupérer utilisateur */

const userRef = db.collection("users").doc(uid);
const userSnap = await userRef.get();

if(!userSnap.exists){
return res.json({error:"User not found"});
}

const user = userSnap.data();

if(user.suspended === true){
  return res.status(403).json({error:"Compte suspendu"});
}


/* verifier délai 150 jours */

const last = user.withdrawNumberUpdatedAt?.toMillis?.() || 0;

const days150 = 150 * 24 * 60 * 60 * 1000;

if(last && Date.now() - last < days150){
return res.json({error:"Modification possible tous les 150 jours"});
}

/* enregistrer numero */

await userRef.update({

withdrawNumber: clean,
withdrawNumberUpdatedAt: admin.firestore.FieldValue.serverTimestamp()

});

res.json({success:true});

}catch(e){

console.log("SAVE NUMBER ERROR:",e);
res.status(500).json({error:e.message});

}

});



/* ================= WALLET RECHARGE ================= */
app.post("/api/requestRecharge", verifyFirebaseToken, async (req,res)=>{
  try{
    const uid = req.user.uid;
    const amount = Number(req.body.amount);

    const userRef = db.collection("users").doc(uid);
    const userSnap = await userRef.get();

    if(!userSnap.exists){
      return res.status(404).json({error:"User not found"});
    }

    const user = userSnap.data() || {};

    if(user.suspended === true){
      return res.status(403).json({error:"Compte suspendu"});
    }

    const allowed = [50,100,250,500,1000,5000,10000];

    if(!allowed.includes(amount)){
      return res.status(400).json({error:"Montant invalide"});
    }

    const pending = await db.collection("walletTransactions")
      .where("userId","==",uid)
      .where("type","==","deposit")
      .where("status","==","pending")
      .limit(1)
      .get();

    if(!pending.empty){
      return res.status(400).json({error:"Une recharge est déjà en cours"});
    }

    const txRef = db.collection("walletTransactions").doc();
    const orderId = makeOrderId("DEP");

    await txRef.set({
      userId: uid,
      amount,
      type: "deposit",
      method: "moncash",
      status: "pending",
      orderId,
      createdAt: admin.firestore.FieldValue.serverTimestamp()
    });

    const payment = await moncashCreatePayment(amount, orderId);

    await txRef.update({
      paymentToken: payment.token,
      redirectUrl: payment.redirectUrl
    });

    res.json({
      success: true,
      txId: txRef.id,
      orderId,
      redirectUrl: payment.redirectUrl
    });

  }catch(e){
console.log("RECHARGE ERROR FULL:");
console.log("STATUS:", e.response?.status);
console.log("DATA:", e.response?.data);
console.log("MESSAGE:", e.message);
    res.status(500).json({error:"Impossible de démarrer la recharge"});
  }
});



app.get("/api/moncash/payment-return", async (req,res)=>{
  try{
    const orderId = String(req.query.orderId || "").trim();

    if(!orderId){
      return res.status(400).send("orderId manquant");
    }

    const txSnap = await db.collection("walletTransactions")
      .where("orderId","==",orderId)
      .where("type","==","deposit")
      .limit(1)
      .get();

    if(txSnap.empty){
      return res.status(404).send("Transaction introuvable");
    }

    const txDoc = txSnap.docs[0];
    const tx = txDoc.data();

    if(tx.status !== "pending"){
      return res.redirect("/wallet.html");
    }

    const details = await moncashRetrieveOrderPayment(orderId);
    const payment = details?.payment;

    if(!payment || payment.message !== "successful"){
      await txDoc.ref.update({
        status: "failed",
        failedAt: admin.firestore.FieldValue.serverTimestamp(),
        failReason: "Paiement non confirmé"
      });

      return res.redirect("/wallet.html");
    }

    await db.runTransaction(async (t)=>{
      const freshTx = await t.get(txDoc.ref);

      if(!freshTx.exists){
        throw new Error("Transaction manquante");
      }

      const freshTxData = freshTx.data();

      if(freshTxData.status !== "pending"){
        return;
      }

      const userRef = db.collection("users").doc(freshTxData.userId);
      const userSnap = await t.get(userRef);

      if(!userSnap.exists){
        throw new Error("Utilisateur introuvable");
      }

      const user = userSnap.data() || {};
      const wallet = Number(user.wallet || 0);
      const amount = Number(freshTxData.amount || 0);
      const newWallet = wallet + amount;

      t.update(userRef,{
        wallet: newWallet
      });

      t.update(txDoc.ref,{
        status: "completed",
        completedAt: admin.firestore.FieldValue.serverTimestamp(),
        transactionId: payment.transaction_id || null,
        moncashReference: payment.reference || null,
        payer: payment.payer || null,
        balanceAfter: newWallet
      });
    });

    return res.redirect("/wallet.html");

  }catch(e){
    console.log("MONCASH RETURN ERROR:", e.response?.data || e.message || e);
    return res.status(500).send("Erreur callback recharge");
  }
});




function addDaysTimestamp(days) {
  return admin.firestore.Timestamp.fromMillis(
    Date.now() + days * 24 * 60 * 60 * 1000
  );
}

function isMerchantExpired(userData = {}) {
  if (!userData.merchantExpiresAt?.toMillis) return true;
  return userData.merchantExpiresAt.toMillis() <= Date.now();
}

function isMerchantBlocked(userData = {}) {
  return userData.merchantRenewalBlocked === true || isMerchantExpired(userData);
}

app.post("/api/initUserProfile", verifyFirebaseToken, async (req, res) => {
  try {
    const uid = req.user.uid;
    const { prenom, nom, phone } = req.body;

    if (
      typeof prenom !== "string" ||
      typeof nom !== "string" ||
      typeof phone !== "string"
    ) {
      return res.status(400).json({ error: "Données invalides" });
    }

    const cleanPrenom = prenom.trim().replace(/\s+/g, " ");
    const cleanNom = nom.trim().replace(/\s+/g, " ");
    const cleanPhone = phone.trim().replace(/\s+/g, "");

    if (cleanPrenom.length < 2 || cleanPrenom.length > 30) {
      return res.status(400).json({ error: "Prénom invalide" });
    }

    if (cleanNom.length < 2 || cleanNom.length > 30) {
      return res.status(400).json({ error: "Nom invalide" });
    }

    if (!/^\+509\d{8}$/.test(cleanPhone)) {
      return res.status(400).json({ error: "Numéro haïtien invalide" });
    }

    const userRecord = await admin.auth().getUser(uid);
    const email = (userRecord.email || "").trim().toLowerCase();

    if (!email) {
      return res.status(400).json({ error: "Email introuvable" });
    }

    const userRef = db.collection("users").doc(uid);
    const userSnap = await userRef.get();

    if (userSnap.exists) {
      return res.status(400).json({ error: "Profil déjà initialisé" });
    }

    const phoneCheck = await db.collection("users")
      .where("phone", "==", cleanPhone)
      .limit(1)
      .get();

    if (!phoneCheck.empty) {
      return res.status(400).json({ error: "Ce numéro est déjà utilisé" });
    }

    const baseUsername = `${cleanPrenom}${cleanNom}`
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/\s+/g, "")
      .replace(/[^a-z0-9_]/g, "")
      .slice(0, 16);

    let username = "";
    let attempts = 0;

    while (attempts < 10) {
      const suffix = Math.floor(1000 + Math.random() * 9000);
      const candidate = `${baseUsername}${suffix}`.slice(0, 20);

      const usernameCheck = await db.collection("users")
        .where("usernameLower", "==", candidate)
        .limit(1)
        .get();

      if (usernameCheck.empty) {
        username = candidate;
        break;
      }

      attempts++;
    }

    if (!username) {
      return res.status(500).json({ error: "Impossible de générer un username unique" });
    }

  const referralCode = await generateUniqueReferralCode(); 
 await userRef.set({
      uid,
      prenom: cleanPrenom,
      nom: cleanNom,
      username,
      usernameLower: username,
      email,
      phone: cleanPhone,
      bio: "",
      avatar: "",
      followersCount: 0,
      followingCount: 0,
      totalLikes: 0,
      wallet: 0,
      walletLocked: 0,
      walletEarned: 0,
      role: "user",
      verified: false,
      profileCompleted: false,
      suspended: false,
       referralCode,
canReferMerchant: false,
merchantStatus: "none",
merchantEnabled: false,
merchantShopName: "",
merchantProductType: "",
merchantReferrerUid: null,
merchantApprovedAt: null,
merchantApprovedBy: null,
merchantExpiresAt: null,
merchantLastRenewedAt: null,
merchantRenewalAmount: MERCHANT_RENEWAL_AMOUNT,
merchantRenewalDays: MERCHANT_RENEWAL_DAYS,
merchantRenewalBlocked: false,
merchantAccessModel: "free-v2",
      security: { enabled: false
        },
      createdAt: admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true }); 

    return res.json({ success: true });

  } catch (e) {
    console.log("INIT USER PROFILE ERROR:", e);
    return res.status(500).json({ error: e.message });
  }
});



app.post(
  "/api/requestMerchantAccess",
  verifyFirebaseToken,
  async (req, res) => {
    try {
      const uid = req.user.uid;

      const {
        shopName,
        productType,
        email,
        description,
        requestKey
      } = req.body;

      if (
        typeof shopName !== "string" ||
        typeof productType !== "string" ||
        typeof email !== "string" ||
        typeof description !== "string" ||
        typeof requestKey !== "string"
      ) {
        return res.status(400).json({
          success: false,
          error: "Informations invalides"
        });
      }

      const cleanShopName = shopName.trim();
      const cleanProductType = productType.trim();
      const cleanEmail = email.trim().toLowerCase();
      const cleanDescription = description.trim();

      if (
        cleanShopName.length < 2 ||
        cleanShopName.length > 60
      ) {
        return res.status(400).json({
          success: false,
          error: "Nom de boutique invalide"
        });
      }

      if (
        cleanProductType.length < 2 ||
        cleanProductType.length > 60
      ) {
        return res.status(400).json({
          success: false,
          error: "Catégorie commerciale invalide"
        });
      }

      if (
        cleanEmail.length > 254 ||
        !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(cleanEmail)
      ) {
        return res.status(400).json({
          success: false,
          error: "Email invalide"
        });
      }

      if (
        cleanDescription.length < 5 ||
        cleanDescription.length > 300
      ) {
        return res.status(400).json({
          success: false,
          error: "La description doit contenir entre 5 et 300 caractères"
        });
      }

      if (!/^[a-zA-Z0-9_-]{16,80}$/.test(requestKey)) {
        return res.status(400).json({
          success: false,
          error: "Référence de demande invalide. Actualise la page."
        });
      }

      const payload = {
        shopName: cleanShopName,
        productType: cleanProductType,
        email: cleanEmail,
        description: cleanDescription
      };

      const fingerprint = crypto
        .createHash("sha256")
        .update(JSON.stringify(payload))
        .digest("hex");

      const operationId = crypto
        .createHash("sha256")
        .update(`merchant-request:${uid}:${requestKey}`)
        .digest("hex");

      const userRef = db.collection("users").doc(uid);

      const operationRef = db
        .collection("merchantOperations")
        .doc(operationId);

      const requestRef = db
        .collection("merchantRequests")
        .doc();

      const result = await db.runTransaction(async (t) => {
        const operationSnap = await t.get(operationRef);

        if (operationSnap.exists) {
          const previous = operationSnap.data();

          if (previous.fingerprint !== fingerprint) {
            throw new Error(
              "Cette référence correspond à une autre demande"
            );
          }

          return previous.result;
        }

        const userSnap = await t.get(userRef);

        if (!userSnap.exists) {
          throw new Error("Utilisateur introuvable");
        }

        const user = userSnap.data() || {};

        if (user.suspended === true) {
          throw new Error("Compte suspendu");
        }

        if (
          user.merchantEnabled === true ||
          ["pending", "active", "suspended"].includes(
            user.merchantStatus
          )
        ) {
          throw new Error(
            "Une boutique ou une demande existe déjà pour ce compte"
          );
        }

        const lastRequestMs =
          user.merchantRequestedAt?.toMillis?.() || 0;

        if (
          lastRequestMs &&
          Date.now() - lastRequestMs < 60000
        ) {
          throw new Error(
            "Attends une minute avant une nouvelle demande"
          );
        }

        const pendingQuery = db
          .collection("merchantRequests")
          .where("uid", "==", uid)
          .where("status", "==", "pending")
          .limit(1);

        const pendingSnap = await t.get(pendingQuery);

        if (!pendingSnap.empty) {
          throw new Error("Une demande est déjà en attente");
        }

        const response = {
          success: true,
          requestId: requestRef.id
        };

        t.create(requestRef, {
          uid,
          ...payload,
          accessModel: "free-v2",
          amountPaid: 0,
          status: "pending",
          reviewedBy: null,
          reviewedAt: null,
          rejectReason: "",
          refundDone: false,
          payoutDone: false,
          createdAt: admin.firestore.FieldValue.serverTimestamp()
        });

        t.update(userRef, {
          merchantStatus: "pending",
          merchantEnabled: false,
          merchantRequestId: requestRef.id,
          merchantRequestedAt:
            admin.firestore.FieldValue.serverTimestamp()
        });

        t.create(operationRef, {
          uid,
          kind: "merchant-request",
          fingerprint,
          result: response,
          createdAt: admin.firestore.FieldValue.serverTimestamp()
        });

        return response;
      });

      return res.json(result);

    } catch (error) {
      console.error("MERCHANT REQUEST ERROR:", error.message);

      return res.status(400).json({
        success: false,
        error: error.message || "Impossible d’envoyer la demande"
      });
    }
  }
);



async function readLegacyMerchantRefund(t, requestId, requestData) {
  if (
    requestData.accessModel === "free-v2" &&
    Number(requestData.amountPaid || 0) === 0
  ) {
    return 0;
  }

  const historyQuery = db
    .collection("walletTransactions")
    .where("merchantRequestId", "==", requestId)
    .limit(51);

  const historySnap = await t.get(historyQuery);

  if (historySnap.size > 50) {
    throw new Error(
      "Historique de cette ancienne demande à vérifier manuellement"
    );
  }

  let paidMinor = 0;
  let refundedMinor = 0;

  function toMinor(value) {
    const amount = Number(value);
    const minor = Math.round(amount * 100);

    if (
      !Number.isFinite(amount) ||
      !Number.isSafeInteger(minor) ||
      minor < 0 ||
      Math.abs(minor / 100 - amount) > 0.0000001
    ) {
      throw new Error("Ancien montant financier incohérent");
    }

    return minor;
  }

  for (const document of historySnap.docs) {
    const transaction = document.data();

    if (
      transaction.userId !== requestData.uid ||
      transaction.status !== "completed"
    ) {
      continue;
    }

    if (transaction.type === "merchant_request_payment") {
      paidMinor += toMinor(transaction.amount);
    }

    if (transaction.type === "merchant_request_refund") {
      refundedMinor += toMinor(transaction.amount);
    }
  }

  if (
    Number(requestData.amountPaid || 0) > 0 &&
    paidMinor === 0
  ) {
    throw new Error(
      "Ancien paiement non vérifiable : examen manuel nécessaire"
    );
  }

  if (refundedMinor > paidMinor) {
    throw new Error(
      "Les remboursements historiques dépassent le paiement vérifié"
    );
  }

  return (paidMinor - refundedMinor) / 100;
}



app.post(
  "/api/approveMerchant",
  verifyFirebaseToken,
  async (req, res) => {
    try {
      const staffUid = req.user.uid;
      const requestId = req.body.requestId;

      if (
        typeof requestId !== "string" ||
        !requestId.trim() ||
        requestId.includes("/") ||
        requestId.length > 128
      ) {
        return res.status(400).json({
          success: false,
          error: "Identifiant de demande invalide"
        });
      }

      const requestRef = db
        .collection("merchantRequests")
        .doc(requestId.trim());

      const staffRef = db.collection("users").doc(staffUid);

      const result = await db.runTransaction(async (t) => {
        const staffSnap = await t.get(staffRef);
        const requestSnap = await t.get(requestRef);

        const staff = staffSnap.data() || {};

        if (
          !staffSnap.exists ||
          staff.suspended === true ||
          !["admin", "merchant_manager"].includes(staff.role)
        ) {
          throw new Error("Accès refusé");
        }

        if (!requestSnap.exists) {
          throw new Error("Demande introuvable");
        }

        const request = requestSnap.data();

        if (request.status === "approved") {
          return { alreadyProcessed: true };
        }

        if (request.status !== "pending") {
          throw new Error("Cette demande a déjà été traitée");
        }

        const userRef = db.collection("users").doc(request.uid);
        const userSnap = await t.get(userRef);

        if (!userSnap.exists) {
          throw new Error("Utilisateur introuvable");
        }

        const user = userSnap.data() || {};

        if (user.suspended === true) {
          throw new Error("Impossible d’activer un compte suspendu");
        }

        if (
          user.merchantStatus === "suspended" ||
          user.merchantEnabled === true
        ) {
          throw new Error(
            "Ce compte possède déjà un accès ou une suspension à examiner"
          );
        }

        const refundAmount = await readLegacyMerchantRefund(
          t,
          requestRef.id,
          request
        );

        const fields = {
          merchantStatus: "active",
          merchantEnabled: true,
          merchantAccessModel: "free-v2",
          merchantShopName: request.shopName || "",
          merchantProductType: request.productType || "",
          merchantEmail: request.email || user.email || "",
          merchantBio: request.description || user.merchantBio || "",
          merchantApprovedAt:
            admin.firestore.FieldValue.serverTimestamp(),
          merchantApprovedBy: staffUid,
          merchantRenewalBlocked: false,
          merchantRenewalAmount: 0,
          merchantRenewalDays: 0,
          merchantExpiresAt: null,
          merchantRejectReason: ""
        };

        if (refundAmount > 0) {
          const wallet = Number(user.wallet || 0);

          if (!Number.isFinite(wallet) || wallet < 0) {
            throw new Error("Solde à vérifier avant remboursement");
          }

          const newWallet = Math.round(
            (wallet + refundAmount) * 100
          ) / 100;

          fields.wallet = newWallet;

          const refundRef = db.collection("walletTransactions")
            .doc(`merchant_access_refund_${requestRef.id}`);

          t.create(refundRef, {
            userId: request.uid,
            type: "merchant_request_refund",
            amount: refundAmount,
            status: "completed",
            merchantRequestId: requestRef.id,
            balanceAfter: newWallet,
            description: "Passage gratuit d’une ancienne demande en attente",
            createdAt:
              admin.firestore.FieldValue.serverTimestamp()
          });
        }

        t.update(userRef, fields);

        t.update(requestRef, {
          status: "approved",
          accessModel: "free-v2",
          reviewedBy: staffUid,
          reviewedAt:
            admin.firestore.FieldValue.serverTimestamp(),
          verifiedLegacyRefund: refundAmount,
          refundDone: refundAmount > 0 || request.refundDone === true,
          payoutDone: false
        });

        t.create(db.collection("notifications").doc(), {
          to: request.uid,
          from: staffUid,
          type: "merchant_approved",
          message: "Ta boutique est approuvée. Ton accès marchand est gratuit.",
          read: false,
          createdAt:
            admin.firestore.FieldValue.serverTimestamp()
        });

        t.create(db.collection("merchantAuditLogs").doc(), {
          actorUid: staffUid,
          action: "merchant-approved",
          targetUid: request.uid,
          requestId: requestRef.id,
          createdAt:
            admin.firestore.FieldValue.serverTimestamp()
        });

        return {
          alreadyProcessed: false,
          uid: request.uid
        };
      });

      if (!result.alreadyProcessed) {
        await sendPushNotification(
          result.uid,
          "Boutique approuvée",
          "Ton Espace Marchand est activé gratuitement.",
          { type: "merchant_approved" }
        );
      }

      return res.json({ success: true });

    } catch (error) {
      console.error("APPROVE MERCHANT ERROR:", error.message);

      return res.status(400).json({
        success: false,
        error: error.message || "Impossible d’approuver la demande"
      });
    }
  }
);


app.post(
  "/api/rejectMerchant",
  verifyFirebaseToken,
  async (req, res) => {
    try {
      const staffUid = req.user.uid;
      const { requestId, rejectReason } = req.body;

      if (
        typeof requestId !== "string" ||
        !requestId.trim() ||
        requestId.includes("/") ||
        requestId.length > 128
      ) {
        return res.status(400).json({
          success: false,
          error: "Identifiant de demande invalide"
        });
      }

      if (
        typeof rejectReason !== "string" ||
        rejectReason.trim().length < 5 ||
        rejectReason.trim().length > 1000
      ) {
        return res.status(400).json({
          success: false,
          error: "Indique un motif de refus entre 5 et 1000 caractères"
        });
      }

      const reason = rejectReason.trim();

      const requestRef = db.collection("merchantRequests")
        .doc(requestId.trim());

      const staffRef = db.collection("users").doc(staffUid);

      const result = await db.runTransaction(async (t) => {
        const staffSnap = await t.get(staffRef);
        const requestSnap = await t.get(requestRef);

        const staff = staffSnap.data() || {};

        if (
          !staffSnap.exists ||
          staff.suspended === true ||
          !["admin", "merchant_manager"].includes(staff.role)
        ) {
          throw new Error("Accès refusé");
        }

        if (!requestSnap.exists) {
          throw new Error("Demande introuvable");
        }

        const request = requestSnap.data();

        if (request.status === "rejected") {
          return { alreadyProcessed: true };
        }

        if (request.status !== "pending") {
          throw new Error("Cette demande a déjà été traitée");
        }

        const userRef = db.collection("users").doc(request.uid);
        const userSnap = await t.get(userRef);

        if (!userSnap.exists) {
          throw new Error("Utilisateur introuvable");
        }

        const user = userSnap.data() || {};

        if (
          user.merchantEnabled === true ||
          ["active", "suspended"].includes(user.merchantStatus)
        ) {
          throw new Error(
            "L’état de cette boutique doit être examiné avant ce refus"
          );
        }

        const refundAmount = await readLegacyMerchantRefund(
          t,
          requestRef.id,
          request
        );

        const fields = {
          merchantStatus: "rejected",
          merchantEnabled: false,
          merchantRejectReason: reason
        };

        if (refundAmount > 0) {
          const wallet = Number(user.wallet || 0);

          if (!Number.isFinite(wallet) || wallet < 0) {
            throw new Error("Solde à vérifier avant remboursement");
          }

          const newWallet = Math.round(
            (wallet + refundAmount) * 100
          ) / 100;

          fields.wallet = newWallet;

          const refundRef = db.collection("walletTransactions")
            .doc(`merchant_access_refund_${requestRef.id}`);

          t.create(refundRef, {
            userId: request.uid,
            type: "merchant_request_refund",
            amount: refundAmount,
            status: "completed",
            merchantRequestId: requestRef.id,
            balanceAfter: newWallet,
            description: "Remboursement vérifié d’une ancienne demande",
            createdAt:
              admin.firestore.FieldValue.serverTimestamp()
          });
        }

        t.update(userRef, fields);

        t.update(requestRef, {
          status: "rejected",
          accessModel: "free-v2",
          reviewedBy: staffUid,
          reviewedAt:
            admin.firestore.FieldValue.serverTimestamp(),
          rejectReason: reason,
          verifiedLegacyRefund: refundAmount,
          refundDone: refundAmount > 0 || request.refundDone === true
        });

        t.create(db.collection("notifications").doc(), {
          to: request.uid,
          from: staffUid,
          type: "merchant_rejected",
          message: `Ta demande a été refusée. Motif : ${reason}`,
          read: false,
          createdAt:
            admin.firestore.FieldValue.serverTimestamp()
        });

        t.create(db.collection("merchantAuditLogs").doc(), {
          actorUid: staffUid,
          action: "merchant-rejected",
          targetUid: request.uid,
          requestId: requestRef.id,
          reason,
          createdAt:
            admin.firestore.FieldValue.serverTimestamp()
        });

        return {
          alreadyProcessed: false,
          uid: request.uid
        };
      });

      if (!result.alreadyProcessed) {
        await sendPushNotification(
          result.uid,
          "Demande marchand examinée",
          `Ta demande a été refusée. Motif : ${reason}`,
          { type: "merchant_rejected" }
        );
      }

      return res.json({ success: true });

    } catch (error) {
      console.error("REJECT MERCHANT ERROR:", error.message);

      return res.status(400).json({
        success: false,
        error: error.message || "Impossible de refuser la demande"
      });
    }
  }
);


for (const route of [
  "/api/enableMerchantReferrer",
  "/api/disableMerchantReferrer"
]) {
  app.post(route, verifyFirebaseToken, (req, res) => {
    return res.status(410).json({
      success: false,
      error: "Le parrainage n’est plus nécessaire pour accéder à l’Espace Marchand.",
      code: "MERCHANT_ACCESS_FREE"
    });
  });
}



app.post(
  "/api/removeMerchantAccess",
  verifyFirebaseToken,
  async (req, res) => {
    try {
      const staffUid = req.user.uid;
      const { targetUid, reason } = req.body;

      if (
        typeof targetUid !== "string" ||
        !targetUid.trim() ||
        targetUid.includes("/") ||
        targetUid.length > 128
      ) {
        return res.status(400).json({
          success: false,
          error: "Utilisateur cible invalide"
        });
      }

      if (
        typeof reason !== "string" ||
        reason.trim().length < 5 ||
        reason.trim().length > 1000
      ) {
        return res.status(400).json({
          success: false,
          error: "Un motif de suspension est obligatoire"
        });
      }

      const cleanUid = targetUid.trim();
      const cleanReason = reason.trim();

      await db.runTransaction(async (t) => {
        const staffRef = db.collection("users").doc(staffUid);
        const userRef = db.collection("users").doc(cleanUid);

        const staffSnap = await t.get(staffRef);
        const userSnap = await t.get(userRef);

        const staff = staffSnap.data() || {};

        if (
          !staffSnap.exists ||
          staff.suspended === true ||
          !["admin", "merchant_manager"].includes(staff.role)
        ) {
          throw new Error("Accès refusé");
        }

        if (!userSnap.exists) {
          throw new Error("Marchand introuvable");
        }

        const merchant = userSnap.data() || {};

        if (merchant.merchantStatus === "suspended") {
          return;
        }

        if (merchant.merchantStatus !== "active") {
          throw new Error("Cette boutique n’est pas active");
        }

        t.update(userRef, {
          merchantStatus: "suspended",
          merchantEnabled: false,
          merchantSuspensionReason: cleanReason,
          merchantSuspendedBy: staffUid,
          merchantSuspendedAt:
            admin.firestore.FieldValue.serverTimestamp()
        });

        t.create(db.collection("notifications").doc(), {
          to: cleanUid,
          from: staffUid,
          type: "merchant_suspended",
          message: `Ta boutique est suspendue. Motif : ${cleanReason}`,
          read: false,
          createdAt:
            admin.firestore.FieldValue.serverTimestamp()
        });

        t.create(db.collection("merchantAuditLogs").doc(), {
          actorUid: staffUid,
          action: "merchant-suspended",
          targetUid: cleanUid,
          reason: cleanReason,
          createdAt:
            admin.firestore.FieldValue.serverTimestamp()
        });
      });

      return res.json({ success: true });

    } catch (error) {
      console.error("SUSPEND MERCHANT ERROR:", error.message);

      return res.status(400).json({
        success: false,
        error: error.message || "Impossible de suspendre la boutique"
      });
    }
  }
);


const merchantOrdersV2 = (() => {
  const C = (() => {
    const crypto = require("node:crypto");
    const TIMEZONE = "America/Port-au-Prince";
    const RATE_BPS = MERCHANT_SALE_RATE_BPS;
    const MAX_MONEY_MINOR = 100_000_000_000;

    class MerchantError extends Error {
      constructor(status, message, code = "MERCHANT_ERROR") {
        super(message);
        this.status = status;
        this.code = code;
      }
    }

    function fail(condition, message, status = 400, code) {
      if (!condition) {
        throw new MerchantError(status, message, code);
      }
    }

    function text(value, name, max, min = 0) {
      fail(typeof value === "string", `${name} invalide`);

      const result = value.trim();

      fail(
        result.length >= min && result.length <= max,
        `${name} invalide`
      );

      return result;
    }

    function id(value, name = "Identifiant") {
      const result = text(value, name, 128, 1);

      fail(
        !/[\/\x00-\x1f]/.test(result) &&
        ![".", ".."].includes(result),
        `${name} invalide`
      );

      return result;
    }

    function minor(value, name = "Montant", zero = true) {
      fail(
        typeof value === "number" && Number.isFinite(value),
        `${name} invalide`
      );

      const n = Math.round(value * 100);

      fail(
        Number.isSafeInteger(n) &&
        n >= (zero ? 0 : 1) &&
        n <= MAX_MONEY_MINOR &&
        Math.abs(n / 100 - value) < 0.0000001,
        `${name} invalide`
      );

      return n;
    }

    const htg = n => n / 100;

    function feeFor(gross) {
      fail(
        Number.isSafeInteger(gross) &&
        gross > 0 &&
        gross <= MAX_MONEY_MINOR,
        "Total invalide"
      );

      return Math.floor((gross * RATE_BPS + 5000) / 10000);
    }

    function money(user = {}, allowLegacy = false) {
      const wallet = minor(user.wallet ?? 0, "Solde");
      const locked = minor(user.walletLocked ?? 0, "Réservation");

      fail(
        allowLegacy || locked <= wallet,
        "Solde à rapprocher par l’administration",
        409,
        "RECONCILIATION_REQUIRED"
      );

      return {
        wallet,
        locked,
        available: Math.max(0, wallet - locked)
      };
    }

    function active(user) {
      return Boolean(
        user &&
        user.suspended !== true &&
        user.merchantEnabled === true &&
        user.merchantStatus === "active"
      );
    }

    function staff(user, adminOnly = false) {
      fail(
        user &&
        user.suspended !== true &&
        (
          user.role === "admin" ||
          (!adminOnly && user.role === "merchant_manager")
        ),
        "Accès refusé",
        403
      );
    }

    const sha = value =>
      crypto.createHash("sha256").update(value).digest("hex");

    function operation(uid, kind, key, payload) {
      fail(
        typeof key === "string" &&
        /^[a-zA-Z0-9_-]{16,80}$/.test(key),
        "Actualise la page : référence de requête manquante",
        400,
        "REQUEST_KEY_REQUIRED"
      );

      return {
        id: sha(`${uid}:${kind}:${key}`),
        fingerprint: sha(JSON.stringify(payload))
      };
    }

    function replay(snapshot, fingerprint) {
      if (!snapshot.exists) return null;

      fail(
        snapshot.data().fingerprint === fingerprint,
        "Cette référence est déjà utilisée pour une autre opération",
        409
      );

      return snapshot.data().result;
    }

    function day(ms = Date.now()) {
      const parts = new Intl.DateTimeFormat("en-CA", {
        timeZone: TIMEZONE,
        year: "numeric",
        month: "2-digit",
        day: "2-digit"
      }).formatToParts(new Date(ms));

      const p = Object.fromEntries(
        parts.map(x => [x.type, x.value])
      );

      return `${p.year}-${p.month}-${p.day}`;
    }

    function asMs(value) {
      if (value?.toMillis) return value.toMillis();
      if (value?.toDate) return value.toDate().getTime();

      return typeof value === "number" && Number.isFinite(value)
        ? value
        : 0;
    }

    function addDay(date, count) {
      const d = new Date(`${date}T12:00:00Z`);
      d.setUTCDate(d.getUTCDate() + count);
      return d.toISOString().slice(0, 10);
    }

    function validDate(value) {
      return (
        typeof value === "string" &&
        /^\d{4}-\d{2}-\d{2}$/.test(value) &&
        !Number.isNaN(Date.parse(`${value}T12:00:00Z`)) &&
        new Date(`${value}T12:00:00Z`)
          .toISOString().startsWith(value)
      );
    }

    function range(input = {}, now = Date.now()) {
      const today = day(now);
      let from;
      let to;

      switch (input.period || "30") {
        case "today":
          from = to = today;
          break;
        case "7":
          from = addDay(today, -6);
          to = today;
          break;
        case "30":
          from = addDay(today, -29);
          to = today;
          break;
        case "month":
          from = `${today.slice(0, 7)}-01`;
          to = today;
          break;
        case "year":
          from = `${today.slice(0, 4)}-01-01`;
          to = today;
          break;
        case "custom":
          from = input.from;
          to = input.to;
          break;
        default:
          throw new MerchantError(400, "Période invalide");
      }

      fail(
        validDate(from) &&
        validDate(to) &&
        from <= to &&
        to <= today,
        "Dates invalides"
      );

      fail(
        Date.parse(`${to}T12:00:00Z`) -
        Date.parse(`${from}T12:00:00Z`) <= 366 * 86400000,
        "Période limitée à 366 jours"
      );

      return {
        from,
        to,
        timezone: TIMEZONE,
        generatedAt: now
      };
    }

    function inRange(timestamp, r) {
      const ms = asMs(timestamp);
      if (!ms) return false;

      const d = day(ms);

      return d >= r.from && d <= r.to && ms <= r.generatedAt;
    }

    function contact(body = {}) {
      const email = text(
        body.customerEmail,
        "Email",
        254,
        3
      ).toLowerCase();

      fail(
        /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email),
        "Email invalide"
      );

      const phone = value => {
        let p = text(
          value,
          "Téléphone",
          30,
          8
        ).replace(/\D/g, "");

        if (p.length === 8) p = `509${p}`;

        fail(
          /^509\d{8}$/.test(p),
          "Téléphone haïtien invalide"
        );

        return p;
      };

      const sexe = text(
        body.customerSexe || "",
        "Sexe",
        20
      );

      fail(
        ["", "M", "F", "Autre"].includes(sexe),
        "Valeur invalide"
      );

      return {
        customerNom: text(body.customerNom, "Nom", 60, 1),
        customerPrenom: text(body.customerPrenom, "Prénom", 60, 1),
        customerEmail: email,
        customerPhone: phone(body.customerPhone),
        customerWhatsapp: phone(
          body.customerWhatsapp || body.customerPhone
        ),
        customerSexe: sexe,
        customerDetails: text(
          body.customerDetails || "",
          "Détails",
          1000
        )
      };
    }

    function codecs(base64) {
      const key = Buffer.from(base64 || "", "base64");

      fail(
        key.length === 32,
        "MERCHANT_CODE_KEY doit contenir 32 octets encodés en base64",
        503,
        "CODE_KEY_MISSING"
      );

      function digest(orderId, code) {
        return crypto.createHmac("sha256", key)
          .update(`merchant-delivery:${orderId}:${code}`)
          .digest("hex");
      }

      return {
        create(orderId) {
          const alphabet = "23456789ABCDEFGHJKLMNPQRSTUVWXYZ";
          let code = "";

          for (let i = 0; i < 12; i++) {
            code += alphabet[crypto.randomInt(alphabet.length)];
          }

          const iv = crypto.randomBytes(12);

          const cipher = crypto.createCipheriv(
            "aes-256-gcm",
            key,
            iv
          );

          cipher.setAAD(Buffer.from(orderId));

          const encrypted = Buffer.concat([
            cipher.update(code, "utf8"),
            cipher.final()
          ]);

          return {
            cipher: encrypted.toString("base64"),
            iv: iv.toString("base64"),
            tag: cipher.getAuthTag().toString("base64"),
            hash: digest(orderId, code),
            failedAttempts: 0,
            lockUntilMs: 0,
            usedAt: null,
            keyVersion: 1
          };
        },

        read(orderId, secret) {
          const decipher = crypto.createDecipheriv(
            "aes-256-gcm",
            key,
            Buffer.from(secret.iv, "base64")
          );

          decipher.setAAD(Buffer.from(orderId));

          decipher.setAuthTag(
            Buffer.from(secret.tag, "base64")
          );

          return Buffer.concat([
            decipher.update(Buffer.from(secret.cipher, "base64")),
            decipher.final()
          ]).toString("utf8");
        },

        matches(orderId, input, secret) {
          const normalized = String(input || "")
            .toUpperCase()
            .replace(/[\s-]/g, "");

          if (
            !/^[23456789ABCDEFGHJKLMNPQRSTUVWXYZ]{12}$/
              .test(normalized)
          ) {
            return false;
          }

          const a = Buffer.from(
            digest(orderId, normalized),
            "hex"
          );

          const b = Buffer.from(secret.hash || "", "hex");

          return (
            a.length === b.length &&
            crypto.timingSafeEqual(a, b)
          );
        }
      };
    }

    function publicOrder(id, order) {
      const out = { id };

      for (const k of [
        "buyerUid", "merchantUid", "productId",
        "productName", "productImage", "quantity",
        "unitPrice", "totalAmount", "feeAmount",
        "merchantNet", "feeRateBps", "feePolicy",
        "status", "fundsStatus", "accountingVersion",
        "customerNom", "customerPrenom", "customerEmail",
        "customerPhone", "customerWhatsapp", "customerSexe",
        "customerDetails", "disputeId", "disputeDeadlineMs",
        "deliveryTerms", "stockRestored", "requiresReconciliation"
      ]) {
        if (order[k] !== undefined) out[k] = order[k];
      }

      for (const k of [
        "createdAt", "acceptedAt", "shippedAt",
        "completedAt", "settledAt", "cancelledAt", "refundedAt"
      ]) {
        out[`${k}Ms`] = asMs(order[k]);
      }

      return out;
    }

    return {
      MerchantError, fail, text, id, minor, htg,
      feeFor, money, active, staff, operation, replay,
      sha, day, asMs, inRange, range, contact,
      codecs, publicOrder, RATE_BPS, TIMEZONE
    };
  })();

  const auth = verifyFirebaseToken;

  // Les écritures sont différées pour effectuer toutes les lectures
  // Firestore avant les écritures de la transaction.
  class Staged {
    constructor(real) {
      this.real = real;
      this.writes = [];
    }

    get(ref) {
      return this.real.get(ref);
    }

    set(...args) {
      this.writes.push(["set", args]);
    }

    update(...args) {
      this.writes.push(["update", args]);
    }

    create(...args) {
      this.writes.push(["create", args]);
    }

    delete(...args) {
      this.writes.push(["delete", args]);
    }

    flush() {
      for (const [method, args] of this.writes) {
        this.real[method](...args);
      }
    }
  }

  const timestamp = () =>
    admin.firestore.FieldValue.serverTimestamp();

  const ref = (name, key) => db.collection(name).doc(key);

  const run = fn => db.runTransaction(async real => {
    const tx = new Staged(real);
    const out = await fn(tx);
    tx.flush();
    return out;
  });

  const maxRows = Number(
    process.env.MERCHANT_REPORT_MAX_ROWS || 10000
  );

  const disputeHours = Number(
    process.env.MERCHANT_DISPUTE_HOURS || 48
  );

  C.fail(
    Number.isInteger(disputeHours) &&
    disputeHours >= 1 &&
    disputeHours <= 720,
    "MERCHANT_DISPUTE_HOURS invalide",
    503
  );

  const codeTools = () =>
    C.codecs(process.env.MERCHANT_CODE_KEY);

  const wrap = fn => async (req, res) => {
    try {
      await fn(req, res);
    } catch (e) {
      if (!(e instanceof C.MerchantError)) {
        console.error("MERCHANT:", e.message);
      }

      if (!res.headersSent) {
        res.status(e.status || 500).json({
          success: false,
          error: e.status
            ? e.message
            : "Erreur serveur. Vérifie le statut avant de réessayer.",
          code: e.code || "SERVER_ERROR"
        });
      }
    }
  };

  const getUser = async (tx, uid) => {
    const s = await tx.get(ref("users", uid));
    C.fail(s.exists, "Profil introuvable", 404);
    return s.data();
  };

  const staffUser = async (tx, uid, onlyAdmin = false) => {
    const u = await getUser(tx, uid);
    C.staff(u, onlyAdmin);
    return u;
  };

  const operationKey = req =>
    req.headers["idempotency-key"] || req.body.requestKey;

  function audit(tx, actorUid, action, target, details = {}) {
    tx.create(db.collection("merchantAuditLogs").doc(), {
      actorUid,
      action,
      target,
      details,
      createdAt: timestamp()
    });
  }

  function notify(tx, uid, actor, type, message, key, extra = {}) {
    tx.set(
      ref("notifications", C.sha(`merchant:${key}`)),
      {
        to: uid,
        from: actor,
        type,
        message,
        read: false,
        ...extra,
        createdAt: timestamp()
      }
    );
  }

  async function finance(tx, type, incoming, outgoing) {
    if (!tx.financeState) {
      const statsRef = ref("adminStats", "finance");
      const snapshot = await tx.get(statsRef);

      tx.financeState = {
        ref: statsRef,
        data: snapshot.exists ? snapshot.data() : {}
      };
    }

    const { ref: statsRef, data } = tx.financeState;

    data.typeTotals = { ...(data.typeTotals || {}) };

    const previous = data.typeTotals[type] || {};

    data.typeTotals[type] = {
      in: C.htg(C.minor(Number(previous.in || 0)) + incoming),
      out: C.htg(C.minor(Number(previous.out || 0)) + outgoing),
      count: Number(previous.count || 0) + 1
    };

    data.totalIn = C.htg(
      C.minor(Number(data.totalIn || 0)) + incoming
    );

    data.totalOut = C.htg(
      C.minor(Number(data.totalOut || 0)) + outgoing
    );

    data.netTotal = Number(
      (data.totalIn - data.totalOut).toFixed(2)
    );

    data.transactionsCount =
      Number(data.transactionsCount || 0) + 1;

    tx.set(statsRef, data, { merge: true });
  }

  function accounting(tx) {
    const accounts = new Map();

    async function account(uid) {
      if (!accounts.has(uid)) {
        const u = await getUser(tx, uid);
        const m = C.money(u, true);

        accounts.set(uid, {
          user: u,
          ...m,
          earned: C.minor(u.walletEarned ?? 0, "Revenus")
        });
      }

      return accounts.get(uid);
    }

    return {
      account,

      async post(
        uid,
        walletDelta,
        lockedDelta,
        type,
        amount,
        eventId,
        extra = {},
        earnedDelta = 0
      ) {
        const a = await account(uid);

        a.wallet += walletDelta;
        a.locked += lockedDelta;
        a.earned = Math.max(0, a.earned + earnedDelta);

        C.fail(
          Number.isSafeInteger(a.wallet) &&
          a.wallet >= 0 &&
          a.locked >= 0,
          "Solde incohérent : rapprochement nécessaire",
          409
        );

        tx.create(
          ref(
            "walletTransactions",
            C.sha(`merchant-event:${eventId}:${uid}`)
          ),
          {
            userId: uid,
            type,
            amount: C.htg(amount),
            status: "completed",
            accountingVersion: 2,
            scope: "merchant",
            walletDelta: C.htg(walletDelta),
            lockedDelta: C.htg(lockedDelta),
            balanceAfter: C.htg(a.wallet),
            walletLockedAfter: C.htg(a.locked),
            ...extra,
            createdAt: timestamp()
          }
        );
      },

      flush() {
        for (const [uid, a] of accounts) {
          C.fail(
            a.locked <= a.wallet,
            "Réservations incohérentes : rapprochement nécessaire",
            409,
            "RECONCILIATION_REQUIRED"
          );

          tx.update(ref("users", uid), {
            wallet: C.htg(a.wallet),
            walletLocked: C.htg(a.locked),
            walletEarned: C.htg(a.earned)
          });
        }
      }
    };
  }

  async function loadOrder(tx, orderId) {
    const orderRef = ref(
      "merchantOrders",
      C.id(orderId, "Commande")
    );

    const s = await tx.get(orderRef);
    C.fail(s.exists, "Commande introuvable", 404);

    return { orderRef, order: s.data() };
  }

  // Conversion des anciennes commandes à partir des transactions réelles.
  async function legacyOrder(tx, orderId, original, ledger) {
    if (original.accountingVersion === 2) return original;

    C.fail(
      original.buyerUid !== original.merchantUid,
      "Ancienne commande à vérifier manuellement",
      409,
      "RECONCILIATION_REQUIRED"
    );

    const q = await tx.get(
      db.collection("walletTransactions")
        .where("orderId", "==", orderId)
        .limit(51)
    );

    C.fail(
      q.size <= 50,
      "Historique de commande à rapprocher",
      409,
      "RECONCILIATION_REQUIRED"
    );

    const rows = q.docs.map(s => s.data());
    const gross = C.minor(original.totalAmount, "Total", false);

    const received = rows.filter(x =>
      x.type === "merchant_sale_received" &&
      x.userId === original.merchantUid &&
      x.status === "completed"
    );

    const paid = rows.filter(x =>
      x.type === "merchant_order_paid" &&
      x.userId === original.buyerUid &&
      x.status === "completed"
    );

    let fee;
    let state;

    if (original.status === "pending") {
      const created = rows.filter(x =>
        x.type === "merchant_order_created" &&
        x.userId === original.buyerUid &&
        C.minor(x.amount) === gross
      );

      C.fail(
        created.length === 1 &&
        !received.length &&
        !paid.length &&
        !rows.some(x => x.type === "merchant_order_refund"),
        "Ancienne réservation à rapprocher avant traitement",
        409,
        "RECONCILIATION_REQUIRED"
      );

      const buyer = await ledger.account(original.buyerUid);

      C.fail(
        buyer.locked >= gross,
        "Ancien montant bloqué manquant",
        409,
        "RECONCILIATION_REQUIRED"
      );

      await ledger.post(
        original.buyerUid,
        gross,
        0,
        "merchant_legacy_reservation_adjustment",
        gross,
        `${orderId}:legacy-basis`,
        {
          orderId,
          note: "Correction comptable d’une ancienne réservation"
        }
      );

      // Ancien tarif de la version fournie : 10 HTG.
      fee = Math.min(1000, gross);
      state = "held";

    } else if (["accepted", "completed"].includes(original.status)) {
      C.fail(
        received.length === 1 &&
        paid.length === 1 &&
        C.minor(paid[0].amount) === gross,
        "Ancien règlement à rapprocher",
        409,
        "RECONCILIATION_REQUIRED"
      );

      fee = C.minor(received[0].feeAmount ?? 0);

      C.fail(
        C.minor(received[0].amount) + fee === gross,
        "Ancien règlement incohérent",
        409,
        "RECONCILIATION_REQUIRED"
      );

      state = "settled";

    } else {
      throw new C.MerchantError(
        409,
        "Ancienne commande clôturée ou à rapprocher",
        "LEGACY_CLOSED"
      );
    }

    const changes = {
      accountingVersion: 2,
      migratedFromLegacy: true,
      fundsStatus: state,
      feeAmount: C.htg(fee),
      merchantNet: C.htg(gross - fee),
      feeRateBps: null,
      feePolicy: "legacy-fixed",
      migratedAt: timestamp()
    };

    if (state === "settled") {
      changes.settledAt =
        original.acceptedAt ||
        original.completedAt ||
        original.createdAt;
    }

    tx.update(ref("merchantOrders", orderId), {
      ...changes,
      deliveryCode: admin.firestore.FieldValue.delete()
    });

    if (original.status !== "completed") {
      tx.set(ref("merchantOrderSecrets", orderId), {
        ...codeTools().create(orderId),
        createdAt: timestamp()
      });
    }

    return { ...original, ...changes };
  }

  async function settle(tx, ledger, orderId, order, actor, source) {
    if (order.fundsStatus === "settled") return false;

    C.fail(
      order.fundsStatus === "held",
      "Fonds non disponibles pour ce règlement",
      409
    );

    const gross = C.minor(order.totalAmount, "Total", false);
    const fee = C.minor(order.feeAmount);
    const net = C.minor(order.merchantNet);

    C.fail(fee + net === gross, "Montants incohérents", 409);

    const buyer = await ledger.account(order.buyerUid);

    C.fail(
      buyer.locked >= gross && buyer.wallet >= gross,
      "Réservation insuffisante",
      409,
      "RECONCILIATION_REQUIRED"
    );

    await ledger.post(
      order.buyerUid,
      -gross,
      -gross,
      "merchant_order_paid",
      gross,
      `${orderId}:paid`,
      { orderId, merchantUid: order.merchantUid }
    );

    await ledger.post(
      order.merchantUid,
      net,
      0,
      "merchant_sale_received",
      net,
      `${orderId}:sale`,
      {
        orderId,
        buyerId: order.buyerUid,
        grossAmount: C.htg(gross),
        feeAmount: C.htg(fee),
        productName: order.productName,
        quantity: order.quantity,
        heldDelta: -C.htg(net)
      },
      net
    );

    if (fee) {
      await ledger.post(
        ADMIN_UID,
        fee,
        0,
        "merchant_sale_fee",
        fee,
        `${orderId}:fee`,
        { orderId, merchantUid: order.merchantUid },
        fee
      );

      await finance(tx, "merchant_sale_fee", fee, 0);
    }

    tx.update(ref("merchantOrders", orderId), {
      fundsStatus: "settled",
      settledAt: timestamp(),
      settlementSource: source,
      settledBy: actor
    });

    return true;
  }

  async function unreserve(tx, ledger, orderId, order, restock) {
    C.fail(
      order.fundsStatus === "held",
      "Commande déjà réglée ou remboursée",
      409
    );

    const gross = C.minor(order.totalAmount, "Total", false);
    const buyer = await ledger.account(order.buyerUid);

    C.fail(
      buyer.locked >= gross,
      "Réservation insuffisante",
      409
    );

    await ledger.post(
      order.buyerUid,
      0,
      -gross,
      "merchant_order_unlocked",
      gross,
      `${orderId}:unlocked`,
      { orderId }
    );

    tx.create(
      ref(
        "walletTransactions",
        C.sha(`merchant-event:${orderId}:sale-cancelled`)
      ),
      {
        userId: order.merchantUid,
        type: "merchant_sale_cancelled",
        scope: "merchant",
        orderId,
        amount: order.merchantNet,
        walletDelta: 0,
        lockedDelta: 0,
        heldDelta: -order.merchantNet,
        accountingVersion: 2,
        status: "completed",
        createdAt: timestamp()
      }
    );

    if (restock && order.stockRestored !== true) {
      const pRef = ref("merchantProducts", order.productId);
      const p = await tx.get(pRef);

      if (
        p.exists &&
        p.data().merchantUid === order.merchantUid
      ) {
        tx.update(pRef, {
          stock: Number(p.data().stock || 0) + order.quantity,
          version: Number(p.data().version || 0) + 1
        });
      }
    }

    tx.update(ref("merchantOrders", orderId), {
      fundsStatus: "cancelled",
      stockRestored: restock === true,
      cancelledAt: timestamp()
    });
  }

  // ACHAT : réservation du montant, sans versement au marchand.
  app.post("/api/merchant/buy-product", auth, wrap(async (req, res) => {
    C.fail(
      process.env.MERCHANT_CHECKOUT_PAUSED === "false",
      "Achats temporairement en maintenance",
      503,
      "CHECKOUT_PAUSED"
    );

    const uid = req.user.uid;
    const productId = C.id(req.body.productId);
    const qty = req.body.quantity ?? 1;
    const customer = C.contact(req.body);

    C.fail(
      Number.isInteger(qty) && qty > 0 && qty <= 1000,
      "Quantité invalide"
    );

    const op = C.operation(
      uid,
      "buy",
      operationKey(req),
      { productId, qty, customer }
    );

    const opRef = ref("merchantOperations", op.id);
    const orderRef = db.collection("merchantOrders").doc();
    const secret = codeTools().create(orderRef.id);

    const out = await run(async tx => {
      const previous = C.replay(
        await tx.get(opRef),
        op.fingerprint
      );

      if (previous) return previous;

      const buyer = await getUser(tx, uid);
      C.fail(buyer.suspended !== true, "Compte suspendu", 403);

      const pRef = ref("merchantProducts", productId);
      const p = await tx.get(pRef);

      C.fail(p.exists, "Produit introuvable", 404);

      const product = p.data();

      C.fail(
        product.isActive === true && product.archived !== true,
        "Produit indisponible",
        409
      );

      C.fail(
        product.merchantUid !== uid,
        "Vous ne pouvez pas acheter vos propres produits"
      );

      const merchant = await getUser(tx, product.merchantUid);

      C.fail(C.active(merchant), "Boutique inactive", 409);

      C.fail(
        Number.isInteger(product.stock) && product.stock >= qty,
        "Stock insuffisant",
        409
      );


const unit = C.minor(product.price, "Prix", false);
      const gross = unit * qty;
      const fee = C.feeFor(gross);
      const net = gross - fee;

      const expectedTotal = C.minor(
        req.body.expectedTotal,
        "Total confirmé",
        false
      );

      C.fail(
        gross === expectedTotal,
        "Le prix a changé. Actualise la boutique et confirme le nouveau montant.",
        409,
        "PRICE_CHANGED"
      );



      const ledger = accounting(tx);
      const a = await ledger.account(uid);

      C.money(buyer);

      C.fail(
        a.available >= gross,
        "Solde disponible insuffisant",
        409
      );

      await ledger.post(
        uid,
        0,
        gross,
        "merchant_order_created",
        gross,
        `${orderRef.id}:reserved`,
        {
          orderId: orderRef.id,
          merchantUid: product.merchantUid,
          productId,
          note: "Réservation sans débit du wallet"
        }
      );

      tx.update(pRef, {
        stock: product.stock - qty,
        version: Number(product.version || 0) + 1
      });

      tx.create(orderRef, {
        buyerUid: uid,
        merchantUid: product.merchantUid,
        productId,
        productName: product.name,
        productImage: product.images?.[0] || product.imageUrl || "",
        quantity: qty,
        unitPrice: C.htg(unit),
        totalAmount: C.htg(gross),
        feeAmount: C.htg(fee),
        merchantNet: C.htg(net),
        feeRateBps: C.RATE_BPS,
        feePolicy: "percent-3",
        accountingVersion: 2,
        status: "pending",
        fundsStatus: "held",
        deliveryTerms: merchant.merchantDeliveryTerms ||
          "À convenir avant remise du code",
        stockRestored: false,
        ...customer,
        createdAt: timestamp()
      });

      tx.create(ref("merchantOrderSecrets", orderRef.id), {
        ...secret,
        createdAt: timestamp()
      });

      tx.create(
        ref(
          "walletTransactions",
          C.sha(`merchant-event:${orderRef.id}:sale-reserved`)
        ),
        {
          userId: product.merchantUid,
          type: "merchant_sale_reserved",
          scope: "merchant",
          orderId: orderRef.id,
          amount: C.htg(net),
          grossAmount: C.htg(gross),
          feeAmount: C.htg(fee),
          walletDelta: 0,
          lockedDelta: 0,
          heldDelta: C.htg(net),
          accountingVersion: 2,
          status: "completed",
          createdAt: timestamp()
        }
      );

      notify(
        tx,
        product.merchantUid,
        uid,
        "merchant_order",
        "Nouvelle commande à préparer. Les fonds restent en attente.",
        `order:${orderRef.id}`,
        { orderId: orderRef.id }
      );

      const result = {
        success: true,
        orderId: orderRef.id,
        totalAmount: C.htg(gross),
        feeAmount: C.htg(fee)
      };

      tx.create(opRef, {
        uid,
        kind: "buy",
        fingerprint: op.fingerprint,
        result,
        createdAt: timestamp()
      });

      ledger.flush();
      return result;
    });

    res.json(out);
  }));

  app.get("/api/merchant/orders", auth, wrap(async (req, res) => {
    const buyer = req.query.side === "buyer";

    const query = db.collection("merchantOrders")
      .where(
        buyer ? "buyerUid" : "merchantUid",
        "==",
        req.user.uid
      );

    const q = await query.limit(maxRows + 1).get();

    C.fail(
      q.size <= maxRows,
      "Historique trop important : pagination nécessaire",
      413
    );

    const orders = q.docs
      .map(x => C.publicOrder(x.id, x.data()))
      .sort((a, b) => b.createdAtMs - a.createdAtMs);

    const u = await ref("users", req.user.uid).get();

    C.fail(u.exists, "Profil introuvable", 404);

    C.fail(
      u.data().suspended !== true || buyer,
      "Compte suspendu",
      403
    );

    res.json({ success: true, orders });
  }));

  app.get("/api/merchant/checkout-status", auth, wrap(async (req, res) => {
    const key = C.text(req.query.requestKey, "Référence", 80, 16);

    C.fail(/^[a-zA-Z0-9_-]+$/.test(key), "Référence invalide");

    const snap = await ref(
      "merchantOperations",
      C.sha(`${req.user.uid}:buy:${key}`)
    ).get();

    res.json({
      success: true,
      found: snap.exists,
      result: snap.exists ? snap.data().result : null
    });
  }));

  app.get("/api/merchant/order/:orderId", auth, wrap(async (req, res) => {
    const result = await run(async tx => {
      const { order } = await loadOrder(tx, req.params.orderId);
      const u = await getUser(tx, req.user.uid);

      const participant = [
        order.buyerUid,
        order.merchantUid
      ].includes(req.user.uid);

      C.fail(
        participant ||
        (
          u.suspended !== true &&
          ["admin", "merchant_manager"].includes(u.role)
        ),
        "Accès refusé",
        403
      );

      const history = await tx.get(
        db.collection("walletTransactions")
          .where("orderId", "==", req.params.orderId)
          .limit(101)
      );

      C.fail(history.size <= 100, "Historique à rapprocher", 409);

      return {
        success: true,
        order: C.publicOrder(req.params.orderId, order),

        history: history.docs
          .filter(s =>
            !participant || s.data().userId === req.user.uid
          )
          .map(s => ({
            id: s.id,
            userId: s.data().userId,
            type: s.data().type,
            amount: s.data().amount,
            status: s.data().status,
            walletDelta: s.data().walletDelta ?? null,
            atMs: C.asMs(s.data().createdAt)
          }))
      };
    });

    res.json(result);
  }));

  // Le code en clair n'est retourné qu'à l'acheteur.
  app.get("/api/merchant/order-code/:orderId", auth, wrap(async (req, res) => {
    const orderId = C.id(req.params.orderId);

    const result = await run(async tx => {
      const loaded = await loadOrder(tx, orderId);
      let order = loaded.order;

      C.fail(
        order.buyerUid === req.user.uid,
        "Code réservé à l’acheteur",
        403
      );

      C.fail(
        !["completed", "rejected", "cancelled", "refunded"]
          .includes(order.status),
        "Code indisponible pour une commande clôturée",
        409
      );

      const ledger = accounting(tx);
      const wasLegacy = order.accountingVersion !== 2;

      order = await legacyOrder(tx, orderId, order, ledger);

      let secret;

      if (wasLegacy) {
        secret = codeTools().create(orderId);

        tx.set(ref("merchantOrderSecrets", orderId), {
          ...secret,
          createdAt: timestamp()
        });

      } else {
        const s = await tx.get(
          ref("merchantOrderSecrets", orderId)
        );

        C.fail(
          s.exists,
          "Code indisponible : contacte l’assistance",
          409
        );

        secret = s.data();
      }

      C.fail(!secret.usedAt, "Code déjà utilisé", 409);

      ledger.flush();

      return {
        success: true,
        code: codeTools().read(orderId, secret),
        warning:
          "Communique ce code uniquement après réception et vérification du produit."
      };
    });

    res.set("Cache-Control", "no-store").json(result);
  }));

  function transition(kind) {
    return wrap(async (req, res) => {
      const orderId = C.id(req.body.orderId);

      const out = await run(async tx => {
        const { orderRef, order: raw } = await loadOrder(tx, orderId);

        C.fail(
          raw.merchantUid === req.user.uid,
          "Accès refusé",
          403
        );

        const merchant = await getUser(tx, req.user.uid);

        C.fail(C.active(merchant), "Boutique inactive", 403);

        const ledger = accounting(tx);

        const order = await legacyOrder(
          tx,
          orderId,
          raw,
          ledger
        );

        C.fail(
          !order.disputeId,
          "Un litige bloque cette action",
          409
        );

        if (kind === "accept") {
          C.fail(
            order.status === "pending",
            "Commande déjà traitée",
            409
          );

          tx.update(orderRef, {
            status: "accepted",
            acceptedAt: timestamp()
          });
        }

        if (kind === "ship") {
          C.fail(
            order.status === "accepted",
            "Commande non prête pour livraison",
            409
          );

          tx.update(orderRef, {
            status: "shipping",
            shippedAt: timestamp()
          });
        }

        if (kind === "reject") {
          C.fail(
            order.status === "pending",
            "Cette commande ne peut plus être refusée directement",
            409
          );

          await unreserve(
            tx,
            ledger,
            orderId,
            order,
            true
          );

          tx.update(orderRef, {
            status: "rejected",
            rejectedAt: timestamp()
          });
        }

        if (kind === "complete") {
          C.fail(
            ["accepted", "shipping"].includes(order.status),
            "Accepte la commande avant la livraison",
            409
          );

          C.fail(
            raw.accountingVersion === 2,
            "L’acheteur doit ouvrir sa commande pour obtenir son code",
            409,
            "LEGACY_CODE_REQUIRED"
          );

          const sRef = ref("merchantOrderSecrets", orderId);
          const s = await tx.get(sRef);

          C.fail(s.exists, "Code indisponible", 409);

          const secret = s.data();

          C.fail(
            !secret.usedAt,
            "Livraison déjà validée",
            409
          );

          C.fail(
            Number(secret.lockUntilMs || 0) <= Date.now(),
            "Trop de tentatives. Réessaie après 15 minutes.",
            429
          );

          if (
            !codeTools().matches(
              orderId,
              req.body.deliveryCode,
              secret
            )
          ) {
            const n = Number(secret.failedAttempts || 0) + 1;

            tx.update(sRef, {
              failedAttempts: n % 5,
              lockUntilMs: n >= 5
                ? Date.now() + 15 * 60000
                : 0
            });

            ledger.flush();

            return {
              error: "Code incorrect",
              status: 400
            };
          }

          await settle(
            tx,
            ledger,
            orderId,
            order,
            req.user.uid,
            "buyer-delivery-code"
          );

          tx.update(sRef, {
            usedAt: timestamp(),
            cipher: admin.firestore.FieldValue.delete(),
            iv: admin.firestore.FieldValue.delete(),
            tag: admin.firestore.FieldValue.delete()
          });

          tx.update(orderRef, {
            status: "completed",
            completedAt: timestamp(),
            disputeDeadlineMs:
              Date.now() + disputeHours * 3600000
          });

          audit(
            tx,
            req.user.uid,
            "delivery-confirmed",
            orderId
          );
        }

        const message =
          kind === "complete"
            ? "Livraison confirmée. Les fonds ont été libérés."
            : kind === "reject"
              ? "Commande refusée. La réservation a été annulée."
              : kind === "ship"
                ? "Votre commande est en livraison."
                : "Votre commande a été acceptée et sera préparée.";

        notify(
          tx,
          order.buyerUid,
          req.user.uid,
          "merchant_order_update",
          message,
          `${orderId}:${kind}`,
          { orderId }
        );

        ledger.flush();

        return { success: true };
      });

      if (out.error) {
        return res.status(out.status).json({
          success: false,
          error: out.error
        });
      }

      res.json(out);
    });
  }

  app.post("/api/merchant/accept-order", auth, transition("accept"));
  app.post("/api/merchant/ship-order", auth, transition("ship"));
  app.post("/api/merchant/reject-order", auth, transition("reject"));
  app.post("/api/merchant/complete-order", auth, transition("complete"));

  app.post("/api/merchant/cancel-order", auth, wrap(async (req, res) => {
    const orderId = C.id(req.body.orderId);

    await run(async tx => {
      const { orderRef, order: raw } = await loadOrder(tx, orderId);

      C.fail(raw.buyerUid === req.user.uid, "Accès refusé", 403);

      C.fail(
        raw.status === "pending" && !raw.disputeId,
        "Ouvre un litige pour cette commande",
        409
      );

      const ledger = accounting(tx);
      const order = await legacyOrder(tx, orderId, raw, ledger);

      await unreserve(tx, ledger, orderId, order, true);

      tx.update(orderRef, { status: "cancelled" });
      ledger.flush();
    });

    res.json({ success: true });
  }));

  // LITIGES
  app.post("/api/merchant/open-dispute", auth, wrap(async (req, res) => {
    const orderId = C.id(req.body.orderId);
    const reason = C.text(req.body.reason, "Motif", 1000, 10);
    const disputeId = C.sha(`order-dispute:${orderId}`);

    await run(async tx => {
      const { orderRef, order: raw } = await loadOrder(tx, orderId);

      C.fail(
        raw.buyerUid === req.user.uid,
        "Seul l’acheteur ouvre la réclamation",
        403
      );

      const old = await tx.get(ref("merchantDisputes", disputeId));
      if (old.exists) return;

      C.fail(
        !["rejected", "cancelled", "refunded"].includes(raw.status),
        "Commande clôturée",
        409
      );

      const ledger = accounting(tx);
      const order = await legacyOrder(tx, orderId, raw, ledger);

      if (order.status === "completed") {
        C.fail(
          Number(order.disputeDeadlineMs || 0) >= Date.now(),
          "Délai de contestation expiré. Contacte l’assistance.",
          409
        );
      }

      tx.create(ref("merchantDisputes", disputeId), {
        orderId,
        buyerUid: order.buyerUid,
        merchantUid: order.merchantUid,
        previousStatus: order.status,
        reason,
        status: "open",
        messages: [{
          uid: req.user.uid,
          text: reason,
          atMs: Date.now()
        }],
        evidence: [],
        createdAt: timestamp()
      });

      tx.update(orderRef, {
        disputeId,
        status: "disputed"
      });

      notify(
        tx,
        order.merchantUid,
        req.user.uid,
        "merchant_dispute",
        "Une réclamation a été ouverte sur une commande.",
        `dispute:${disputeId}`,
        { orderId }
      );

      audit(tx, req.user.uid, "dispute-opened", orderId);
      ledger.flush();
    });

    res.json({ success: true, disputeId });
  }));

  app.post("/api/merchant/request-review", auth, wrap(async (req, res) => {
    const orderId = C.id(req.body.orderId);

    const reason = C.text(
      req.body.reason,
      "Motif de l’examen",
      1000,
      10
    );

    const disputeId = C.sha(`order-dispute:${orderId}`);

    await run(async tx => {
      const { orderRef, order: raw } = await loadOrder(tx, orderId);

      C.fail(raw.merchantUid === req.user.uid, "Accès refusé", 403);

      const old = await tx.get(ref("merchantDisputes", disputeId));

      C.fail(
        !old.exists,
        "Un dossier existe déjà pour cette commande",
        409
      );

      C.fail(
        ["accepted", "shipping"].includes(raw.status),
        "Commande non admissible à cet examen",
        409
      );

      const wait =
        Number(process.env.MERCHANT_REVIEW_AFTER_HOURS || 72) *
        3600000;

      C.fail(
        Date.now() - C.asMs(raw.acceptedAt || raw.createdAt) >= wait,
        "Délai avant demande d’examen non atteint",
        409
      );

      const ledger = accounting(tx);
      const order = await legacyOrder(tx, orderId, raw, ledger);

      tx.create(ref("merchantDisputes", disputeId), {
        orderId,
        buyerUid: order.buyerUid,
        merchantUid: order.merchantUid,
        previousStatus: order.status,
        reason,
        openedByMerchant: true,
        status: "open",
        messages: [{
          uid: req.user.uid,
          text: reason,
          atMs: Date.now()
        }],
        evidence: [],
        createdAt: timestamp()
      });

      tx.update(orderRef, {
        disputeId,
        status: "disputed"
      });

      audit(
        tx,
        req.user.uid,
        "stalled-order-review",
        orderId
      );

      notify(
        tx,
        order.buyerUid,
        req.user.uid,
        "merchant_dispute",
        "Une commande sans confirmation a été soumise à examen. Aucun versement automatique.",
        `stalled:${disputeId}`,
        { orderId }
      );

      ledger.flush();
    });

    res.json({ success: true, disputeId });
  }));

  async function disputeAccess(tx, disputeId, uid) {
    const dRef = ref("merchantDisputes", disputeId);
    const d = await tx.get(dRef);

    C.fail(d.exists, "Litige introuvable", 404);

    const user = await getUser(tx, uid);
    const data = d.data();

    C.fail(
      [data.buyerUid, data.merchantUid].includes(uid) ||
      (
        user.suspended !== true &&
        ["admin", "merchant_manager"].includes(user.role)
      ),
      "Accès refusé",
      403
    );

    return { dRef, data, user };
  }

  app.get("/api/merchant/disputes", auth, wrap(async (req, res) => {
    const u = await ref("users", req.user.uid).get();

    C.fail(u.exists, "Profil introuvable", 404);

    const isStaff =
      u.data().suspended !== true &&
      ["admin", "merchant_manager"].includes(u.data().role);

    let rows;

    if (isStaff && req.query.staff === "1") {
      const q = await db.collection("merchantDisputes")
        .limit(maxRows + 1)
        .get();

      rows = q.docs;

    } else {
      const [a, b] = await Promise.all([
        db.collection("merchantDisputes")
          .where("buyerUid", "==", req.user.uid)
          .get(),

        db.collection("merchantDisputes")
          .where("merchantUid", "==", req.user.uid)
          .get()
      ]);

      rows = [...new Map(
        [...a.docs, ...b.docs].map(d => [d.id, d])
      ).values()];
    }

    C.fail(rows.length <= maxRows, "Historique trop important", 413);

    res.json({
      success: true,
      disputes: rows
        .map(x => ({
          id: x.id,
          ...x.data(),
          createdAtMs: C.asMs(x.data().createdAt)
        }))
        .sort((a, b) => b.createdAtMs - a.createdAtMs)
    });
  }));

  app.post("/api/merchant/dispute-message", auth, wrap(async (req, res) => {
    const disputeId = C.id(req.body.disputeId);
    const message = C.text(req.body.message, "Message", 2000, 2);

    await run(async tx => {
      const { dRef, data } = await disputeAccess(
        tx,
        disputeId,
        req.user.uid
      );

      C.fail(data.status === "open", "Litige clôturé", 409);

      C.fail(
        (data.messages || []).length < 100,
        "Limite de messages atteinte",
        409
      );

      tx.update(dRef, {
        messages: [
          ...(data.messages || []),
          {
            uid: req.user.uid,
            text: message,
            atMs: Date.now()
          }
        ]
      });
    });

    res.json({ success: true });
  }));

  app.post("/api/merchant/resolve-dispute", auth, wrap(async (req, res) => {
    const disputeId = C.id(req.body.disputeId);
    const decision = req.body.decision;

    const reason = C.text(
      req.body.reason,
      "Décision motivée",
      2000,
      10
    );

    C.fail(
      ["refund", "release"].includes(decision),
      "Décision invalide"
    );

    await run(async tx => {
      const staff = await staffUser(tx, req.user.uid);

      const { dRef, data: d } = await disputeAccess(
        tx,
        disputeId,
        req.user.uid
      );

      C.fail(d.status === "open", "Litige déjà traité", 409);

      const { orderRef, order } = await loadOrder(tx, d.orderId);

      C.fail(
        order.accountingVersion === 2,
        "Commande à rapprocher",
        409
      );

      const ledger = accounting(tx);
      const gross = C.minor(order.totalAmount);
      const net = C.minor(order.merchantNet);
      const fee = C.minor(order.feeAmount);

      let funding = "none";

      if (decision === "refund") {
        if (order.fundsStatus === "held") {
          await unreserve(
            tx,
            ledger,
            d.orderId,
            order,
            req.body.restock === true
          );

          funding = "reservation";

        } else {
          C.fail(
            order.fundsStatus === "settled",
            "Remboursement impossible dans cet état",
            409
          );

          if (req.body.funding === "platform") {
            C.fail(
              staff.role === "admin",
              "Seul l’admin peut financer ce remboursement",
              403
            );

            C.fail(
              order.buyerUid !== ADMIN_UID,
              "Utilise la reprise de la vente pour cet achat admin",
              409
            );

            const a = await ledger.account(ADMIN_UID);

            C.fail(
              a.wallet - a.locked >= gross,
              "Fonds plateforme insuffisants. Le dossier reste ouvert.",
              409
            );

            await ledger.post(
              ADMIN_UID,
              -gross,
              0,
              "merchant_platform_refund",
              gross,
              `${d.orderId}:platform-refund`,
              { orderId: d.orderId }
            );

            await finance(
              tx,
              "merchant_platform_refund",
              0,
              gross
            );

            funding = "platform";

          } else {
            // Fusion des mouvements si l’acheteur ou le marchand est l’admin.
            const deltas = new Map();

            for (const [uid, amount] of [
              [order.merchantUid, -net],
              [ADMIN_UID, -fee],
              [order.buyerUid, gross]
            ]) {
              deltas.set(
                uid,
                (deltas.get(uid) || 0) + amount
              );
            }

            for (const [uid, delta] of deltas) {
              const a = await ledger.account(uid);

              C.fail(
                a.wallet - a.locked >= Math.max(0, -delta),
                "Fonds retirés ou réservés. Financement admin nécessaire.",
                409
              );
            }

            await ledger.post(
              order.buyerUid,
              gross,
              0,
              "merchant_order_refund",
              gross,
              `${d.orderId}:buyer-refund`,
              { orderId: d.orderId, funding: "sale" }
            );

            await ledger.post(
              order.merchantUid,
              -net,
              0,
              "merchant_sale_refunded",
              net,
              `${d.orderId}:seller-refund`,
              {
                orderId: d.orderId,
                grossAmount: C.htg(gross)
              },
              -net
            );

            if (fee) {
              await ledger.post(
                ADMIN_UID,
                -fee,
                0,
                "merchant_sale_fee_refunded",
                fee,
                `${d.orderId}:fee-refund`,
                { orderId: d.orderId },
                -fee
              );

              await finance(
                tx,
                "merchant_sale_fee_refunded",
                0,
                fee
              );
            }

            funding = "sale";
          }

          if (funding === "platform") {
            await ledger.post(
              order.buyerUid,
              gross,
              0,
              "merchant_order_refund",
              gross,
              `${d.orderId}:buyer-refund`,
              { orderId: d.orderId, funding }
            );
          }

          tx.update(orderRef, {
            fundsStatus: "refunded",
            refundedAt: timestamp(),
            refundFunding: funding
          });
        }

        tx.update(orderRef, { status: "refunded" });

      } else {
        await settle(
          tx,
          ledger,
          d.orderId,
          order,
          req.user.uid,
          "staff-dispute-decision"
        );

        tx.update(orderRef, {
          status: "completed",
          completedAt: order.completedAt || timestamp(),
          disputeDeadlineMs: Date.now()
        });

        const secret = await tx.get(
          ref("merchantOrderSecrets", d.orderId)
        );

        if (secret.exists) {
          tx.update(secret.ref, {
            usedAt: timestamp(),
            cipher: admin.firestore.FieldValue.delete(),
            iv: admin.firestore.FieldValue.delete(),
            tag: admin.firestore.FieldValue.delete()
          });
        }
      }

      tx.update(orderRef, { disputeId: null });

      tx.update(dRef, {
        status: "resolved",
        decision,
        reasonDecision: reason,
        funding,
        resolvedBy: req.user.uid,
        resolvedAt: timestamp()
      });

      for (const uid of [order.buyerUid, order.merchantUid]) {
        notify(
          tx,
          uid,
          req.user.uid,
          "merchant_dispute_resolved",
          `Réclamation traitée : ${reason}`,
          `${disputeId}:resolved:${uid}`,
          { orderId: d.orderId }
        );
      }

      audit(
        tx,
        req.user.uid,
        "dispute-resolved",
        d.orderId,
        { decision, reason, funding }
      );

      ledger.flush();
    });

    res.json({ success: true });
  }));

  // STOCKAGE PRIVÉ DES PREUVES
  let privateBucketReady;

  async function privateBucket() {
    if (!privateBucketReady) {
      privateBucketReady = (async () => {
        const found = await supabase.storage.getBucket(
          "merchant-private"
        );

        if (found.error) {
          const made = await supabase.storage.createBucket(
            "merchant-private",
            {
              public: false,
              fileSizeLimit: 15 * 1024 * 1024
            }
          );

          C.fail(
            !made.error,
            "Impossible de préparer le stockage privé",
            503
          );

        } else {
          C.fail(
            found.data.public === false,
            "Le bucket merchant-private doit être privé",
            503
          );
        }
      })().catch(e => {
        privateBucketReady = null;
        throw e;
      });
    }

    await privateBucketReady;
  }

  function fileType(file, videoAllowed = false) {
    C.fail(
      file?.buffer?.length &&
      file.buffer.length <= 15 * 1024 * 1024,
      "Fichier manquant ou trop volumineux"
    );

    const b = file.buffer;

    if (b[0] === 0xff && b[1] === 0xd8 && b[2] === 0xff) {
      return { ext: "jpg", mime: "image/jpeg" };
    }

    if (
      b.subarray(0, 8).equals(
        Buffer.from([137, 80, 78, 71, 13, 10, 26, 10])
      )
    ) {
      return { ext: "png", mime: "image/png" };
    }

    if (
      b.toString("ascii", 0, 4) === "RIFF" &&
      b.toString("ascii", 8, 12) === "WEBP"
    ) {
      return { ext: "webp", mime: "image/webp" };
    }

    if (
      videoAllowed &&
      b.length > 12 &&
      b.toString("ascii", 4, 8) === "ftyp"
    ) {
      return { ext: "mp4", mime: "video/mp4" };
    }

    throw new C.MerchantError(
      400,
      videoAllowed
        ? "Utilise JPG, PNG, WebP ou une vidéo MP4"
        : "Utilise une image JPG, PNG ou WebP"
    );
  }

  if (uploadMemory) {
    const oneFile = (field, fn) => (req, res) => {
      uploadMemory.single(field)(req, res, error => {
        if (error) {
          return res.status(400).json({
            success: false,
            error: "Fichier trop volumineux ou invalide"
          });
        }

        wrap(fn)(req, res);
      });
    };

    app.post(
      "/api/merchant/upload-image",
      auth,
      oneFile("file", async (req, res) => {
        const user = await ref("users", req.user.uid).get();

        C.fail(
          user.exists && C.active(user.data()),
          "Boutique inactive",
          403
        );

        const kind = req.body.kind;

        C.fail(
          ["shop", "product"].includes(kind),
          "Destination invalide"
        );

        const type = fileType(req.file);

        const path =
          `${kind === "shop" ? "shops" : "products"}/` +
          `${req.user.uid}/${crypto.randomUUID()}.${type.ext}`;

        const up = await supabase.storage
          .from("media")
          .upload(path, req.file.buffer, {
            contentType: type.mime,
            upsert: false
          });

        C.fail(!up.error, "Téléversement impossible", 503);

        const url = supabase.storage
          .from("media")
          .getPublicUrl(path).data.publicUrl;

        res.json({ success: true, url });
      })
    );

    app.post(
      "/api/merchant/dispute-evidence",
      auth,
      oneFile("file", async (req, res) => {
        const disputeId = C.id(req.body.disputeId);
        const type = fileType(req.file, true);

        await run(async tx => {
          const { data } = await disputeAccess(
            tx,
            disputeId,
            req.user.uid
          );

          C.fail(
            data.status === "open" &&
            (data.evidence || []).length < 12,
            "Litige fermé ou limite de preuves atteinte",
            409
          );
        });

        await privateBucket();

        const fileId = crypto.randomUUID();

        const storagePath =
          `${disputeId}/${req.user.uid}/${fileId}.${type.ext}`;

        const up = await supabase.storage
          .from("merchant-private")
          .upload(storagePath, req.file.buffer, {
            contentType: type.mime,
            upsert: false
          });

        C.fail(!up.error, "Envoi de la preuve impossible", 503);

        try {
          await run(async tx => {
            const { dRef, data } = await disputeAccess(
              tx,
              disputeId,
              req.user.uid
            );

            C.fail(
              data.status === "open" &&
              (data.evidence || []).length < 12,
              "Litige fermé ou limite de preuves atteinte",
              409
            );

            tx.update(dRef, {
              evidence: [
                ...(data.evidence || []),
                {
                  id: fileId,
                  uid: req.user.uid,
                  path: storagePath,
                  mime: type.mime,
                  atMs: Date.now()
                }
              ]
            });
          });

        } catch (e) {
          await supabase.storage
            .from("merchant-private")
            .remove([storagePath]);

          throw e;
        }

        res.json({ success: true });
      })
    );
  }

  app.get(
    "/api/merchant/dispute-evidence/:disputeId/:fileId",
    auth,
    wrap(async (req, res) => {
      const disputeId = C.id(req.params.disputeId);
      const fileId = C.id(req.params.fileId);

      const evidence = await run(async tx => {
        const { data } = await disputeAccess(
          tx,
          disputeId,
          req.user.uid
        );

        const e = (data.evidence || []).find(
          x => x.id === fileId
        );

        C.fail(e, "Preuve introuvable", 404);
        return e;
      });

      await privateBucket();

      const d = await supabase.storage
        .from("merchant-private")
        .download(evidence.path);

      C.fail(!d.error, "Preuve indisponible", 503);

      res.set("Cache-Control", "no-store")
        .type(evidence.mime)
        .send(Buffer.from(await d.data.arrayBuffer()));
    })
  );

  // RÉACTIVATION : aucune suppression ni nouvelle facturation.
  app.post("/api/merchant/reactivate", auth, wrap(async (req, res) => {
    const uid = C.id(req.body.targetUid);

    await run(async tx => {
      await staffUser(tx, req.user.uid);
      const u = await getUser(tx, uid);

      C.fail(
        u.suspended !== true &&
        u.merchantStatus === "suspended" &&
        u.merchantApprovedAt,
        "Réactivation non autorisée",
        409
      );

      tx.update(ref("users", uid), {
        merchantStatus: "active",
        merchantEnabled: true,
        merchantAccessModel: "free-v2",
        merchantRenewalBlocked: false,
        merchantSuspensionReason: ""
      });

      notify(
        tx,
        uid,
        req.user.uid,
        "merchant_reactivated",
        "Ta boutique est réactivée gratuitement.",
        `reactivate:${uid}:${crypto.randomUUID()}`
      );

      audit(tx, req.user.uid, "reactivate", uid);
    });

    res.json({ success: true });
  }));

  // MIGRATION DES ANCIENNES COMMANDES : admin uniquement.
  let migrationRunning = false;

  app.post(
    "/api/merchant/admin/migrate-orders",
    auth,
    wrap(async (req, res) => {
      await run(tx => staffUser(tx, req.user.uid, true));

      const apply = req.body.apply === true;

      if (apply) {
        C.fail(
          process.env.MERCHANT_CHECKOUT_PAUSED === "true",
          "Mettre les achats en maintenance avant la migration",
          409
        );

        C.fail(
          req.body.confirm === "MIGRATE_ORDERS_V2",
          "Confirmation de migration manquante"
        );
      }

      C.fail(
        !migrationRunning,
        "Une migration est déjà en cours",
        409
      );

      migrationRunning = true;

      try {
        codeTools();

        const snapshot = await db.collection("merchantOrders")
          .where("status", "in", ["pending", "accepted"])
          .limit(maxRows + 1)
          .get();

        C.fail(
          snapshot.size <= maxRows,
          "Migration trop volumineuse : traitement par lots nécessaire",
          413
        );

        const selected = snapshot.docs
          .filter(s => s.data().accountingVersion !== 2)
          .sort((a, b) =>
            Number(b.data().totalAmount || 0) -
            Number(a.data().totalAmount || 0)
          );

        const result = {
          success: true,
          apply,
          candidates: selected.length,
          migrated: 0,
          errors: []
        };

        if (apply) {
          for (const snap of selected) {
            try {
              const changed = await run(async tx => {
                await staffUser(tx, req.user.uid, true);

                const fresh = await tx.get(snap.ref);

                if (
                  !fresh.exists ||
                  fresh.data().accountingVersion === 2
                ) {
                  return false;
                }

                const ledger = accounting(tx);

                await legacyOrder(
                  tx,
                  snap.id,
                  fresh.data(),
                  ledger
                );

                ledger.flush();

                audit(
                  tx,
                  req.user.uid,
                  "legacy-order-migrated",
                  snap.id
                );

                return true;
              });

              if (changed) result.migrated++;

            } catch (e) {
              result.errors.push({
                orderId: snap.id,
                error: e.message
              });
            }
          }
        }

        result.success = result.errors.length === 0;
        res.json(result);

      } finally {
        migrationRunning = false;
      }
    })
  );

  return {
    C,
    Staged,
    run,
    accounting,
    legacyOrder,
    loadOrder,
    settle,
    unreserve,
    getUser,
    staffUser,
    finance,
    notify,
    audit,
    ref,
    timestamp,
    wrap,
    maxRows
  };
})();


app.post(
  "/api/merchant/renew-access",
  verifyFirebaseToken,
  (req, res) => {
    return res.status(410).json({
      success: false,
      error: "L’accès marchand est gratuit. Aucun renouvellement payant n’est nécessaire.",
      code: "MERCHANT_ACCESS_FREE"
    });
  }
);



/* ================= BLUE PAYMENT BACKEND ================= */

app.post("/api/blue/request-payment", verifyFirebaseToken, async (req,res)=>{
  try{
    const uid = req.user.uid;
    const amount = 250;

    const userRef = db.collection("users").doc(uid);

    await db.runTransaction(async (t)=>{
      const userSnap = await t.get(userRef);

      if(!userSnap.exists){
        throw new Error("Utilisateur introuvable");
      }

      const userData = userSnap.data() || {};

      if(userData.suspended === true){
        throw new Error("Compte suspendu");
      }

      if(
        userData.verification?.type === "blue" &&
        userData.verification?.status === "active" &&
        userData.verification?.expiresAt &&
        userData.verification.expiresAt.toMillis() > Date.now()
      ){
        throw new Error("Vous avez déjà un badge Blue actif");
      }

      const pendingSnap = await db.collection("payments")
        .where("userId","==",uid)
        .where("type","==","blue")
        .where("status","==","pending")
        .limit(1)
        .get();

      if(!pendingSnap.empty){
        throw new Error("Demande déjà en attente");
      }


const { wallet, walletLocked, available } = getMoneyState(userData);

      if(available < amount){
        throw new Error("Solde insuffisant");
      }

      const newLocked = walletLocked + amount;
      const paymentRef = db.collection("payments").doc();
      const txRef = db.collection("walletTransactions").doc();

      t.update(userRef,{
        walletLocked: newLocked
      });

      t.set(paymentRef,{
        userId: uid,
        amount,
        type: "blue",
        status: "pending",
        createdAt: admin.firestore.FieldValue.serverTimestamp()
      });

      t.set(txRef,{
        userId: uid,
        type: "blue_payment_pending",
        amount,
        status: "pending",
        paymentId: paymentRef.id,
        walletLockedAfter: newLocked,
        balanceAfter: wallet,
        createdAt: admin.firestore.FieldValue.serverTimestamp()
      });
    });

    return res.json({success:true});
  }catch(e){
    console.log("BLUE REQUEST PAYMENT ERROR:", e);
    return res.status(500).json({error:e.message});
  }
});





app.post("/api/blue/approve-payment", verifyFirebaseToken, async (req,res)=>{
  try{
    const staffUid = req.user.uid;
    const { paymentId } = req.body;

    if(typeof paymentId !== "string" || !paymentId.trim()){
      return res.status(400).json({error:"paymentId invalide"});
    }

    const staffRef = db.collection("users").doc(staffUid);
    const adminRef = db.collection("users").doc(ADMIN_UID);
    const paymentRef = db.collection("payments").doc(paymentId);
    const statsRef = db.collection("adminStats").doc("finance");

    await db.runTransaction(async (t)=>{

      // ✅ TOUS LES READS D’ABORD
      const staffSnap = await t.get(staffRef);
      const adminSnap = await t.get(adminRef);
      const paymentSnap = await t.get(paymentRef);
      const statsDoc = await t.get(statsRef);

      if(!staffSnap.exists){
        throw new Error("Accès refusé");
      }

      if(!adminSnap.exists){
        throw new Error("Admin introuvable");
      }

      if(!paymentSnap.exists){
        throw new Error("Paiement introuvable");
      }

      const staffData = staffSnap.data() || {};
      const role = staffData.role || "";

      if(role !== "admin" && role !== "badge_manager"){
        throw new Error("Accès refusé");
      }

      const paymentData = paymentSnap.data() || {};

      if(paymentData.type !== "blue"){
        throw new Error("Paiement invalide");
      }

      if(paymentData.status !== "pending"){
        throw new Error("Déjà traité");
      }

      const userRef = db.collection("users").doc(paymentData.userId);
      const userSnap = await t.get(userRef);

      if(!userSnap.exists){
        throw new Error("Utilisateur introuvable");
      }

      const userData = userSnap.data() || {};
      const wallet = Number(userData.wallet || 0);
      const walletLocked = Number(userData.walletLocked || 0);
      const amount = Number(paymentData.amount || 0);

      if(walletLocked < amount){
        throw new Error("Montant bloqué invalide");
      }

      let baseMs = Date.now();

      if(
        userData.verification?.type === "blue" &&
        userData.verification?.status === "active" &&
        userData.verification?.expiresAt &&
        userData.verification.expiresAt.toMillis() > Date.now()
      ){
        baseMs = userData.verification.expiresAt.toMillis();
      }


// ... (juste au-dessus, tu as la vérification de la date d'expiration)
      const expiresAt = admin.firestore.Timestamp.fromMillis(
        baseMs + (30 * 24 * 60 * 60 * 1000)
      );

      // ================= DÉBUT DES MODIFICATIONS =================
      const isBuyerAdmin = (paymentData.userId === ADMIN_UID);
      const newLocked = walletLocked - amount;

      let newWallet = wallet;
      let newAdminWallet = 0;
      let newAdminEarned = 0;
      let addedToStats = 0;

      if (!isBuyerAdmin) {
        // Acheteur normal : on déduit l'argent et on paie l'admin
        newWallet = wallet - amount;
        
        const adminData = adminSnap.data() || {};
        newAdminWallet = Number(adminData.wallet || 0) + amount;
        newAdminEarned = Number(adminData.walletEarned || 0) + amount;
        addedToStats = amount;
      }

      let stats = statsDoc.exists ? statsDoc.data() : {
        totalIn: 0,
        totalOut: 0,
        netTotal: 0,
        transactionsCount: 0,
        typeTotals: {}
      };

      const type = "blue_payment_received";

      if(!stats.typeTotals){
        stats.typeTotals = {};
      }

      if(!stats.typeTotals[type]){
        stats.typeTotals[type] = { in:0, out:0, count:0 };
      }

      if (!isBuyerAdmin) {
        stats.totalIn = Number(stats.totalIn || 0) + addedToStats;
        stats.transactionsCount = Number(stats.transactionsCount || 0) + 1;
        stats.typeTotals[type].in = Number(stats.typeTotals[type].in || 0) + addedToStats;
        stats.typeTotals[type].count = Number(stats.typeTotals[type].count || 0) + 1;
        stats.netTotal = stats.totalIn - stats.totalOut;
      }

      const adminTxRef = db.collection("walletTransactions").doc();
      const userTxRef = db.collection("walletTransactions").doc();
      const notifRef = db.collection("notifications").doc();

      // ✅ APRÈS TOUS LES READS : WRITES
      if (isBuyerAdmin) {
        // L'Admin s'auto-approuve : On débloque juste l'argent, on ne déduit rien
        t.update(userRef, {
          walletLocked: newLocked,
          verification: {
            type: "blue",
            status: "active",
            expiresAt
          }
        });
      } else {
        // Utilisateur Normal
        t.update(userRef, {
          wallet: newWallet,
          walletLocked: newLocked,
          verification: {
            type: "blue",
            status: "active",
            expiresAt
          }
        });

        t.update(adminRef, {
          wallet: newAdminWallet,
          walletEarned: newAdminEarned
        });

        t.set(adminTxRef, {
          userId: ADMIN_UID,
          type: "blue_payment_received",
          amount,
          status: "completed",
          payerId: paymentData.userId,
          paymentId,
          balanceAfter: newAdminWallet,
          approvedBy: staffUid,
          createdAt: admin.firestore.FieldValue.serverTimestamp()
        });

        t.set(statsRef, stats);
      }

      t.update(paymentRef, {
        status: "approved",
        approvedAt: admin.firestore.FieldValue.serverTimestamp(),
        approvedBy: staffUid
      });

      t.set(userTxRef, {
        userId: paymentData.userId,
        type: "blue_payment_completed",
        amount: isBuyerAdmin ? 0 : amount, // L'admin ne dépense rien au final
        status: "completed",
        paymentId,
        balanceAfter: newWallet,
        walletLockedAfter: newLocked,
        createdAt: admin.firestore.FieldValue.serverTimestamp()
      });

      t.set(notifRef, {
        to: paymentData.userId,
        from: staffUid,
        type: "blue_approved",
        read: false,
        userAvatar: userData.avatar || null,
        expiresAt: expiresAt,  
        createdAt: admin.firestore.FieldValue.serverTimestamp()
      });
      // ================= FIN DES MODIFICATIONS =================

}); // Fin du db.runTransaction

    // 🔥 MODIFICATION : Déclencheur Push
    await sendPushNotification(
      paymentData.userId,
      "Badge Blue Approuvé ! ✅",
      "Félicitations, votre compte a été vérifié.",
      { type: "blue_approved" }
    );

    return res.json({success:true});


  }catch(e){
    console.log("BLUE APPROVE ERROR:", e);
    return res.status(500).json({error:e.message});
  }
});



app.post("/api/blue/reject-payment", verifyFirebaseToken, async (req,res)=>{
  try{
    const staffUid = req.user.uid;
    const { paymentId } = req.body;

    if(typeof paymentId !== "string" || !paymentId.trim()){
      return res.status(400).json({error:"paymentId invalide"});
    }

    const staffRef = db.collection("users").doc(staffUid);
    const paymentRef = db.collection("payments").doc(paymentId);

    await db.runTransaction(async (t)=>{
      const staffSnap = await t.get(staffRef);
      const paymentSnap = await t.get(paymentRef);

      if(!staffSnap.exists){
        throw new Error("Accès refusé");
      }

      if(!paymentSnap.exists){
        throw new Error("Paiement introuvable");
      }

      const staffData = staffSnap.data() || {};
      const role = staffData.role || "";

      if(role !== "admin" && role !== "badge_manager"){
        throw new Error("Accès refusé");
      }

      const paymentData = paymentSnap.data() || {};

      if(paymentData.type !== "blue"){
        throw new Error("Paiement invalide");
      }

      if(paymentData.status !== "pending"){
        throw new Error("Déjà traité");
      }

      const userRef = db.collection("users").doc(paymentData.userId);
      const userSnap = await t.get(userRef);

      if(!userSnap.exists){
        throw new Error("Utilisateur introuvable");
      }

      const userData = userSnap.data() || {};
      const walletLocked = Number(userData.walletLocked || 0);
      const amount = Number(paymentData.amount || 0);
      const newLocked = Math.max(0, walletLocked - amount);

      t.update(userRef,{
        walletLocked: newLocked
      });

      t.update(paymentRef,{
        status:"rejected",
        rejectedAt: admin.firestore.FieldValue.serverTimestamp(),
        rejectedBy: staffUid
      });

      const userTxRef = db.collection("walletTransactions").doc();

      t.set(userTxRef,{
        userId: paymentData.userId,
        type: "blue_payment_rejected",
        amount,
        status: "failed",
        paymentId,
        balanceAfter: Number(userData.wallet || 0),
        walletLockedAfter: newLocked,
        createdAt: admin.firestore.FieldValue.serverTimestamp()
      });
    });

    return res.json({success:true});
  }catch(e){
    console.log("BLUE REJECT ERROR:", e);
    return res.status(500).json({error:e.message});
  }
});



// Helper asynchrone pour nettoyer les fichiers temporaires sans bloquer Node.js
async function unlinkSafe(filePath) {
  if (!filePath) return;
  try {
    if (fs.existsSync(filePath)) {
      await fs.promises.unlink(filePath);
    }
  } catch (err) {
    console.error(`⚠️ Erreur suppression fichier temporaire ${filePath}:`, err.message);
  }
}

// Configuration centralisée des seuils de modération
const MODERATION_CONFIG = {
  nuditySexualActivity: 0.5,
  nuditySexualDisplay: 0.5,
  nudityErotica: 0.5,
  nudityVerySuggestive: 0.6,
  goreProb: 0.5,
  wadWeapons: 0.5,
  wadDrugs: 0.5,
  forbiddenKeywords: ["tiktok"]
};


app.post("/api/optimize-video", verifyFirebaseToken, async (req, res) => {
  const { videoId, videoUrl: bodyVideoUrl, filePath: bodyFilePath } = req.body;

  if (!videoId || typeof videoId !== "string") {
    return res.status(400).json({ error: "videoId requis" });
  }

  // 1️⃣ VÉRIFICATION DU PROPRIÉTAIRE ET RÉCUPÉRATION DES DONNÉES FIRESTORE
  const videoRef = db.collection("videos").doc(videoId);
  let videoDoc;
  try {
    videoDoc = await videoRef.get();
    if (!videoDoc.exists) {
      return res.status(404).json({ error: "Vidéo introuvable" });
    }
  } catch (e) {
    return res.status(500).json({ error: "Erreur lecture Firestore" });
  }

  const videoData = videoDoc.data();

  // Sécurité : Seul le propriétaire ou l'admin peut optimiser cette vidéo
  if (videoData.userId !== req.user.uid && req.user.uid !== ADMIN_UID) {
    return res.status(403).json({ error: "Accès non autorisé à cette vidéo" });
  }

  // Vérification de verrouillage pour éviter des traitements simultanés
  if (videoData.moderationStatus === "processing") {
    return res.status(409).json({ error: "Traitement déjà en cours pour cette vidéo" });
  }

  // Fallbacks intelligents si les champs manquent dans Firestore
  const sourceVideoUrl = videoData.videoUrl || (videoData.mediaUrls && videoData.mediaUrls[0]) || bodyVideoUrl;
  const oldFilePath = videoData.filePath || bodyFilePath;

  if (!sourceVideoUrl || !oldFilePath) {
    return res.status(400).json({ error: "Metadonnées vidéo incomplètes (videoUrl/filePath)" });
  }

  // Passer Firestore en statut 'processing'
  await videoRef.update({
    moderationStatus: "processing",
    videoUrl: sourceVideoUrl,
    filePath: oldFilePath,
    processingStartedAt: admin.firestore.FieldValue.serverTimestamp()
  });

  // Réponse HTTP immédiate pour éviter le timeout de Render
  res.json({ success: true, message: "Traitement démarré en arrière-plan" });

  // Variables de gestion des fichiers temporaires localement
  const uniqueId = crypto.randomUUID();
  const localDir = path.join(__dirname, "uploads");
  const localVideoPath = path.join(localDir, `raw_${uniqueId}.mp4`);
  const compressedVideoPath = path.join(localDir, `compressed_${uniqueId}.mp4`);
  let framePaths = [];
  let newUploadedFilePath = null;

  // =========================================================================
  // 🔄 EXÉCUTION EN ARRIÈRE-PLAN
  // =========================================================================
  (async () => {
    try {
      if (!fs.existsSync(localDir)) {
        await fs.promises.mkdir(localDir, { recursive: true });
      }

      // 2️⃣ TÉLÉCHARGEMENT UNIQUE EN LOCAL
      console.log(`⬇️ [1/5] Téléchargement local de la vidéo pour ${videoId}...`);
      const downloadResponse = await axios({
        method: 'get',
        url: sourceVideoUrl,
        responseType: 'stream',
        timeout: 35000
      });
      await pipeline(downloadResponse.data, fs.createWriteStream(localVideoPath));

      // 3️⃣ EXTRACTION ET ANALYSE IA (SIGHTENGINE)
      let modStatus = "clean";
      const timestamps = ['25%', '50%', '75%'];

      for (let i = 0; i < timestamps.length; i++) {
        if (modStatus === "flagged") break;

        const framePath = path.join(localDir, `frame_${uniqueId}_${i}.jpg`);
        framePaths.push(framePath);

        console.log(`🤖 [2/5] Screenshot IA (${timestamps[i]}) pour ${videoId}...`);
        await new Promise((resolve, reject) => {
          ffmpeg(localVideoPath)
            .screenshots({
              timestamps: [timestamps[i]],
              filename: path.basename(framePath),
              folder: localDir,
              size: '320x240'
            })
            .on("end", resolve)
            .on("error", reject);
        });

        if (fs.existsSync(framePath)) {
          try {
            const FormData = require('form-data');
            const formData = new FormData();
            formData.append('media', fs.createReadStream(framePath));
            formData.append('models', 'nudity-2.0,wad,gore,text');
            formData.append('api_user', process.env.SIGHTENGINE_USER || '');
            formData.append('api_secret', process.env.SIGHTENGINE_SECRET || '');

            const seResponse = await axios({
              method: 'post',
              url: 'https://api.sightengine.com/1.0/check.json',
              data: formData,
              headers: formData.getHeaders(),
              timeout: 15000
            });

            const result = seResponse.data;
            if (result && result.status === "success") {
              const fullJsonStr = JSON.stringify(result).toLowerCase();
              for (const kw of MODERATION_CONFIG.forbiddenKeywords) {
                if (fullJsonStr.includes(kw)) {
                  console.log(`⚠️ [IA] Mot-clé interdit '${kw}' détecté !`);
                  modStatus = "flagged";
                  break;
                }
              }

              if (result.nudity) {
                if (
                  (result.nudity.sexual_activity || 0) > MODERATION_CONFIG.nuditySexualActivity ||
                  (result.nudity.sexual_display || 0) > MODERATION_CONFIG.nuditySexualDisplay ||
                  (result.nudity.erotica || 0) > MODERATION_CONFIG.nudityErotica ||
                  (result.nudity.very_suggestive || 0) > MODERATION_CONFIG.nudityVerySuggestive
                ) {
                  modStatus = "flagged";
                }
              }

              if (result.gore && (result.gore.prob || 0) > MODERATION_CONFIG.goreProb) modStatus = "flagged";
              if (result.wad) {
                if ((result.wad.weapons || 0) > MODERATION_CONFIG.wadWeapons) modStatus = "flagged";
                if ((result.wad.drugs || 0) > MODERATION_CONFIG.wadDrugs) modStatus = "flagged";
              }
            }
          } catch (seErr) {
            console.error("⚠️ Erreur appel Sightengine :", seErr.message);
          }
        }
      }

      console.log(`🤖 Résultat IA Final pour ${videoId} : ${modStatus}`);

      if (modStatus === "flagged") {
        console.log(`🛑 Vidéo ${videoId} bloquée par la modération.`);
        await videoRef.update({
          moderationStatus: "flagged",
          processingEndedAt: admin.firestore.FieldValue.serverTimestamp()
        });
        return;
      }

      // 5️⃣ COMPRESSION FFMPEG FORCÉE
      console.log(`⚡ [3/5] Compression FFmpeg pour ${videoId}...`);
      await new Promise((resolve, reject) => {
        ffmpeg(localVideoPath)
          .outputOptions([
            "-vf scale='min(360\\,iw)':-2", // 🔥 Virgule échappée \\, indispensable pour FFmpeg
            "-r 24",                       // Plafond 24 FPS
            "-c:v libx264",                // Codec H.264
            "-b:v 300k",                   // Débit vidéo maximal 300 kbps
            "-maxrate 400k",               // Plafond strict
            "-bufsize 600k",               // Tampon VBV
            "-preset medium",              // Compression optimale
            "-c:a aac",                    // Codec audio AAC
            "-b:a 48k",                    // Audio 48 kbps
            "-ac 1",                       // Son Mono
            "-pix_fmt yuv420p",            // Format compatible mobile
            "-movflags +faststart"         // Streaming rapide MP4
          ])
          .save(compressedVideoPath)
          .on("end", resolve)
          .on("error", (err) => {
            console.error("❌ Erreur FFmpeg :", err.message);
            reject(err);
          });
      });

      const stats = await fs.promises.stat(compressedVideoPath);
      if (stats.size === 0) {
        throw new Error("Le fichier vidéo compressé produit est vide");
      }

      // 6️⃣ UPLOAD SUPABASE VIA BUFFER
      console.log(`☁️ [4/5] Upload du fichier compressé sur Supabase (${(stats.size / 1024 / 1024).toFixed(2)} Mo)...`);
      newUploadedFilePath = oldFilePath.replace(/\.[^/.]+$/, "") + `_comp_${Date.now()}.mp4`;

      const compressedBuffer = await fs.promises.readFile(compressedVideoPath);
      const { error: uploadError } = await supabase.storage
        .from("media")
        .upload(newUploadedFilePath, compressedBuffer, {
          contentType: "video/mp4",
          upsert: true
        });

      if (uploadError) {
        throw new Error(`Échec upload Supabase: ${uploadError.message}`);
      }

      const { data: publicUrlData } = supabase.storage
        .from("media")
        .getPublicUrl(newUploadedFilePath);

      const newVideoUrl = publicUrlData.publicUrl;

      // 7️⃣ MISE À JOUR FIRESTORE (videoUrl + mediaUrls)
      console.log(`🔥 [5/5] Enregistrement de la vidéo compressée dans Firestore...`);
      await videoRef.update({
        moderationStatus: "clean",
        videoUrl: newVideoUrl,
        mediaUrls: [newVideoUrl],
        filePath: newUploadedFilePath,
        processingEndedAt: admin.firestore.FieldValue.serverTimestamp()
      });

      // 8️⃣ SUPPRESSION DE L'ANCIENNE VIDÉO BRUTE
      try {
        const { error: removeError } = await supabase.storage
          .from("media")
          .remove([oldFilePath]);

        if (removeError) {
          console.error("⚠️ Erreur suppression ancien fichier Supabase :", removeError.message);
        } else {
          console.log("🗑️ Ancienne vidéo brute supprimée de Supabase.");
        }
      } catch (cleanErr) {
        console.error("⚠️ Erreur lors de la suppression Supabase :", cleanErr.message);
      }

      console.log(`🎉 COMPRESSION ET REMPLACEMENT RÉUSSIS POUR ${videoId}`);

    } catch (err) {
      console.error(`❌ ERREUR OPTIMISATION VIDÉO (${videoId}) :`, err.message);

      if (newUploadedFilePath) {
        try {
          await supabase.storage.from("media").remove([newUploadedFilePath]);
        } catch (rbErr) {
          console.error("Erreur rollback Supabase :", rbErr.message);
        }
      }

      try {
        await videoRef.update({
          moderationStatus: "failed",
          processingError: err.message,
          processingEndedAt: admin.firestore.FieldValue.serverTimestamp()
        });
      } catch (fsErr) {
        console.error("Erreur mise à jour statut 'failed' :", fsErr.message);
      }
    } finally {
      await unlinkSafe(localVideoPath);
      await unlinkSafe(compressedVideoPath);
      for (const fPath of framePaths) {
        await unlinkSafe(fPath);
      }
    }
  })();
});

// ================= ADMIN FINANCE SUMMARY =================

app.get("/api/admin/finance-summary", async (req, res) => {
  try {
    const authHeader = req.headers.authorization || "";

    if (!authHeader.startsWith("Bearer ")) {
      return res.status(401).json({ success: false, error: "Token manquant" });
    }

    const token = authHeader.split("Bearer ")[1];

    const decoded = await admin.auth().verifyIdToken(token);
    const uid = decoded.uid;

    const userDoc = await admin.firestore().collection("users").doc(uid).get();

    if (!userDoc.exists || userDoc.data().role !== "admin") {
      return res.status(403).json({ success: false, error: "Accès refusé" });
    }

    // 🔥 1. LIRE LES STATS GLOBAL (ULTRA RAPIDE)
    const statsDoc = await admin.firestore().collection("adminStats").doc("finance").get();

    let stats = {
      totalIn: 0,
      totalOut: 0,
      netTotal: 0,
      transactionsCount: 0,
      typeTotals: []
    };

    if (statsDoc.exists) {
      stats = statsDoc.data();
    }

    // 🔥 2. LIRE SEULEMENT LES 50 DERNIERES TRANSACTIONS
    const txSnap = await admin.firestore()
      .collection("walletTransactions")


.where("userId", "==", ADMIN_UID)

      .orderBy("createdAt", "desc")
      .limit(50)
      .get();

    const latestTransactions = [];

    txSnap.forEach(doc => {
      const tx = doc.data();

      let direction = "neutral";

      if ([
        "gift_received",
        "merchant_platform_fee",
        "merchant_sale_fee",
        "merchant_renewal_admin_fee",
        "blue_payment_received",
        "merchant_sale_received",
        "merchant_referral_reward",
        "deposit",
        "gift_comission_received"
      ].includes(tx.type)) {
        direction = "in";
      }

      if ([
        "gift_sent",
        "withdraw",
        "merchant_request_payment",
        "merchant_renewal_payment",
        "blue_payment_completed"
      ].includes(tx.type)) {
        direction = "out";
      }

      latestTransactions.push({
        id: doc.id,
        ...tx,
        direction
      });
    });

    return res.json({
      success: true,
      ...stats,
      latestTransactions
    });

  } catch (err) {
    console.error("ADMIN FINANCE ERROR:", err);
    return res.status(500).json({
      success: false,
      error: "Erreur serveur"
    });
  }
});

// SERVER
app.get("/", (req, res) => {
  res.send("HaytiClips backend OK");
});


(() => {
  const { C, Staged } = merchantOrdersV2;
  const auth = verifyFirebaseToken;

  const stamp = () =>
    admin.firestore.FieldValue.serverTimestamp();

  const ref = (collection, id) =>
    db.collection(collection).doc(id);

  const run = fn =>
    db.runTransaction(async real => {
      const t = new Staged(real);
      const result = await fn(t);
      t.flush();
      return result;
    });

  const wrap = fn => async (req, res) => {
    try {
      await fn(req, res);
    } catch (e) {
      console.error(
        "WALLET:",
        e.status ? e.code || e.message : "Internal wallet failure"
      );

      if (!res.headersSent) {
        res.status(e.status || 500).json({
          success: false,
          error: e.status
            ? e.message
            : "Erreur de traitement. Vérifie le statut avant de réessayer.",
          code: e.code || "WALLET_ERROR"
        });
      }
    }
  };

  async function user(t, uid) {
    const snap = await t.get(ref("users", uid));
    C.fail(snap.exists, "Utilisateur introuvable", 404);
    return snap.data();
  }

  function enabled(data, kind) {
    const blockedField =
      kind === "withdraw" ? "withdrawBlocked" : "depositBlocked";

    C.fail(
      data.suspended !== true &&
      data.frozenWallet !== true &&
      data[blockedField] !== true,
      "Opération bloquée sur ce compte",
      403
    );
  }

  async function pin(t, uid, data, value) {
    C.fail(/^\d{6}$/.test(value || ""), "PIN invalide");

    const privateRef = ref("userPrivate", uid);
    const snap = await t.get(privateRef);
    const privateData = snap.exists ? snap.data() : {};

    const hash = privateData.pinHash || data.security?.pinHash;
    C.fail(hash, "PIN non configuré");

    C.fail(
      Number(privateData.pinLockUntilMs || 0) <= Date.now(),
      "Trop de tentatives. Réessaie après 15 minutes.",
      429
    );

    if (!await bcrypt.compare(value, hash)) {
      const count =
        Number(privateData.pinFailedAttempts || 0) + 1;

      t.set(privateRef, {
        pinFailedAttempts: count % 5,
        pinLockUntilMs:
          count >= 5 ? Date.now() + 15 * 60000 : 0
      }, { merge: true });

      return false;
    }

    t.set(privateRef, {
      pinFailedAttempts: 0,
      pinLockUntilMs: 0
    }, { merge: true });

    return true;
  }

  function dailyLimit(data, uid, amount) {
    const today = C.day();

    const before = data.withdrawResetAt === today
      ? C.minor(Number(data.withdrawDailyTotal || 0))
      : 0;

    const maximum = uid === ADMIN_UID ? 8000000 : 2000000;

    C.fail(
      before + amount <= maximum,
      "Limite journalière de retrait atteinte",
      409
    );

    return {
      withdrawDailyTotal: C.htg(before + amount),
      withdrawResetAt: today
    };
  }

  function journal(
    t, id, uid, type, amount, wallet, locked, extra = {}
  ) {
    t.create(ref("walletTransactions", id), {
      userId: uid,
      type,
      amount: C.htg(amount),
      status: "completed",
      accountingVersion: 2,
      scope: "wallet",
      balanceAfter: C.htg(wallet),
      walletLockedAfter: C.htg(locked),
      ...extra,
      createdAt: stamp()
    });
  }

  async function finalizeAutomatic(txId, provider) {
    return run(async t => {
      const txRef = ref("walletTransactions", txId);
      const snap = await t.get(txRef);

      C.fail(snap.exists, "Retrait introuvable", 404);
      const tx = snap.data();

      if (tx.status === "completed") {
        return { success: true, txId, status: "completed" };
      }

      C.fail(
        tx.type === "withdraw" &&
        tx.status === "pending" &&
        tx.reservationSource === "server-v2",
        "Retrait ancien à rapprocher manuellement",
        409
      );

      const data = await user(t, tx.userId);
      const money = C.money(data);
      const amount = C.minor(tx.amount, "Montant", false);

      C.fail(
        money.locked >= amount && money.wallet >= amount,
        "Réservation de retrait incohérente",
        409
      );

      const statsRef = ref("adminStats", "finance");
      const statsSnap = await t.get(statsRef);
      const stats = statsSnap.exists ? statsSnap.data() : {};
      const previous = stats.typeTotals?.withdraw || {};

      const totalIn = C.minor(Number(stats.totalIn || 0));
      const totalOut =
        C.minor(Number(stats.totalOut || 0)) + amount;

      t.set(statsRef, {
        totalIn: C.htg(totalIn),
        totalOut: C.htg(totalOut),
        netTotal: C.htg(totalIn - totalOut),
        transactionsCount:
          Number(stats.transactionsCount || 0) + 1,
        typeTotals: {
          ...(stats.typeTotals || {}),
          withdraw: {
            in: Number(previous.in || 0),
            out: C.htg(
              C.minor(Number(previous.out || 0)) + amount
            ),
            count: Number(previous.count || 0) + 1
          }
        }
      }, { merge: true });

      t.update(ref("users", tx.userId), {
        wallet: C.htg(money.wallet - amount),
        walletLocked: C.htg(money.locked - amount),
        pendingAutomaticWithdrawId: null,
        lastWithdrawRequest: Date.now()
      });

      t.update(txRef, {
        status: "completed",
        providerState: "confirmed",
        completedAt: stamp(),
        providerTransactionId:
          provider?.transfer?.transaction_id || null,
        balanceAfter: C.htg(money.wallet - amount),
        walletLockedAfter: C.htg(money.locked - amount),
        walletDelta: -C.htg(amount),
        lockedDelta: -C.htg(amount)
      });

      return { success: true, txId, status: "completed" };
    });
  }

  async function reconcile(txId) {
    const snap = await ref("walletTransactions", txId).get();
    C.fail(snap.exists, "Retrait introuvable", 404);

    const tx = snap.data();

    if (tx.status === "completed") {
      return { success: true, txId, status: "completed" };
    }

    C.fail(
      tx.type === "withdraw" && tx.status === "pending",
      "Ce retrait ne nécessite pas de vérification automatique",
      409
    );

    let result;

    try {
      result = await moncashPrefundedTransactionStatus(
        tx.providerReference
      );
    } catch {
      // Une réponse inconnue ne débloque pas les fonds.
    }

    if (result?.transStatus === "successful") {
      return finalizeAutomatic(txId, result);
    }

    await ref("walletTransactions", txId).update({
      providerState: "reconciliation_required",
      checkedAt: stamp()
    });

    return {
      success: true,
      txId,
      status: "pending",
      pending: true,
      message:
        "Vérification MonCash en cours. Les fonds restent réservés. Aucun second envoi n’a été effectué."
    };
  }

  app.post("/api/requestWithdraw", auth, wrap(async (req, res) => {
    const uid = req.user.uid;
    const amount = C.minor(req.body.amount, "Montant", false);

    C.fail(
      Number.isInteger(req.body.amount) &&
      [250, 500, 1000, 2000, 5000, 10000, 20000, 80000]
        .includes(req.body.amount),
      "Montant non autorisé"
    );

    const txRef = db.collection("walletTransactions").doc();
    const reference = `WDR_${crypto.randomUUID()}`;

    const result = await run(async t => {
      const data = await user(t, uid);
      enabled(data, "withdraw");

      if (!await pin(t, uid, data, req.body.pin)) {
        return { pinError: true };
      }

      C.fail(
        /^509\d{8}$/.test(data.withdrawNumber || ""),
        "Numéro de retrait non enregistré"
      );

      if (data.pendingAutomaticWithdrawId) {
        const previous = await t.get(
          ref("walletTransactions", data.pendingAutomaticWithdrawId)
        );

        if (previous.exists && previous.data().status === "pending") {
          C.fail(
            previous.data().amount === req.body.amount,
            "Un autre retrait est déjà en cours",
            409
          );

          return { txId: previous.id, created: false };
        }
      }

      const oldPending = await t.get(
        db.collection("walletTransactions")
          .where("userId", "==", uid)
          .where("type", "==", "withdraw")
          .where("status", "==", "pending")
          .limit(1)
      );

      C.fail(
        oldPending.empty,
        "Un retrait existant doit être rapproché avant une nouvelle demande",
        409
      );

      const money = C.money(data);

      C.fail(
        money.available >= amount,
        "Solde disponible insuffisant",
        409
      );

      const daily = dailyLimit(data, uid, amount);

      t.update(ref("users", uid), {
        walletLocked: C.htg(money.locked + amount),
        pendingAutomaticWithdrawId: txRef.id,
        ...daily
      });

      t.create(txRef, {
        userId: uid,
        amount: C.htg(amount),
        number: data.withdrawNumber,
        method: "moncash",
        type: "withdraw",
        status: "pending",
        reservationSource: "server-v2",
        providerState: "reserved",
        providerReference: reference,
        reservedDay: daily.withdrawResetAt,
        accountingVersion: 2,
        scope: "wallet",
        walletDelta: 0,
        lockedDelta: 0,
        createdAt: stamp()
      });

      journal(
        t, `hold_${txRef.id}`, uid, "withdraw_reserved",
        amount, money.wallet, money.locked + amount,
        {
          walletDelta: 0,
          lockedDelta: C.htg(amount),
          withdrawTxId: txRef.id
        }
      );

      return {
        txId: txRef.id,
        created: true,
        number: data.withdrawNumber
      };
    });

    if (result.pinError) {
      return res.status(400).json({
        success: false,
        error: "PIN incorrect"
      });
    }

    if (!result.created) {
      return res.json(await reconcile(result.txId));
    }

    let payout;

    try {
      payout = await moncashTransfer(
        C.htg(amount),
        result.number,
        `Retrait HaytiClips ${uid}`,
        reference
      );
    } catch {
      // Ne jamais répéter un transfert dont la réponse est incertaine.
    }

    if (payout?.transfer?.message === "successful") {
      return res.json(
        await finalizeAutomatic(result.txId, payout)
      );
    }

    const state = await reconcile(result.txId);
    res.status(state.pending ? 202 : 200).json(state);
  }));

  app.get("/api/withdraw/status/:txId", auth, wrap(async (req, res) => {
    const txId = C.id(req.params.txId);
    const snap = await ref("walletTransactions", txId).get();

    C.fail(
      snap.exists && snap.data().type === "withdraw",
      "Retrait introuvable",
      404
    );

    if (snap.data().userId !== req.user.uid) {
      const staff = await ref("users", req.user.uid).get();
      C.staff(staff.exists ? staff.data() : null, true);
    }

    res.json(await reconcile(txId));
  }));

  app.get("/api/admin/wallet-reviews", auth, wrap(async (req, res) => {
    const staff = await ref("users", req.user.uid).get();
    C.staff(staff.exists ? staff.data() : null, true);

    const [manual, automatic] = await Promise.all([
      db.collection("manualTransfers")
        .where("status", "==", "pending")
        .limit(301)
        .get(),
      db.collection("walletTransactions")
        .where("type", "==", "withdraw")
        .where("status", "==", "pending")
        .limit(301)
        .get()
    ]);

    C.fail(
      manual.size <= 300 && automatic.size <= 300,
      "File de vérification trop importante : lecture paginée nécessaire",
      413
    );

    res.json({
      success: true,
      manual: manual.docs.map(s => {
        const x = s.data();

        return {
          id: s.id,
          userId: x.userId,
          type: x.type,
          amount: x.amount ?? x.amountRequested,
          amountToReceive: x.amountToReceive ?? null,
          feeApplied: x.feeApplied ?? null,
          phone: x.phone || "",
          receiptUrl: x.receiptUrl || "",
          legacy: x.source !== "server-v2",
          legacyReviewed: !!x.legacyReviewedBy,
          atMs: C.asMs(x.createdAt)
        };
      }),
      automatic: automatic.docs.map(s => {
        const x = s.data();

        return {
          id: s.id,
          userId: x.userId,
          amount: x.amount,
          number: x.number,
          legacy: x.reservationSource !== "server-v2",
          state: x.providerState || "unknown",
          atMs: C.asMs(x.createdAt)
        };
      })
    });
  }));

  app.post("/api/admin/resolve-withdraw", auth, wrap(async (req, res) => {
    const txId = C.id(req.body.txId);
    const reason = C.text(
      req.body.reason,
      "Preuve de non-paiement vérifiée",
      2000,
      15
    );

    C.fail(
      req.body.confirmedNotPaid === true,
      "Vérifie le non-paiement dans MonCash avant de débloquer"
    );

    const staff = await ref("users", req.user.uid).get();
    C.staff(staff.exists ? staff.data() : null, true);

    const top = await ref("walletTransactions", txId).get();
    C.fail(top.exists, "Retrait introuvable", 404);

    let provider;

    try {
      provider = await moncashPrefundedTransactionStatus(
        top.data().providerReference
      );
    } catch {
      // Le contrôle manuel documenté reste obligatoire.
    }

    C.fail(
      provider?.transStatus !== "successful",
      "MonCash confirme le paiement : rapproche le retrait au lieu de débloquer",
      409
    );

    await run(async t => {
      C.staff(await user(t, req.user.uid), true);

      const snap = await t.get(ref("walletTransactions", txId));
      C.fail(snap.exists, "Retrait introuvable", 404);

      const tx = snap.data();

      C.fail(
        tx.type === "withdraw" &&
        tx.status === "pending" &&
        tx.reservationSource === "server-v2",
        "Retrait non déblocable par cette route",
        409
      );

      const data = await user(t, tx.userId);
      const money = C.money(data);
      const amount = C.minor(tx.amount);

      C.fail(
        money.locked >= amount,
        "Réservation insuffisante",
        409
      );

      const fields = {
        walletLocked: C.htg(money.locked - amount),
        pendingAutomaticWithdrawId: null
      };

      if (data.withdrawResetAt === tx.reservedDay) {
        fields.withdrawDailyTotal = C.htg(Math.max(
          0,
          C.minor(Number(data.withdrawDailyTotal || 0)) - amount
        ));
      }

      t.update(ref("users", tx.userId), fields);

      t.update(snap.ref, {
        status: "failed",
        failReason: reason,
        failedAt: stamp(),
        resolvedBy: req.user.uid
      });

      journal(
        t, `unlock_${txId}`, tx.userId,
        "withdraw_unlocked", amount,
        money.wallet, money.locked - amount,
        {
          walletDelta: 0,
          lockedDelta: -C.htg(amount),
          withdrawTxId: txId
        }
      );

      t.create(db.collection("merchantAuditLogs").doc(), {
        actorUid: req.user.uid,
        action: "withdraw-unlocked-after-review",
        target: txId,
        details: { reason },
        createdAt: stamp()
      });
    });

    res.json({ success: true });
  }));

  app.post("/set-pin", auth, wrap(async (req, res) => {
    C.fail(/^\d{6}$/.test(req.body.pin || ""), "PIN invalide");

    const hash = await bcrypt.hash(req.body.pin, 10);

    await run(async t => {
      const data = await user(t, req.user.uid);
      C.fail(data.suspended !== true, "Compte suspendu", 403);

      t.set(ref("userPrivate", req.user.uid), {
        pinHash: hash,
        pinFailedAttempts: 0,
        pinLockUntilMs: 0,
        pinUpdatedAt: stamp()
      }, { merge: true });

      t.update(ref("users", req.user.uid), {
        "security.enabled": true,
        "security.pinHash": admin.firestore.FieldValue.delete(),
        "security.createdAt": stamp()
      });
    });

    res.json({ success: true });
  }));

  app.post("/check-pin", auth, wrap(async (req, res) => {
    const valid = await run(async t => {
      const data = await user(t, req.user.uid);
      C.fail(data.suspended !== true, "Compte suspendu", 403);

      const ok = await pin(t, req.user.uid, data, req.body.pin);

      if (ok) {
        t.update(ref("users", req.user.uid), {
          "security.lastVerified": stamp()
        });
      }

      return ok;
    });

    res.json({ success: valid });
  }));

  app.post("/remove-pin", auth, wrap(async (req, res) => {
    const valid = await run(async t => {
      const data = await user(t, req.user.uid);
      C.fail(data.suspended !== true, "Compte suspendu", 403);

      if (!await pin(t, req.user.uid, data, req.body.pin)) {
        return false;
      }

      t.set(ref("userPrivate", req.user.uid), {
        pinHash: admin.firestore.FieldValue.delete(),
        pinFailedAttempts: 0,
        pinLockUntilMs: 0
      }, { merge: true });

      t.update(ref("users", req.user.uid), {
        "security.enabled": false,
        "security.pinHash": admin.firestore.FieldValue.delete(),
        "security.lastVerified": stamp()
      });

      return true;
    });

    if (!valid) {
      return res.status(400).json({
        success: false,
        error: "PIN incorrect"
      });
    }

    res.json({ success: true });
  }));

  app.post("/api/manual-transfer", auth, (req, res) => {
    uploadMemory.single("receipt")(req, res, err => {
      if (err) {
        return res.status(400).json({
          success: false,
          error: "Capture invalide ou trop volumineuse"
        });
      }

      wrap(async (req, res) => {
        const uid = req.user.uid;
        const body = req.body;

        C.fail(
          ["deposit", "withdraw"].includes(body.type),
          "Type invalide"
        );

        const method = C.text(
          body.method, "Méthode de transfert", 60, 1
        );

        const phone = C.text(body.phone, "Téléphone", 30, 8);

        const amount = C.minor(
          Number(
            body.type === "withdraw"
              ? body.amountRequested
              : body.amount
          ),
          "Montant",
          false
        );

        if (body.type === "withdraw") {
          const fee = C.minor(
            Number(body.feeApplied), "Frais estimés"
          );

          const receive = C.minor(
            Number(body.amountToReceive),
            "Montant à recevoir",
            false
          );

          C.fail(
            fee + receive === amount,
            "Montant et frais incohérents"
          );

          const transferRef =
            db.collection("manualTransfers").doc();

          const result = await run(async t => {
            const data = await user(t, uid);
            enabled(data, "withdraw");

            if (!await pin(t, uid, data, body.pin)) {
              return { pinError: true };
            }

            if (data.pendingManualWithdrawId) {
              const previous = await t.get(
                ref("manualTransfers", data.pendingManualWithdrawId)
              );

              if (
                previous.exists &&
                previous.data().status === "pending"
              ) {
                return {
                  success: true,
                  transferId: previous.id,
                  alreadyPending: true
                };
              }
            }

            const pending = await t.get(
              db.collection("manualTransfers")
                .where("userId", "==", uid)
                .where("type", "==", "withdraw")
                .where("status", "==", "pending")
                .limit(1)
            );

            C.fail(
              pending.empty,
              "Un retrait manuel est déjà en attente",
              409
            );

            const money = C.money(data);

            C.fail(
              money.available >= amount,
              "Solde disponible insuffisant",
              409
            );

            const daily = dailyLimit(data, uid, amount);

            t.update(ref("users", uid), {
              walletLocked: C.htg(money.locked + amount),
              pendingManualWithdrawId: transferRef.id,
              ...daily
            });

            t.create(transferRef, {
              source: "server-v2",
              userId: uid,
              type: "withdraw",
              method,
              amountRequested: C.htg(amount),
              feeApplied: C.htg(fee),
              amountToReceive: C.htg(receive),
              feeIsEstimate: true,
              phone,
              accountName: C.text(
                body.accountName || "",
                "Nom du bénéficiaire",
                150
              ),
              status: "pending",
              reservedDay: daily.withdrawResetAt,
              createdAt: stamp()
            });

            journal(
              t, `manual_hold_${transferRef.id}`, uid,
              "manual_withdraw_pending", amount,
              money.wallet, money.locked + amount,
              {
                transferId: transferRef.id,
                walletDelta: 0,
                lockedDelta: C.htg(amount)
              }
            );

            return {
              success: true,
              transferId: transferRef.id
            };
          });

          if (result.pinError) {
            return res.status(400).json({
              success: false,
              error: "PIN incorrect"
            });
          }

          return res.json(result);
        }

        C.fail(
          req.file?.buffer?.length > 0,
          "Capture d’écran obligatoire"
        );

        C.fail(
          ["image/jpeg", "image/png", "image/webp"]
            .includes(req.file.mimetype),
          "Capture JPG, PNG ou WebP requise"
        );

        const fileHash = crypto.createHash("sha256")
          .update(req.file.buffer)
          .digest("hex");

        const transferRef =
          db.collection("manualTransfers").doc();

        const fileName =
          `receipts/${uid}/${crypto.randomUUID()}`;

        const top = await ref("users", uid).get();
        C.fail(top.exists, "Utilisateur introuvable", 404);
        enabled(top.data(), "deposit");

        const uploaded = await supabase.storage
          .from("media")
          .upload(fileName, req.file.buffer, {
            contentType: req.file.mimetype,
            upsert: false
          });

        C.fail(
          !uploaded.error,
          "Envoi de capture impossible",
          503
        );

        try {
          await run(async t => {
            const data = await user(t, uid);
            enabled(data, "deposit");

            const claimRef =
              ref("manualReceiptClaims", fileHash);

            const claim = await t.get(claimRef);

            C.fail(
              !claim.exists,
              "Cette capture a déjà été soumise",
              409
            );

            t.create(claimRef, {
              transferId: transferRef.id,
              uid,
              createdAt: stamp()
            });

            t.create(transferRef, {
              source: "server-v2",
              userId: uid,
              type: "deposit",
              method,
              amount: C.htg(amount),
              phone,
              receiptFileName: fileName,
              receiptUrl: supabase.storage
                .from("media")
                .getPublicUrl(fileName).data.publicUrl,
              status: "pending",
              createdAt: stamp()
            });
          });
        } catch (e) {
          await supabase.storage.from("media").remove([fileName]);
          throw e;
        }

        res.json({
          success: true,
          transferId: transferRef.id
        });
      })(req, res);
    });
  });

  app.post(
    "/api/admin/review-legacy-manual-transfer",
    auth,
    wrap(async (req, res) => {
      const id = C.id(req.body.transferId);

      const reason = C.text(
        req.body.reason,
        "Vérification de l’ancienne demande",
        2000,
        15
      );

      await run(async t => {
        C.staff(await user(t, req.user.uid), true);

        const snap = await t.get(ref("manualTransfers", id));

        C.fail(
          snap.exists && snap.data().status === "pending",
          "Demande introuvable ou déjà traitée",
          409
        );

        t.update(snap.ref, {
          legacyReviewedBy: req.user.uid,
          legacyReviewReason: reason,
          legacyReviewedAt: stamp()
        });

        t.create(db.collection("merchantAuditLogs").doc(), {
          actorUid: req.user.uid,
          action: "legacy-manual-transfer-reviewed",
          target: id,
          details: { reason },
          createdAt: stamp()
        });
      });

      res.json({ success: true });
    })
  );

  app.post(
    "/api/admin/process-manual-transfer",
    auth,
    wrap(async (req, res) => {
      const transferId = C.id(req.body.transferId);
      const status = req.body.status;

      C.fail(
        ["approved", "rejected"].includes(status),
        "Statut invalide"
      );

      const reason = status === "rejected"
        ? C.text(req.body.reason, "Motif de refus", 1000, 5)
        : "";

      let receipt = null;

      await run(async t => {
        C.staff(await user(t, req.user.uid), true);

        const snap = await t.get(
          ref("manualTransfers", transferId)
        );

        C.fail(snap.exists, "Transfert introuvable", 404);
        const transfer = snap.data();

        C.fail(
          transfer.status === "pending",
          "Transfert déjà traité",
          409
        );

        C.fail(
          transfer.source === "server-v2" ||
          transfer.legacyReviewedBy,
          "Cette ancienne demande doit être vérifiée avant traitement",
          409,
          "LEGACY_REVIEW_REQUIRED"
        );

        C.fail(
          ["deposit", "withdraw"].includes(transfer.type),
          "Type invalide"
        );

        const amount = C.minor(
          Number(
            transfer.type === "withdraw"
              ? transfer.amountRequested
              : transfer.amount
          ),
          "Montant",
          false
        );

        const data = await user(t, transfer.userId);
        const money = C.money(data);

        let wallet = money.wallet;
        let locked = money.locked;
        let walletDelta = 0;
        let lockedDelta = 0;

        if (transfer.type === "withdraw") {
          C.fail(
            locked >= amount,
            "Réservation insuffisante",
            409
          );

          locked -= amount;
          lockedDelta = -amount;

          if (status === "approved") {
            C.fail(wallet >= amount, "Solde insuffisant", 409);
            wallet -= amount;
            walletDelta = -amount;
          }

          const fields = {
            wallet: C.htg(wallet),
            walletLocked: C.htg(locked),
            pendingManualWithdrawId: null
          };

          if (
            status === "rejected" &&
            transfer.source === "server-v2" &&
            data.withdrawResetAt === transfer.reservedDay
          ) {
            fields.withdrawDailyTotal = C.htg(Math.max(
              0,
              C.minor(Number(data.withdrawDailyTotal || 0)) - amount
            ));
          }

          t.update(ref("users", transfer.userId), fields);
        } else if (status === "approved") {
          wallet += amount;
          walletDelta = amount;

          t.update(ref("users", transfer.userId), {
            wallet: C.htg(wallet)
          });
        }

        journal(
          t, `manual_done_${transferId}`, transfer.userId,
          `manual_${transfer.type}_${status}`,
          amount, wallet, locked,
          {
            transferId,
            walletDelta: C.htg(walletDelta),
            lockedDelta: C.htg(lockedDelta)
          }
        );

        t.update(snap.ref, {
          status,
          processedAt: stamp(),
          processedBy: req.user.uid,
          rejectReason: reason
        });

        if (status === "rejected") {
          t.create(db.collection("notifications").doc(), {
            to: transfer.userId,
            from: req.user.uid,
            type: "transfer_rejected",
            message: `Demande refusée : ${reason}`,
            read: false,
            createdAt: stamp()
          });
        }

        const name = transfer.receiptFileName;

        receipt =
          transfer.type === "deposit" &&
          typeof name === "string" &&
          (
            name.startsWith(`receipts/${transfer.userId}/`) ||
            name.startsWith(`${transfer.userId}_`)
          )
            ? name
            : null;
      });

      if (receipt) {
        try {
          await supabase.storage.from("media").remove([receipt]);
        } catch {
          // Le nettoyage ne modifie pas le résultat comptable.
        }
      }

      res.json({ success: true });
    })
  );

  app.post(
    "/api/merchant/admin/migrate-pin-hashes",
    auth,
    wrap(async (req, res) => {
      const staff = await ref("users", req.user.uid).get();
      C.staff(staff.exists ? staff.data() : null, true);

      const apply = req.body.apply === true;

      if (apply) {
        C.fail(
          process.env.MERCHANT_CHECKOUT_PAUSED === "true",
          "Active la maintenance avant la migration",
          409
        );

        C.fail(
          req.body.confirm === "MIGRATE_PRIVATE_PINS",
          "Confirmation de migration manquante"
        );
      }

      const found = await db.collection("users")
        .where("security.pinHash", ">=", "")
        .limit(201)
        .get();

      if (!apply) {
        return res.json({
          success: true,
          preview: true,
          found: found.size,
          more: found.size > 200
        });
      }

      let migrated = 0;

      for (const row of found.docs.slice(0, 200)) {
        const changed = await run(async t => {
          C.staff(await user(t, req.user.uid), true);

          const data = await user(t, row.id);
          const privateRef = ref("userPrivate", row.id);
          const previous = await t.get(privateRef);

          if (typeof data.security?.pinHash !== "string") {
            return false;
          }

          if (!previous.exists || !previous.data().pinHash) {
            t.set(privateRef, {
              pinHash: data.security.pinHash,
              pinFailedAttempts: 0,
              pinLockUntilMs: 0
            }, { merge: true });
          }

          t.update(ref("users", row.id), {
            "security.pinHash":
              admin.firestore.FieldValue.delete()
          });

          return true;
        });

        if (changed) migrated++;
      }

      res.json({
        success: true,
        migrated,
        more: found.size > 200
      });
    })
  );
})();



const merchantReportsV2 = (() => {
  const { C } = merchantOrdersV2;
  const { spawn } = require("node:child_process");

  const IN = new Set([
    "deposit",
    "manual_deposit_approved",
    "gift_received",
    "gift_commission_received",
    "merchant_sale_received",
    "merchant_sale_fee",
    "merchant_referral_reward",
    "merchant_platform_fee",
    "merchant_renewal_referral_reward",
    "merchant_renewal_admin_fee",
    "merchant_request_refund",
    "merchant_order_refund"
  ]);

  const OUT = new Set([
    "gift_sent",
    "withdraw",
    "manual_withdraw_approved",
    "merchant_request_payment",
    "merchant_renewal_payment",
    "blue_payment_completed",
    "merchant_order_created"
  ]);

  const LABELS = {
    merchant_order_created: "Réservation d’achat",
    merchant_order_paid: "Achat réglé",
    merchant_order_unlocked: "Réservation annulée",
    merchant_sale_reserved: "Vente en attente",
    merchant_sale_received: "Vente réglée (net)",
    merchant_sale_cancelled: "Vente annulée",
    merchant_sale_fee: "Commission de vente",
    merchant_sale_fee_refunded: "Commission remboursée",
    merchant_sale_refunded: "Remboursement de vente",
    merchant_platform_refund: "Remboursement financé par la plateforme",
    merchant_order_refund: "Remboursement reçu",
    merchant_legacy_reservation_adjustment:
      "Correction d’ancienne réservation",
    merchant_request_refund: "Ancien accès remboursé",
    withdraw: "Retrait MonCash",
    withdraw_reserved: "Réservation pour retrait",
    withdraw_unlocked: "Réservation de retrait annulée",
    manual_withdraw_pending: "Retrait manuel réservé",
    manual_withdraw_approved: "Retrait manuel",
    manual_withdraw_rejected: "Retrait manuel refusé",
    manual_deposit_approved: "Recharge manuelle",
    manual_deposit_rejected: "Recharge manuelle refusée",
    deposit: "Recharge MonCash",
    gift_sent: "Cadeau envoyé",
    gift_received: "Cadeau reçu",
    gift_commission_received: "Commission cadeau"
  };

  function entry(id, row) {
    let walletDelta = null;
    let lockedDelta = null;

    if (
      row.accountingVersion === 2 &&
      typeof row.walletDelta === "number"
    ) {
      walletDelta = row.walletDelta;
      lockedDelta = row.lockedDelta || 0;
    } else if (row.status === "completed") {
      if (IN.has(row.type)) {
        walletDelta = Number(row.amount || 0);
      } else if (OUT.has(row.type)) {
        walletDelta = -Number(row.amount || 0);
      } else if ([
        "merchant_order_paid",
        "merchant_sale_pending",
        "merchant_sale_rejected"
      ].includes(row.type)) {
        walletDelta = 0;
      }
    }

    return {
      id,
      type: row.type,
      description: LABELS[row.type] || row.type,
      amount: Number(row.amount || 0),
      status: row.status || "unknown",
      walletDelta,
      lockedDelta,
      heldDelta: Number.isFinite(row.heldDelta)
        ? row.heldDelta
        : null,
      balanceAfter: Number.isFinite(row.balanceAfter)
        ? row.balanceAfter
        : null,
      walletLockedAfter: Number.isFinite(row.walletLockedAfter)
        ? row.walletLockedAfter
        : null,
      orderId: row.orderId || null,
      atMs: C.asMs(row.completedAt || row.createdAt),
      scope: row.scope || "wallet",
      correction:
        row.type === "merchant_legacy_reservation_adjustment",
      legacy: row.accountingVersion !== 2
    };
  }

  function compute(
    user, orders, rows, products, selectedRange, now = Date.now()
  ) {
    const orderMap = new Map(orders.map(o => [o.id, o]));
    const salesByOrder = new Map();

    for (const row of rows) {
      if (
        row.type !== "merchant_sale_received" ||
        row.status !== "completed"
      ) continue;

      const key = row.orderId || `unlinked:${row.id}`;

      C.fail(
        !salesByOrder.has(key),
        "Règlements en double à rapprocher avant de produire les statistiques",
        409,
        "RECONCILIATION_REQUIRED"
      );

      const order = orderMap.get(row.orderId) || {};
      const net = C.minor(Number(row.amount || 0));
      const fee = C.minor(Number(row.feeAmount || 0));

      const gross = C.minor(Number(
        row.grossAmount ??
        order.totalAmount ??
        C.htg(net + fee)
      ));

      C.fail(
        net + fee === gross,
        "Ancien règlement incohérent à rapprocher",
        409,
        "RECONCILIATION_REQUIRED"
      );

      salesByOrder.set(key, {
        orderId: row.orderId,
        productId: order.productId || row.productId || "unknown",
        productName:
          order.productName || row.productName || "Ancien produit",
        quantity: Number(order.quantity || row.quantity || 0),
        gross,
        fee,
        net,
        at: row.createdAt
      });
    }

    const sales = [...salesByOrder.values()];

    const aggregate = range => {
      const chosen = sales.filter(s => C.inRange(s.at, range));

      return {
        count: chosen.length,
        gross: C.htg(chosen.reduce((n, s) => n + s.gross, 0)),
        fee: C.htg(chosen.reduce((n, s) => n + s.fee, 0)),
        net: C.htg(chosen.reduce((n, s) => n + s.net, 0)),
        units: chosen.reduce((n, s) => n + s.quantity, 0)
      };
    };

    const chosen = sales.filter(
      s => C.inRange(s.at, selectedRange)
    );

    const daily = new Map();
    const best = new Map();

    for (const sale of chosen) {
      const date = C.day(C.asMs(sale.at));

      const day = daily.get(date) || {
        date,
        grossMinor: 0,
        netMinor: 0,
        count: 0
      };

      day.grossMinor += sale.gross;
      day.netMinor += sale.net;
      day.count++;
      daily.set(date, day);

      const product = best.get(sale.productId) || {
        name: sale.productName,
        quantity: 0,
        grossMinor: 0
      };

      product.quantity += sale.quantity;
      product.grossMinor += sale.gross;
      best.set(sale.productId, product);
    }

    const warnings = [];

    if (orders.some(
      o => !o.accountingVersion && o.status === "pending"
    )) {
      warnings.push(
        "Des anciennes réservations doivent être migrées. Leur montant n’est pas inclus dans les fonds sécurisés v2."
      );
    }

    if (orders.some(o =>
      o.status === "completed" &&
      !salesByOrder.has(o.id) &&
      o.fundsStatus !== "refunded"
    )) {
      warnings.push(
        "Au moins une ancienne commande indiquée terminée ne possède pas de règlement vérifiable."
      );
    }

    const money = C.money(user, true);

    if (money.locked > money.wallet) {
      warnings.push(
        "Le wallet contient des réservations anciennes incohérentes. Un rapprochement est nécessaire."
      );
    }

    const transactions = rows
      .map(row => entry(row.id, row))
      .filter(row =>
        row.atMs && C.inRange(row.atMs, selectedRange)
      )
      .sort((a, b) =>
        a.atMs - b.atMs || a.id.localeCompare(b.id)
      );

    const pending = orders.filter(o =>
      o.accountingVersion === 2 && o.fundsStatus === "held"
    );

    const refunded = orders.filter(o =>
      o.fundsStatus === "refunded" &&
      C.inRange(o.refundedAt, selectedRange)
    );

    const withdrawals = transactions.filter(tx =>
      ["withdraw", "manual_withdraw_approved"].includes(tx.type) &&
      tx.status === "completed"
    );

    const refundsNet = transactions.filter(tx =>
      tx.type === "merchant_sale_refunded" &&
      tx.status === "completed"
    );

    const counts = {};

    for (const order of orders) {
      const status = order.status || "unknown";
      counts[status] = (counts[status] || 0) + 1;
    }

    return {
      success: true,
      range: selectedRange,
      generatedAtMs: now,

      shop: {
        name: user.merchantShopName || "Boutique",
        logo: user.merchantLogo || "",
        email: user.merchantEmail || user.email || ""
      },

      period: {
        ...aggregate(selectedRange),
        ordersCreated: orders.filter(
          o => C.inRange(o.createdAt, selectedRange)
        ).length,
        refundsGross: C.htg(refunded.reduce(
          (n, o) => n + C.minor(o.totalAmount), 0
        )),
        refundsNet: C.htg(refundsNet.reduce(
          (n, tx) => n + C.minor(tx.amount), 0
        )),
        withdrawalsGlobalWallet: C.htg(withdrawals.reduce(
          (n, tx) => n + C.minor(tx.amount), 0
        ))
      },

      periods: {
        today: aggregate(C.range({ period: "today" }, now)),
        week: aggregate(C.range({ period: "7" }, now)),
        month: aggregate(C.range({ period: "month" }, now))
      },

      snapshot: {
        wallet: C.htg(money.wallet),
        walletLocked: C.htg(money.locked),
        walletAvailable: C.htg(money.available),
        merchantPending: C.htg(pending.reduce(
          (n, o) => n + C.minor(o.merchantNet), 0
        )),
        totalOrders: orders.length,
        activeProducts: products.filter(p =>
          p.isActive === true && p.archived !== true
        ).length,
        counts
      },

      daily: [...daily.values()]
        .sort((a, b) => a.date.localeCompare(b.date))
        .map(x => ({
          date: x.date,
          gross: C.htg(x.grossMinor),
          net: C.htg(x.netMinor),
          count: x.count
        })),

      bestProducts: [...best.values()]
        .sort((a, b) => b.quantity - a.quantity)
        .slice(0, 5)
        .map(x => ({
          name: x.name,
          quantity: x.quantity,
          gross: C.htg(x.grossMinor)
        })),

      transactions,
      warnings,

      walletNotice:
        "Le solde et les retraits concernent le portefeuille HaytiClips partagé (ventes, cadeaux et autres opérations). Les fonds en attente sont une situation actuelle, indépendante de la période du rapport. Les corrections comptables ne sont pas des recharges ni du chiffre d’affaires."
    };
  }

  async function summary(options, uid, input) {
    const now = Date.now();
    const selectedRange = C.range(input, now);

    return options.run(async tx => {
      const user = await options.getUser(tx, uid);

      C.fail(
        user.merchantApprovedAt ||
        ["active", "suspended"].includes(user.merchantStatus),
        "Aucune boutique approuvée",
        403
      );

      const [orders, rows, products] = await Promise.all([
        tx.get(
          options.db.collection("merchantOrders")
            .where("merchantUid", "==", uid)
            .limit(options.maxRows + 1)
        ),
        tx.get(
          options.db.collection("walletTransactions")
            .where("userId", "==", uid)
            .limit(options.maxRows + 1)
        ),
        tx.get(
          options.db.collection("merchantProducts")
            .where("merchantUid", "==", uid)
            .limit(options.maxRows + 1)
        )
      ]);

      C.fail(
        [orders, rows, products].every(
          snap => snap.size <= options.maxRows
        ),
        "Volume supérieur à la limite de sécurité du rapport. Ajouter une lecture paginée par période avant de dépasser cette limite.",
        413
      );

      const result = compute(
        user,
        orders.docs.map(s => ({ id: s.id, ...s.data() })),
        rows.docs.map(s => ({ id: s.id, ...s.data() })),
        products.docs.map(s => s.data()),
        selectedRange,
        now
      );

      result.shop.uid = uid;
      return result;
    });
  }

  function webpLogo(buffer) {
    return new Promise((resolve, reject) => {
      const child = spawn(
        process.env.FFMPEG_PATH || "ffmpeg",
        [
          "-hide_banner",
          "-loglevel", "error",
          "-threads", "1",
          "-i", "pipe:0",
          "-frames:v", "1",
          "-vf", "scale=400:400:force_original_aspect_ratio=decrease",
          "-f", "image2pipe",
          "-vcodec", "mjpeg",
          "pipe:1"
        ],
        { timeout: 10000 }
      );

      const chunks = [];
      let length = 0;

      child.on("error", reject);
      child.stdin.on("error", () => {});

      child.stdout.on("data", chunk => {
        length += chunk.length;

        if (length > 5 * 1024 * 1024) {
          child.kill();
        } else {
          chunks.push(chunk);
        }
      });

      child.stderr.resume();

      child.on("close", code => {
        if (code === 0 && length) {
          resolve(Buffer.concat(chunks));
        } else {
          reject(new Error("Logo non convertible"));
        }
      });

      child.stdin.end(buffer);
    });
  }

  async function pdf(data, supabase, res) {
    C.fail(
      !data.warnings.length,
      "Rapproche les anciennes données signalées dans le dashboard avant de générer un rapport financier",
      409,
      "RECONCILIATION_REQUIRED"
    );

    const PDFDocument = require("pdfkit");

    const doc = new PDFDocument({
      size: "A4",
      margin: 42,
      bufferPages: true,
      info: {
        Title: `Rapport ${data.shop.name}`,
        Author: "HaytiClips"
      }
    });

    const chunks = [];

    const finished = new Promise((resolve, reject) => {
      doc.on("data", chunk => chunks.push(chunk));
      doc.on("end", () => resolve(Buffer.concat(chunks)));
      doc.on("error", reject);
    });

    const money = value =>
      new Intl.NumberFormat("fr-HT", {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
      }).format(value).replace(/[\u202f\u00a0]/g, " ") + " HTG";

    const date = value =>
      new Intl.DateTimeFormat("fr-FR", {
        timeZone: C.TIMEZONE,
        dateStyle: "short",
        timeStyle: "short"
      }).format(new Date(value));

    const width = doc.page.width - 84;

    function header() {
      doc.fillColor("#ff0050")
        .font("Helvetica-Bold")
        .fontSize(19)
        .text("HaytiClips | Rapport marchand");

      doc.moveDown(0.4)
        .fillColor("#222222")
        .font("Helvetica")
        .fontSize(9)
        .text(
          `${data.shop.name} - ${data.range.from} au ${data.range.to}`
        );

      doc.moveDown(0.8);
    }

    function room(height) {
      if (doc.y + height > doc.page.height - 65) {
        doc.addPage();
        header();
      }
    }

    function line(label, value) {
      room(25);
      const y = doc.y;

      doc.font("Helvetica")
        .fontSize(10)
        .fillColor("#333333")
        .text(label, 42, y, { width: width * 0.63 });

      doc.font("Helvetica-Bold")
        .text(value, 42 + width * 0.63, y, {
          width: width * 0.37,
          align: "right"
        });

      doc.y = y + 22;
      doc.x = 42;
    }

    header();

    if (data.shop.logo) {
      try {
        const url = new URL(data.shop.logo);
        const base = new URL(process.env.SUPABASE_URL);

        const prefix =
          `/storage/v1/object/public/media/shops/${data.shop.uid}/`;

        if (
          url.origin === base.origin &&
          url.pathname.startsWith(prefix)
        ) {
          const storagePath = decodeURIComponent(
            url.pathname.split("/public/media/")[1]
          );

          const image = await supabase.storage
            .from("media")
            .download(storagePath);

          if (!image.error) {
            let buffer = Buffer.from(
              await image.data.arrayBuffer()
            );

            if (buffer.length <= 15 * 1024 * 1024) {
              if (
                buffer.toString("ascii", 0, 4) === "RIFF" &&
                buffer.toString("ascii", 8, 12) === "WEBP"
              ) {
                buffer = await webpLogo(buffer);
              }

              doc.image(
                buffer,
                doc.page.width - 106,
                doc.y,
                { fit: [64, 64] }
              );
            }
          }
        }
      } catch {
        // Une image illisible ne supprime pas l’identité textuelle.
      }
    }

    doc.font("Helvetica-Bold")
      .fontSize(17)
      .text(data.shop.name, { width: width - 80 });

    doc.font("Helvetica")
      .fontSize(9)
      .text(`Identifiant : ${data.shop.uid}`, {
        width: width - 80
      })
      .text(`Email : ${data.shop.email}`, {
        width: width - 80
      })
      .text(
        `Généré le ${date(data.generatedAtMs)} - heure d’Haïti`,
        { width: width - 80 }
      );

    doc.y = Math.max(doc.y + 20, 180);

    line(
      "Chiffre d’affaires brut réglé sur la période",
      money(data.period.gross)
    );
    line(
      "Nombre de ventes réglées / unités",
      `${data.period.count} / ${data.period.units}`
    );
    line(
      "Frais des ventes réglées sur la période",
      money(data.period.fee)
    );
    line(
      "Fonds libérés au marchand (net)",
      money(data.period.net)
    );
    line(
      "Remboursements de ventes (débit marchand)",
      money(data.period.refundsNet)
    );
    line(
      "Montant brut remboursé aux acheteurs",
      money(data.period.refundsGross)
    );
    line(
      "Retraits du portefeuille partagé sur la période",
      money(data.period.withdrawalsGlobalWallet)
    );

    doc.moveDown(0.7)
      .font("Helvetica-Bold")
      .fontSize(12)
      .text("Situation actuelle au moment de génération")
      .moveDown(0.5);

    line(
      "Ventes sécurisées en attente (net marchand)",
      money(data.snapshot.merchantPending)
    );
    line(
      "Portefeuille partagé : total",
      money(data.snapshot.wallet)
    );
    line(
      "Portefeuille partagé : réservé",
      money(data.snapshot.walletLocked)
    );
    line(
      "Portefeuille partagé : disponible",
      money(data.snapshot.walletAvailable)
    );

    room(90);

    doc.font("Helvetica")
      .fontSize(8)
      .fillColor("#555555")
      .text(data.walletNotice)
      .moveDown();

    doc.font("Helvetica-Bold")
      .fontSize(12)
      .fillColor("#111111")
      .text("Historique financier détaillé")
      .moveDown(0.7);

    if (!data.transactions.length) {
      doc.font("Helvetica")
        .fontSize(10)
        .text("Aucun mouvement sur cette période.");
    }

    for (const tx of data.transactions) {
      const body =
        `${tx.description} - ${tx.status}\n` +
        `Référence : ${tx.id}` +
        (tx.orderId ? `\nCommande : ${tx.orderId}` : "");

      const meta =
        `Montant de référence : ${money(tx.amount)}` +
        ` | Réservation : ${
          tx.lockedDelta === null ? "n.d." : money(tx.lockedDelta)
        }` +
        ` | Attente marchand : ${
          tx.heldDelta === null ? "n.d." : money(tx.heldDelta)
        }` +
        ` | Solde après : ${
          tx.balanceAfter === null ? "n.d." : money(tx.balanceAfter)
        }`;

      const bodyHeight = doc.font("Helvetica")
        .fontSize(8)
        .heightOfString(body, { width: width - 120 });

      const metaHeight = doc.font("Helvetica")
        .fontSize(7)
        .heightOfString(meta, { width });

      const height =
        16 + bodyHeight + 9 + metaHeight + 18;

      room(height);
      const y = doc.y;

      doc.font("Helvetica-Bold")
        .fontSize(8)
        .fillColor("#111111")
        .text(date(tx.atMs), 42, y);

      doc.font("Helvetica")
        .fontSize(8)
        .text(body, 42, y + 16, {
          width: width - 120
        });

      const delta = tx.walletDelta === null
        ? "Non rapproché"
        : `${tx.walletDelta >= 0 ? "+" : ""}${money(tx.walletDelta)}`;

      doc.font("Helvetica-Bold")
        .fontSize(9)
        .text(delta, 42 + width - 115, y + 16, {
          width: 115,
          align: "right"
        });

      doc.font("Helvetica")
        .fontSize(7)
        .fillColor("#555555")
        .text(meta, 42, y + 16 + bodyHeight + 9, { width });

      doc.y = y + height;
      doc.x = 42;

      doc.moveTo(42, doc.y - 7)
        .lineTo(doc.page.width - 42, doc.y - 7)
        .strokeColor("#dddddd")
        .stroke();
    }

    const pages = doc.bufferedPageRange();

    for (
      let i = pages.start;
      i < pages.start + pages.count;
      i++
    ) {
      doc.switchToPage(i);

      const bottom = doc.page.margins.bottom;
      doc.page.margins.bottom = 0;

      doc.font("Helvetica")
        .fontSize(8)
        .fillColor("#666666")
        .text(
          `HaytiClips - ${i + 1}/${pages.count}`,
          42,
          doc.page.height - 32,
          {
            width,
            align: "right",
            lineBreak: false
          }
        );

      doc.page.margins.bottom = bottom;
    }

    doc.end();
    const buffer = await finished;

    res.set("Content-Type", "application/pdf")
      .set("Cache-Control", "no-store")
      .set(
        "Content-Disposition",
        `attachment; filename="hayticlips-${data.range.from}-${data.range.to}.pdf"`
      )
      .send(buffer);
  }

  return { summary, compute, entry, pdf };
})();



(() => {
  const {
    C,
    run,
    getUser,
    staffUser,
    ref,
    timestamp,
    wrap,
    audit,
    maxRows
  } = merchantOrdersV2;

  const auth = verifyFirebaseToken;

  app.get("/api/merchant/me", auth, wrap(async (req, res) => {
    const result = await run(async tx => {
      const user = await getUser(tx, req.user.uid);

      const snap = await tx.get(
        db.collection("merchantRequests")
          .where("uid", "==", req.user.uid)
          .limit(101)
      );

      C.fail(
        snap.size <= 100,
        "Historique des demandes à paginer",
        409
      );

      const requests = snap.docs
        .map(s => ({
          id: s.id,
          ...s.data(),
          createdAtMs: C.asMs(s.data().createdAt)
        }))
        .sort((a, b) => b.createdAtMs - a.createdAtMs);

      return {
        success: true,
        uid: req.user.uid,
        username: user.username || "",
        email: user.email || req.user.email || "",
        role: user.role || "user",
        suspended: user.suspended === true,
        active: C.active(user),
        status: user.merchantStatus || "none",

        shop: {
          name: user.merchantShopName || "",
          bio: user.merchantBio || "",
          logo: user.merchantLogo || "",
          banner: user.merchantBanner || "",
          email: user.merchantEmail || user.email || "",
          phone: user.merchantPhone || "",
          whatsapp: user.merchantWhatsapp || "",
          address: user.merchantAddress || "",
          zones: user.merchantDeliveryZones || "",
          delivery: user.merchantDeliveryTerms || "",
          category: user.merchantProductType || ""
        },

        lastRequest: requests[0] || null,
        rejectReason:
          user.merchantRejectReason ||
          requests[0]?.rejectReason ||
          "",
        suspensionReason: user.merchantSuspensionReason || ""
      };
    });

    res.json(result);
  }));

  app.post("/api/merchant/save-shop", auth, wrap(async (req, res) => {
    const body = req.body;

    const fields = {
      merchantBio: C.text(body.bio || "", "Description", 300),
      merchantEmail: C.text(
        body.email, "Email", 254, 3
      ).toLowerCase(),
      merchantPhone: C.text(
        body.phone || "", "Téléphone", 30
      ),
      merchantWhatsapp: C.text(
        body.whatsapp || "", "WhatsApp", 30
      ),
      merchantAddress: C.text(
        body.address || "", "Adresse", 200
      ),
      merchantDeliveryZones: C.text(
        body.zones || "", "Zones", 500
      ),
      merchantDeliveryTerms: C.text(
        body.delivery || "", "Livraison", 1000
      )
    };

    C.fail(
      /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(fields.merchantEmail),
      "Email invalide"
    );

    function ownMedia(value) {
      const url = new URL(value);
      const base = new URL(process.env.SUPABASE_URL);

      C.fail(
        url.origin === base.origin &&
        url.pathname.startsWith(
          `/storage/v1/object/public/media/shops/${req.user.uid}/`
        ),
        "Image de boutique invalide"
      );

      return url.href;
    }

    if (body.logo) fields.merchantLogo = ownMedia(body.logo);
    if (body.banner) fields.merchantBanner = ownMedia(body.banner);

    await run(async tx => {
      const user = await getUser(tx, req.user.uid);

      C.fail(C.active(user), "Boutique inactive", 403);

      tx.update(ref("users", req.user.uid), {
        ...fields,
        merchantUpdatedAt: timestamp()
      });
    });

    res.json({ success: true });
  }));

  app.get("/api/merchant/shop", auth, wrap(async (req, res) => {
    const uid = C.id(req.query.uid);
    const user = await ref("users", uid).get();

    C.fail(
      user.exists && C.active(user.data()),
      "Boutique temporairement indisponible",
      404
    );

    const data = user.data();

    const snap = await db.collection("merchantProducts")
      .where("merchantUid", "==", uid)
      .get();

    const products = snap.docs
      .filter(s =>
        s.data().isActive === true &&
        s.data().archived !== true
      )
      .map(s => ({
        id: s.id,
        ...s.data(),
        createdAtMs: C.asMs(s.data().createdAt)
      }));

    res.json({
      success: true,
      merchantUid: uid,
      username: data.username || "",
      shop: {
        name: data.merchantShopName || data.username,
        logo: data.merchantLogo || data.avatar || "",
        banner: data.merchantBanner || "",
        bio: data.merchantBio || "",
        email: data.merchantEmail || "",
        phone: data.merchantPhone || "",
        whatsapp: data.merchantWhatsapp || "",
        zones: data.merchantDeliveryZones || "",
        delivery: data.merchantDeliveryTerms || ""
      },
      products
    });
  }));

  app.get("/api/merchant/products", auth, wrap(async (req, res) => {
    const user = await ref("users", req.user.uid).get();

    C.fail(
      user.exists && C.active(user.data()),
      "Boutique inactive",
      403
    );

    const snap = await db.collection("merchantProducts")
      .where("merchantUid", "==", req.user.uid)
      .get();

    res.json({
      success: true,
      products: snap.docs
        .filter(s => s.data().archived !== true)
        .map(s => ({
          id: s.id,
          ...s.data(),
          createdAtMs: C.asMs(s.data().createdAt)
        }))
    });
  }));

  app.post("/api/merchant/save-product", auth, wrap(async (req, res) => {
    const uid = req.user.uid;
    const body = req.body;

    const productRef = body.productId
      ? ref("merchantProducts", C.id(body.productId))
      : db.collection("merchantProducts").doc();

    await run(async tx => {
      C.fail(
        C.active(await getUser(tx, uid)),
        "Boutique inactive",
        403
      );

      const previous = await tx.get(productRef);

      C.fail(
        !body.productId || previous.exists,
        "Produit introuvable",
        404
      );

      C.fail(
        !previous.exists ||
        previous.data().merchantUid === uid,
        "Accès refusé",
        403
      );

      if (previous.exists) {
        C.fail(
          Number.isInteger(body.expectedVersion) &&
          body.expectedVersion === Number(
            previous.data().version || 0
          ),
          "Produit ou stock modifié. Recharge les produits avant de réessayer.",
          409
        );
      }

      const data = {
        ...(previous.exists ? previous.data() : {}),
        ...body
      };

      const name = C.text(data.name, "Nom produit", 80, 2);

      const description = C.text(
        data.description || "", "Description", 500
      );

      const price = C.htg(
        C.minor(data.price, "Prix", false)
      );

      C.fail(
        Number.isInteger(data.stock) &&
        data.stock >= 0 &&
        data.stock <= 1000000,
        "Stock entier invalide"
      );

      C.fail(
        Array.isArray(data.images) &&
        data.images.length >= 1 &&
        data.images.length <= 8,
        "Ajoute entre 1 et 8 photos"
      );

      const base = new URL(process.env.SUPABASE_URL);

      for (const image of data.images) {
        const url = new URL(image);

        C.fail(
          url.origin === base.origin &&
          url.pathname.startsWith(
            `/storage/v1/object/public/media/products/${uid}/`
          ),
          "Photo non autorisée"
        );
      }

      C.fail(
        typeof data.isActive === "boolean",
        "État produit invalide"
      );

      tx.set(productRef, {
        merchantUid: uid,
        name,
        description,
        price,
        stock: data.stock,
        images: data.images,
        imageUrl: data.images[0],
        isActive: data.isActive,
        archived: false,
        version: Number(previous.data()?.version || 0) + 1,
        createdAt: previous.exists
          ? previous.data().createdAt
          : timestamp(),
        updatedAt: timestamp()
      });
    });

    res.json({
      success: true,
      productId: productRef.id
    });
  }));

  app.post("/api/merchant/archive-product", auth, wrap(async (req, res) => {
    await run(async tx => {
      C.fail(
        C.active(await getUser(tx, req.user.uid)),
        "Boutique inactive",
        403
      );

      const productRef = ref(
        "merchantProducts",
        C.id(req.body.productId)
      );

      const product = await tx.get(productRef);

      C.fail(
        product.exists &&
        product.data().merchantUid === req.user.uid,
        "Produit introuvable",
        404
      );

      tx.update(productRef, {
        isActive: false,
        archived: true,
        archivedAt: timestamp(),
        version: Number(product.data().version || 0) + 1
      });
    });

    res.json({ success: true });
  }));

  app.get("/api/merchant/admin/overview", auth, wrap(async (req, res) => {
    const staff = await ref("users", req.user.uid).get();
    C.staff(staff.exists ? staff.data() : null);

    const [requests, merchants, orders, sales] =
      await Promise.all([
        db.collection("merchantRequests")
          .where("status", "==", "pending")
          .get(),
        db.collection("users")
          .where("merchantStatus", "in", ["active", "suspended"])
          .get(),
        db.collection("merchantOrders")
          .limit(maxRows + 1)
          .get(),
        db.collection("walletTransactions")
          .where("type", "==", "merchant_sale_received")
          .limit(maxRows + 1)
          .get()
      ]);

    C.fail(
      orders.size <= maxRows && sales.size <= maxRows,
      "Vue de gestion trop volumineuse : ajouter une agrégation paginée",
      413
    );

    const heldNet = C.htg(
      orders.docs
        .filter(s =>
          s.data().accountingVersion === 2 &&
          s.data().fundsStatus === "held"
        )
        .reduce(
          (n, s) => n + C.minor(s.data().merchantNet),
          0
        )
    );

    const releasedNet = C.htg(
      sales.docs
        .filter(s => s.data().status === "completed")
        .reduce(
          (n, s) => n + C.minor(s.data().amount),
          0
        )
    );

    const rows = [];

    for (const snap of requests.docs) {
      const request = snap.data();
      const owner = await ref("users", request.uid).get();

      rows.push({
        id: snap.id,
        uid: request.uid,
        shopName: request.shopName,
        productType: request.productType,
        email: request.email || owner.data()?.email || "",
        description: request.description || "",
        legacyPaid: request.accessModel !== "free-v2",
        claimedAmountPaid: request.amountPaid || 0,
        username: owner.data()?.username || "Utilisateur",
        createdAtMs: C.asMs(request.createdAt)
      });
    }

    res.json({
      success: true,
      heldNet,
      releasedNet,
      legacyOpenOrders: orders.docs.filter(s =>
        !s.data().accountingVersion &&
        ["pending", "accepted"].includes(s.data().status)
      ).length,
      requests: rows.sort(
        (a, b) => a.createdAtMs - b.createdAtMs
      ),
      merchants: merchants.docs.map(s => ({
        uid: s.id,
        username: s.data().username || "",
        shopName: s.data().merchantShopName || "",
        status: s.data().merchantStatus,
        email: s.data().merchantEmail || "",
        suspensionReason:
          s.data().merchantSuspensionReason || ""
      }))
    });
  }));

  app.get("/api/merchant/admin/users", auth, wrap(async (req, res) => {
    const staff = await ref("users", req.user.uid).get();
    C.staff(staff.exists ? staff.data() : null, true);

    const search = String(req.query.search || "").trim();
    let docs = [];

    if (search) {
      C.fail(search.length <= 128, "Recherche trop longue");

      if (!search.includes("/")) {
        const byId = await ref("users", search).get();
        if (byId.exists) docs.push(byId);
      }

      const lower = search.toLowerCase();

      const names = await db.collection("users")
        .where("usernameLower", ">=", lower)
        .where("usernameLower", "<=", lower + "\uf8ff")
        .limit(50)
        .get();

      docs.push(...names.docs);
    } else {
      const special = await db.collection("users")
        .where("role", "in", [
          "admin",
          "moderator",
          "badge_manager",
          "merchant_manager"
        ])
        .limit(200)
        .get();

      docs = special.docs;
    }

    const users = [...new Map(
      docs.map(s => [
        s.id,
        {
          uid: s.id,
          username: s.data().username || "",
          avatar: s.data().avatar || "",
          role: s.data().role || "user"
        }
      ])
    ).values()];

    res.json({ success: true, users });
  }));

  app.post("/api/merchant/admin/set-role", auth, wrap(async (req, res) => {
    const target = C.id(req.body.targetUid);
    const role = req.body.role;

    C.fail(
      ["user", "moderator", "badge_manager", "merchant_manager"]
        .includes(role),
      "Rôle non autorisé"
    );

    await run(async tx => {
      await staffUser(tx, req.user.uid, true);
      const user = await getUser(tx, target);

      C.fail(
        target !== req.user.uid &&
        target !== ADMIN_UID &&
        user.role !== "admin",
        "Compte administrateur protégé",
        403
      );

      tx.update(ref("users", target), { role });

      audit(tx, req.user.uid, "set-role", target, {
        role,
        previous: user.role || "user"
      });
    });

    res.json({ success: true });
  }));

  const reportOptions = {
    db,
    run,
    getUser,
    maxRows
  };

  app.get("/api/merchant/summary", auth, wrap(async (req, res) => {
    const data = await merchantReportsV2.summary(
      reportOptions,
      req.user.uid,
      req.query
    );

    res.json(data);
  }));

  app.get("/api/merchant/report.pdf", auth, wrap(async (req, res) => {
    const data = await merchantReportsV2.summary(
      reportOptions,
      req.user.uid,
      req.query
    );

    await merchantReportsV2.pdf(data, supabase, res);
  }));
})();


const PORT = process.env.PORT || 3000;

app.listen(PORT, "0.0.0.0", () => {
  console.log("HaytiClips backend running on port " + PORT);
});
