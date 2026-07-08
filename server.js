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
 const ffmpeg = require("fluent-ffmpeg");
const { createClient } = require('@supabase/supabase-js');
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);


const tf = require('@tensorflow/tfjs-node');
const nsfw = require('nsfwjs');

// 🧠 Chargement du modèle par défaut (désormais intégré localement sans URL)
let nsfwModel = null;
nsfw.load().then(model => {
  nsfwModel = model;
  console.log("🧠 ✅ Modèle d'IA NSFWJS chargé et prêt pour la modération !");
}).catch(err => {
  console.error("❌ Erreur de chargement du modèle NSFWJS :", err);
});


const app = express();

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

const MERCHANT_RENEWAL_AMOUNT = 250;
const MERCHANT_RENEWAL_DAYS = 30;
const MERCHANT_APPROVAL_REFERRER_REWARD = 500;
const MERCHANT_APPROVAL_ADMIN_FEE = 250;
const MERCHANT_RENEWAL_REFERRER_REWARD = 200;
const MERCHANT_RENEWAL_ADMIN_FEE = 50;
const MERCHANT_SALE_ADMIN_FEE = 10;

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

// ROUTE ENVOI CADEAU
app.post("/api/sendGift", giftLimiter, verifyFirebaseToken, async (req,res)=>{

try{

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

  // 3️⃣ --- TOUTES LES ÉCRITURES (WRITES) SANS CONFLITS ---

  if (isCreatorAdmin) {
    // CAS 1 : UN UTILISATEUR NORMAL ENVOIE À L'ADMIN
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
    // CAS 2 : L'ADMIN ENVOIE À UN UTILISATEUR NORMAL
    const newCreatorBalance = creatorWallet + gift.creator;
    const newCreatorEarned = creatorEarned + gift.creator;
    
    // 🔥 CORRECTION ICI : L'admin paie le total (giftAmount), mais récupère sa propre taxe (gift.admin).
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
    // CAS 3 : UN UTILISATEUR NORMAL ENVOIE À UN AUTRE UTILISATEUR NORMAL
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

  // Historique cadeau général
  t.set(giftRef,{
    from: fromUser,
    to: creatorId,
    videoId: videoId,
    amount: giftAmount,
    creatorEarn: gift.creator,
    adminEarn: gift.admin,
    createdAt: admin.firestore.FieldValue.serverTimestamp()
  });

  // Transaction de sortie (Wallet)
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

  // Notification créateur
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

  // 🔥 MISE À JOUR ADMIN STATS - GIFT
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


res.json({ success: true });

}catch(e){

console.log("GIFT ERROR:", e);

res.status(500).json({
error: e.message
});

}

});




function getLocalDateString(date = new Date()) {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

/* ================= WALLET WITHDRAW ================= */


app.post("/api/requestWithdraw", verifyFirebaseToken, async (req,res)=>{
  try{
    const uid = req.user.uid;
    const {amount, pin} = req.body;

    const userRef = db.collection("users").doc(uid);
    const userSnap = await userRef.get();

    if(!userSnap.exists){
      return res.status(404).json({error:"User not found"});
    }

    const user = userSnap.data() || {};

    if(user.suspended === true){
      return res.status(403).json({error:"Compte suspendu"});
    }

    const isAdmin = uid === ADMIN_UID;
    const withdrawLimit = isAdmin ? 80000 : 20000;

    if(!user.withdrawNumber){
      return res.status(400).json({error:"Numero de retrait non enregistré"});
    }

    const pending = await db.collection("walletTransactions")
      .where("userId","==",uid)
      .where("type","==","withdraw")
      .where("status","==","pending")
      .limit(1)
      .get();

    if(!pending.empty){
      return res.status(400).json({error:"Un retrait est déjà en cours"});
    }

    if(!Number.isInteger(amount) || amount <= 0){
      return res.status(400).json({error:"Montant invalide"});
    }

    const allowedWithdraw = [250,500,1000,2000,5000,10000,20000,80000];

    if(!allowedWithdraw.includes(amount)){
      return res.status(400).json({error:"Montant non autorisé"});
    }

    if(!user.security || !user.security.pinHash){
      return res.status(400).json({error:"PIN non configuré"});
    }

    if(!/^\d{6}$/.test(pin || "")){
      return res.status(400).json({error:"PIN invalide"});
    }

    const valid = await bcrypt.compare(pin, user.security.pinHash);

    if(!valid){
      return res.status(400).json({error:"PIN incorrect"});
    }



const today = getLocalDateString();
let dailyTotal = Number(user.withdrawDailyTotal || 0);
let lastReset = user.withdrawResetAt || "";

if(lastReset !== today){
  dailyTotal = 0;
  lastReset = today;
}

if(dailyTotal + amount > withdrawLimit){
  return res.status(400).json({error:`Limite journalière atteinte (${withdrawLimit} Gdes / jour)`});
}

const money = getMoneyState(user);

if(amount > money.available){
  return res.status(400).json({error:"Solde disponible insuffisant"});
}

    const reference = makeOrderId("WDR");
    const txRef = db.collection("walletTransactions").doc();

    await txRef.set({
      userId: uid,
      amount,
      number: user.withdrawNumber,
      method: "moncash",
      type: "withdraw",
      status: "pending",
      providerReference: reference,
      createdAt: admin.firestore.FieldValue.serverTimestamp()
    });

    try{
      const payout = await moncashTransfer(
        amount,
        user.withdrawNumber,
        `Retrait HaytiClips ${uid}`,
        reference
      );

      const transfer = payout?.transfer;

      if(!transfer || transfer.message !== "successful"){
        await txRef.update({
          status: "failed",
          failedAt: admin.firestore.FieldValue.serverTimestamp(),
          failReason: "Transfert MonCash échoué"
        });

        return res.status(500).json({error:"Retrait échoué"});
      }

      await db.runTransaction(async (t)=>{
        const freshUserSnap = await t.get(userRef);

        if(!freshUserSnap.exists){
          throw new Error("Utilisateur introuvable");
        }

        const freshUser = freshUserSnap.data() || {};
const { wallet, available } = getMoneyState(freshUser);

if(amount > available){
  throw new Error("Solde disponible insuffisant");
}

const newWallet = wallet - amount;



t.update(userRef,{
  wallet: newWallet,
  withdrawDailyTotal: dailyTotal + amount,
  withdrawResetAt: lastReset,
  lastWithdrawRequest: Date.now()
});

        t.update(txRef,{
          status: "completed",
          completedAt: admin.firestore.FieldValue.serverTimestamp(),
          providerTransactionId: transfer.transaction_id || null,
          balanceAfter: newWallet
        });





// 🔥 ADMIN STATS - WITHDRAW
const statsRef = db.collection("adminStats").doc("finance");

await db.runTransaction(async (t) => {

  const statsDoc = await t.get(statsRef);

  let stats = statsDoc.exists ? statsDoc.data() : {
    totalIn: 0,
    totalOut: 0,
    netTotal: 0,
    transactionsCount: 0,
    typeTotals: {}
  };

  const amountValue = amount;
  const type = "withdraw";

  if(!stats.typeTotals[type]){
    stats.typeTotals[type] = { in:0, out:0, count:0 };
  }

  stats.totalOut += amountValue;
  stats.transactionsCount += 1;
  stats.typeTotals[type].out += amountValue;
  stats.typeTotals[type].count += 1;
  stats.netTotal = stats.totalIn - stats.totalOut;

  t.set(statsRef, stats);


});




      });

      return res.json({success:true, txId:txRef.id});

    }catch(providerError){
      console.log("WITHDRAW PROVIDER ERROR:", providerError.response?.data || providerError.message || providerError);

      try{
        const statusResponse = await moncashPrefundedTransactionStatus(reference);
        const txStatus = statusResponse?.transStatus;

        if(txStatus === "successful"){
          await db.runTransaction(async (t)=>{
            const freshUserSnap = await t.get(userRef);

            if(!freshUserSnap.exists){
              throw new Error("Utilisateur introuvable");
            }

            const freshUser = freshUserSnap.data() || {};
const { wallet, available } = getMoneyState(freshUser);

if(amount > available){
  throw new Error("Solde disponible insuffisant");
}

const newWallet = wallet - amount;

t.update(userRef,{
  wallet: newWallet,
  withdrawDailyTotal: dailyTotal + amount,
  withdrawResetAt: lastReset,
  lastWithdrawRequest: Date.now()
});



            t.update(txRef,{
              status: "completed",
              completedAt: admin.firestore.FieldValue.serverTimestamp(),
              balanceAfter: newWallet
            });
          });

          return res.json({success:true, txId:txRef.id});
        }
      }catch(statusErr){
        console.log("WITHDRAW STATUS ERROR:", statusErr.response?.data || statusErr.message || statusErr);
      }

      await txRef.update({
        status: "failed",
        failedAt: admin.firestore.FieldValue.serverTimestamp(),
        failReason: "Transfert MonCash échoué"
      });

      return res.status(500).json({error:"Retrait échoué"});
    }

  }catch(e){
    console.log("WITHDRAW ERROR:", e.response?.data || e.message || e);
    res.status(500).json({error:e.message});
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


app.post("/set-pin", verifyFirebaseToken, async (req,res)=>{
  try{

    const uid = req.user.uid;
    const { pin } = req.body;


if(!/^\d{6}$/.test(pin || "")){
  return res.status(400).json({error:"PIN invalide"});
}


const userSnap = await db.collection("users").doc(uid).get();

if(!userSnap.exists){
  return res.status(404).json({error:"User not found"});
}

const userData = userSnap.data() || {};

if(userData.suspended === true){
  return res.status(403).json({error:"Compte suspendu"});
}

    // 🔐 HASH bcrypt
    const hash = await bcrypt.hash(pin, 10);

    await db.collection("users").doc(uid).update({
      "security.enabled": true,
      "security.pinHash": hash,
      "security.createdAt": admin.firestore.FieldValue.serverTimestamp()
    });

    res.json({success:true});

  }catch(e){
    console.log("SET PIN ERROR:", e);
    res.status(500).json({error:e.message});
  }
});



app.post("/check-pin", verifyFirebaseToken, async (req,res)=>{
  try{

    const uid = req.user.uid;
    const { pin } = req.body;
 
if(!/^\d{6}$/.test(pin || "")){
  return res.json({success:false});
}
     
    const userSnap = await db.collection("users").doc(uid).get();

    if(!userSnap.exists){
      return res.json({success:false});
    }

    const user = userSnap.data();

    const hash = user.security?.pinHash;

    if(!hash){
      return res.json({success:false});
    }

    // 🔐 comparaison bcrypt
    const valid = await bcrypt.compare(pin, hash);

    if(!valid){
      return res.json({success:false});
    }

    // ✅ update dernière vérification
    await db.collection("users").doc(uid).update({
      "security.lastVerified": admin.firestore.FieldValue.serverTimestamp()
    });

    res.json({success:true});

  }catch(e){
    console.log("CHECK PIN ERROR:", e);
    res.status(500).json({success:false});
  }
});


app.post("/remove-pin", verifyFirebaseToken, async (req,res)=>{
  try{

    const uid = req.user.uid;
    const { pin } = req.body;

if(!/^\d{6}$/.test(pin || "")){
  return res.json({error:"PIN invalide"});
}

    const userRef = db.collection("users").doc(uid);
    const userSnap = await userRef.get();

    if(!userSnap.exists){
      return res.json({error:"User not found"});
    }

    const user = userSnap.data();
    const hash = user.security?.pinHash;

if(user.suspended === true){
  return res.status(403).json({error:"Compte suspendu"});
}

    if(!hash){
      return res.json({error:"Aucun PIN"});
    }

    const valid = await bcrypt.compare(pin, hash);

    if(!valid){
      return res.json({error:"PIN incorrect"});
    }

    await userRef.update({
      "security.enabled": false,
      "security.pinHash": null,
      "security.lastVerified": admin.firestore.FieldValue.serverTimestamp()
    });

    res.json({success:true});

  }catch(e){
    console.log("REMOVE PIN ERROR:", e);
    res.status(500).json({error:e.message});
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
      security: {
          enabled: false
        },
      createdAt: admin.firestore.FieldValue.serverTimestamp()
    });

    return res.json({ success: true });

  } catch (e) {
    console.log("INIT USER PROFILE ERROR:", e);
    return res.status(500).json({ error: e.message });
  }
});

app.post("/api/requestMerchantAccess", verifyFirebaseToken, async (req,res)=>{
  try{
    const uid = req.user.uid;
    const { shopName, productType, referralCode } = req.body;

    if(
      typeof shopName !== "string" ||
      typeof productType !== "string" ||
      typeof referralCode !== "string"
    ){
      return res.status(400).json({error:"Données invalides"});
    }

    const cleanShopName = shopName.trim();
    const cleanProductType = productType.trim();
    const cleanReferralCode = referralCode.trim().toUpperCase();

    if(!cleanShopName || cleanShopName.length < 2 || cleanShopName.length > 60){
      return res.status(400).json({error:"Nom de boutique invalide"});
    }

    if(!cleanProductType || cleanProductType.length < 2 || cleanProductType.length > 60){
      return res.status(400).json({error:"Type de produits invalide"});
    }

    if(!cleanReferralCode){
      return res.status(400).json({error:"Code parrain obligatoire"});
    }

    const userRef = db.collection("users").doc(uid);
    const userSnap = await userRef.get();

    if(!userSnap.exists){
      return res.status(404).json({error:"Utilisateur introuvable"});
    }

    const userData = userSnap.data() || {};

    if(userData.suspended === true){
      return res.status(403).json({error:"Compte suspendu"});
    }

    if(userData.merchantStatus === "pending"){
      return res.status(400).json({error:"Une demande est déjà en attente"});
    }

    if(userData.merchantStatus === "active"){
      return res.status(400).json({error:"Compte marchand déjà actif"});
    }

    const refSnap = await db.collection("users")
      .where("referralCode","==",cleanReferralCode)
      .limit(1)
      .get();

    if(refSnap.empty){
      return res.status(400).json({error:"Code parrain invalide"});
    }

    const refDoc = refSnap.docs[0];
    const referrerUid = refDoc.id;
    const referrerData = refDoc.data() || {};

    if(referrerUid === uid){
      return res.status(400).json({error:"Vous ne pouvez pas utiliser votre propre code"});
    }

    if(referrerData.canReferMerchant !== true){
      return res.status(400).json({error:"Ce code parrain n’est pas autorisé"});
    }


const money = getMoneyState(userData);

if(money.available < 750){
  return res.status(400).json({error:"Solde disponible insuffisant"});
}

    const existingPending = await db.collection("merchantRequests")
      .where("uid","==",uid)
      .where("status","==","pending")
      .limit(1)
      .get();

    if(!existingPending.empty){
      return res.status(400).json({error:"Une demande est déjà en attente"});
    }

    const requestRef = db.collection("merchantRequests").doc();
    const txRef = db.collection("walletTransactions").doc();

    await db.runTransaction(async (t)=>{
      const freshUserSnap = await t.get(userRef);

      if(!freshUserSnap.exists){
        throw new Error("Utilisateur introuvable");
      }

      const freshUser = freshUserSnap.data() || {};


const { wallet, available } = getMoneyState(freshUser);

if(available < 750){
  throw new Error("Solde disponible insuffisant");
}

const newWallet = wallet - 750;
      t.update(userRef,{
        wallet: newWallet,
        merchantStatus: "pending",
        merchantEnabled: false,
        merchantShopName: cleanShopName,
        merchantProductType: cleanProductType,
        merchantReferrerUid: referrerUid
      });

      t.set(requestRef,{
        uid,
        shopName: cleanShopName,
        productType: cleanProductType,
        referralCode: cleanReferralCode,
        referrerUid,
        amountPaid: 750,
        status: "pending",
        reviewedBy: null,
        reviewedAt: null,
        rejectReason: "",
        refundDone: false,
        payoutDone: false,
        createdAt: admin.firestore.FieldValue.serverTimestamp()
      });

      t.set(txRef,{
        userId: uid,
        type: "merchant_request_payment",
        amount: 750,
        status: "completed",
        merchantRequestId: requestRef.id,
        balanceAfter: newWallet,
        createdAt: admin.firestore.FieldValue.serverTimestamp()
      });
    });

    return res.json({success:true});

  }catch(e){
    console.log("MERCHANT REQUEST ERROR:", e);
    return res.status(500).json({error:e.message});
  }
});

app.post("/api/approveMerchant", verifyFirebaseToken, async (req,res)=>{
  try{
    const staffUid = req.user.uid;
    const { requestId } = req.body;

    const staffSnap = await db.collection("users").doc(staffUid).get();

    if(!staffSnap.exists){
      return res.status(403).json({error:"Accès refusé"});
    }

    const staffData = staffSnap.data() || {};
    const role = staffData.role || "";

    if(role !== "admin" && role !== "merchant_manager"){
      return res.status(403).json({error:"Accès refusé"});
    }

    const requestRef = db.collection("merchantRequests").doc(requestId);
    const requestSnap = await requestRef.get();

    if(!requestSnap.exists){
      return res.status(404).json({error:"Demande introuvable"});
    }

    const requestData = requestSnap.data() || {};

    if(requestData.status !== "pending"){
      return res.status(400).json({error:"Demande déjà traitée"});
    }

    const userRef = db.collection("users").doc(requestData.uid);
    const referrerRef = db.collection("users").doc(requestData.referrerUid);
    const adminRef = db.collection("users").doc(ADMIN_UID);

    await db.runTransaction(async (t)=>{
      const freshReq = await t.get(requestRef);
      const userSnap = await t.get(userRef);
      const referrerSnap = await t.get(referrerRef);
      const adminSnap = await t.get(adminRef);

      if(!freshReq.exists) throw new Error("Demande introuvable");
      if(freshReq.data().status !== "pending") throw new Error("Demande déjà traitée");
      if(!userSnap.exists) throw new Error("Utilisateur introuvable");
      if(!referrerSnap.exists) throw new Error("Parrain introuvable");
      if(!adminSnap.exists) throw new Error("Admin introuvable");

      const referrerData = referrerSnap.data() || {};
      const adminData = adminSnap.data() || {};

const newReferrerWallet = Number(referrerData.wallet || 0) + MERCHANT_APPROVAL_REFERRER_REWARD;
const newAdminWallet = Number(adminData.wallet || 0) + MERCHANT_APPROVAL_ADMIN_FEE;
const newReferrerEarned = Number(referrerData.walletEarned || 0) + MERCHANT_APPROVAL_REFERRER_REWARD;
const newAdminEarned = Number(adminData.walletEarned || 0) + MERCHANT_APPROVAL_ADMIN_FEE;
const expiresAt = addDaysTimestamp(MERCHANT_RENEWAL_DAYS);


t.update(userRef,{
  merchantStatus: "active",
  merchantEnabled: true,
  merchantApprovedAt: admin.firestore.FieldValue.serverTimestamp(),
  merchantApprovedBy: staffUid,
  merchantExpiresAt: expiresAt,
  merchantLastRenewedAt: admin.firestore.FieldValue.serverTimestamp(),
  merchantRenewalAmount: MERCHANT_RENEWAL_AMOUNT,
  merchantRenewalDays: MERCHANT_RENEWAL_DAYS,
  merchantRenewalBlocked: false
});


      t.update(referrerRef,{
        wallet: newReferrerWallet,
        walletEarned: newReferrerEarned
      });

      t.update(adminRef,{
        wallet: newAdminWallet,
        walletEarned: newAdminEarned
      });

      t.update(requestRef,{
        status: "approved",
        reviewedBy: staffUid,
        reviewedAt: admin.firestore.FieldValue.serverTimestamp(),
        payoutDone: true
      });

      const refTx = db.collection("walletTransactions").doc();
      const adminTx = db.collection("walletTransactions").doc();

      t.set(refTx,{
        userId: requestData.referrerUid,
        type: "merchant_referral_reward",
amount: MERCHANT_APPROVAL_REFERRER_REWARD,
        status: "completed",
        merchantRequestId: requestId,
        balanceAfter: newReferrerWallet,
        createdAt: admin.firestore.FieldValue.serverTimestamp()
      });

      t.set(adminTx,{
        userId: ADMIN_UID,
        type: "merchant_platform_fee",
amount: MERCHANT_APPROVAL_ADMIN_FEE,
        status: "completed",
        merchantRequestId: requestId,
        balanceAfter: newAdminWallet,
        createdAt: admin.firestore.FieldValue.serverTimestamp()
      });
    });

    return res.json({success:true});

  }catch(e){
    console.log("APPROVE MERCHANT ERROR:", e);
    return res.status(500).json({error:e.message});
  }
});

app.post("/api/rejectMerchant", verifyFirebaseToken, async (req,res)=>{
  try{
    const staffUid = req.user.uid;
    const { requestId, rejectReason } = req.body;

    const staffSnap = await db.collection("users").doc(staffUid).get();

    if(!staffSnap.exists){
      return res.status(403).json({error:"Accès refusé"});
    }

    const staffData = staffSnap.data() || {};
    const role = staffData.role || "";

    if(role !== "admin" && role !== "merchant_manager"){
      return res.status(403).json({error:"Accès refusé"});
    }

    const requestRef = db.collection("merchantRequests").doc(requestId);
    const requestSnap = await requestRef.get();

    if(!requestSnap.exists){
      return res.status(404).json({error:"Demande introuvable"});
    }

    const requestData = requestSnap.data() || {};

    if(requestData.status !== "pending"){
      return res.status(400).json({error:"Demande déjà traitée"});
    }

    const userRef = db.collection("users").doc(requestData.uid);

    await db.runTransaction(async (t)=>{
      const freshReq = await t.get(requestRef);
      const userSnap = await t.get(userRef);

      if(!freshReq.exists) throw new Error("Demande introuvable");
      if(freshReq.data().status !== "pending") throw new Error("Demande déjà traitée");
      if(!userSnap.exists) throw new Error("Utilisateur introuvable");

      const userData = userSnap.data() || {};
      const newWallet = Number(userData.wallet || 0) + 750;

      t.update(userRef,{
        wallet: newWallet,
        merchantStatus: "rejected",
        merchantEnabled: false
      });

      t.update(requestRef,{
        status: "rejected",
        reviewedBy: staffUid,
        reviewedAt: admin.firestore.FieldValue.serverTimestamp(),
        rejectReason: typeof rejectReason === "string" ? rejectReason.trim() : "",
        refundDone: true
      });

      const refundTx = db.collection("walletTransactions").doc();

      t.set(refundTx,{
        userId: requestData.uid,
        type: "merchant_request_refund",
        amount: 750,
        status: "completed",
        merchantRequestId: requestId,
        balanceAfter: newWallet,
        createdAt: admin.firestore.FieldValue.serverTimestamp()
      });
    });

    return res.json({success:true});

  }catch(e){
    console.log("REJECT MERCHANT ERROR:", e);
    return res.status(500).json({error:e.message});
  }
});

app.post("/api/enableMerchantReferrer", verifyFirebaseToken, async (req,res)=>{
  try{
    const adminUid = req.user.uid;
    const { targetUid } = req.body;

    if(typeof targetUid !== "string" || !targetUid.trim()){
      return res.status(400).json({error:"Utilisateur cible invalide"});
    }

    const adminSnap = await db.collection("users").doc(adminUid).get();

    if(!adminSnap.exists){
      return res.status(403).json({error:"Accès refusé"});
    }

    const adminData = adminSnap.data() || {};

    if(adminData.role !== "admin"){
      return res.status(403).json({error:"Accès refusé"});
    }

    if(targetUid === adminUid){
      return res.status(400).json({error:"Impossible de modifier votre propre compte admin ici."});
    }

    const userRef = db.collection("users").doc(targetUid);

    let finalCode = "";

    await db.runTransaction(async (t)=>{
      const userSnap = await t.get(userRef);

      if(!userSnap.exists){
        throw new Error("Utilisateur introuvable");
      }

      const userData = userSnap.data() || {};

      finalCode = userData.referralCode || "";

      if(!finalCode){
        finalCode = await generateUniqueReferralCode();
      }

      t.update(userRef,{
        canReferMerchant: true,
        referralCode: finalCode
      });
    });

    return res.json({
      success:true,
      referralCode: finalCode
    });

  }catch(e){
    console.log("ENABLE MERCHANT REFERRER ERROR:", e);
    return res.status(500).json({error:e.message});
  }
});

app.post("/api/disableMerchantReferrer", verifyFirebaseToken, async (req,res)=>{
  try{
    const adminUid = req.user.uid;
    const { targetUid } = req.body;

    if(typeof targetUid !== "string" || !targetUid.trim()){
      return res.status(400).json({error:"Utilisateur cible invalide"});
    }

    const adminSnap = await db.collection("users").doc(adminUid).get();

    if(!adminSnap.exists){
      return res.status(403).json({error:"Accès refusé"});
    }

    const adminData = adminSnap.data() || {};

    if(adminData.role !== "admin"){
      return res.status(403).json({error:"Accès refusé"});
    }

    if(targetUid === adminUid){
      return res.status(400).json({error:"Impossible de modifier votre propre compte admin ici."});
    }

    const userRef = db.collection("users").doc(targetUid);
    const userSnap = await userRef.get();

    if(!userSnap.exists){
      return res.status(404).json({error:"Utilisateur introuvable"});
    }

    await userRef.update({
      canReferMerchant: false
    });

    return res.json({success:true});

  }catch(e){
    console.log("DISABLE MERCHANT REFERRER ERROR:", e);
    return res.status(500).json({error:e.message});
  }
});



// 🔥 NOUVELLE LOGIQUE DE SUPPRESSION
app.post("/api/removeMerchantAccess", verifyFirebaseToken, async (req,res)=>{
  try{
    const staffUid = req.user.uid;
    const { targetUid, imagesToDelete } = req.body;

    if(typeof targetUid !== "string" || !targetUid.trim()){
      return res.status(400).json({error:"Utilisateur cible invalide"});
    }

    const staffSnap = await db.collection("users").doc(staffUid).get();

    if(!staffSnap.exists){
      return res.status(403).json({error:"Accès refusé"});
    }

    const staffData = staffSnap.data() || {};
    const role = staffData.role || "";

    if(role !== "admin" && role !== "merchant_manager"){
      return res.status(403).json({error:"Accès refusé"});
    }

    const userRef = db.collection("users").doc(targetUid);
    const userSnap = await userRef.get();

    if(!userSnap.exists){
      return res.status(404).json({error:"Utilisateur introuvable"});
    }

    const userData = userSnap.data() || {};

    if(userData.merchantStatus !== "active"){
      return res.status(400).json({error:"Ce compte marchand n’est pas actif"});
    }

    // 1. Préparer la liste de toutes les images à supprimer (Produits + Logo Boutique)
    let fileNames = [];

    // Récupérer les images des produits (envoyées par le frontend)
    if (imagesToDelete && Array.isArray(imagesToDelete)) {
      imagesToDelete.forEach(url => {
        if (typeof url === 'string') {
          const urlParts = url.split('/media/');
          if (urlParts.length > 1) fileNames.push(decodeURIComponent(urlParts[1]));
        }
      });
    }

    // Récupérer le logo de la boutique depuis Firestore pour le supprimer de Supabase
    if (userData.merchantLogo && typeof userData.merchantLogo === 'string') {
      const logoParts = userData.merchantLogo.split('/media/');
      if (logoParts.length > 1) {
        fileNames.push(decodeURIComponent(logoParts[1]));
      }
    }

    // 2. Remettre le compte marchand TOTALEMENT à zéro dans Firestore
    await userRef.update({
      merchantStatus: "none",
      merchantEnabled: false,
      merchantApprovedAt: null,
      merchantApprovedBy: null,
      merchantExpiresAt: null,        // 🔥 Stoppe la date d'expiration pour bloquer le renouvèlement
      merchantLastRenewedAt: null,    // 🔥 Efface la date de renouvellement
      merchantRenewalBlocked: false,  // 🔥 Permet de refaire une VRAIE nouvelle demande plus tard
      merchantShopName: "",           // 🔥 Efface le nom
      merchantProductType: "",        // 🔥 Efface le type
      merchantBio: "",                // 🔥 Efface la bio (si existante)
      merchantLogo: null,             // 🔥 Efface le logo de la Base de Données
      merchantReferrerUid: null       // 🔥 Efface le parrain associé
    });

    // 3. Supprimer physiquement de Supabase
    if (fileNames.length > 0) {
      try {
        await supabase.storage.from('media').remove(fileNames);
        console.log(`✅ ${fileNames.length} fichiers (produits + logo) supprimés de Supabase pour l'UID: ${targetUid}`);
      } catch (err) {
        console.error("❌ Erreur Supabase lors de la suppression :", err.message);
      }
    }

    return res.json({success:true});

  }catch(e){
    console.log("REMOVE MERCHANT ACCESS ERROR:", e);
    return res.status(500).json({error:e.message});
  }
});


app.post("/api/merchant/buy-product", verifyFirebaseToken, async (req,res)=>{
  try{
    const buyerUid = req.user.uid;
    const {
      productId,
      quantity,
      customerNom,
      customerPrenom,
      customerEmail,
      customerPhone,
      customerWhatsapp,
      customerSexe,
      customerDetails
    } = req.body;

    const qty = Number(quantity || 1);

    if(!productId || !Number.isInteger(qty) || qty <= 0){
      return res.status(400).json({error:"Données invalides"});
    }

    const buyerRef = db.collection("users").doc(buyerUid);
    const productRef = db.collection("merchantProducts").doc(productId);

    await db.runTransaction(async (t)=>{
      const buyerSnap = await t.get(buyerRef);
      const productSnap = await t.get(productRef);

      if(!buyerSnap.exists) throw new Error("Acheteur introuvable");
      if(!productSnap.exists) throw new Error("Produit introuvable");

      const buyer = buyerSnap.data() || {};
      const product = productSnap.data() || {};

      if(product.isActive !== true){
        throw new Error("Produit inactif");
      }

      const merchantUid = product.merchantUid;
      if(!merchantUid){
        throw new Error("Marchand introuvable");
      }

      const merchantRef = db.collection("users").doc(merchantUid);
      const merchantSnap = await t.get(merchantRef);

      if(!merchantSnap.exists){
        throw new Error("Marchand introuvable");
      }

      const merchant = merchantSnap.data() || {};

      if(
        merchant.merchantEnabled !== true ||
        merchant.merchantStatus !== "active" ||
        isMerchantBlocked(merchant)
      ){
        throw new Error("Cette boutique est temporairement indisponible");
      }

      const stock = Number(product.stock || 0);
      if(stock < qty){
        throw new Error("Stock insuffisant");
      }

      const total = Number(product.price || 0) * qty;


const { wallet, walletLocked, available } = getMoneyState(buyer);

if(available < total){
  throw new Error("Solde disponible insuffisant");
}

const newWallet = wallet - total;
const newLocked = walletLocked + total;

      t.update(buyerRef,{
        wallet: newWallet,
        walletLocked: newLocked
      });

      t.update(productRef,{
        stock: stock - qty
      });

      const orderRef = db.collection("merchantOrders").doc();

      t.set(orderRef,{
        buyerUid,
        merchantUid,
        productId,
        productName: product.name || "Produit",
        quantity: qty,
        totalAmount: total,
        status: "pending",
        customerNom: String(customerNom || "").trim(),
        customerPrenom: String(customerPrenom || "").trim(),
        customerEmail: String(customerEmail || "").trim().toLowerCase(),
        customerPhone: String(customerPhone || "").trim(),
        customerWhatsapp: String(customerWhatsapp || "").trim(),
        customerSexe: String(customerSexe || "").trim(),
        customerDetails: String(customerDetails || "").trim(),
        createdAt: admin.firestore.FieldValue.serverTimestamp()
      });

      const buyerTxRef = db.collection("walletTransactions").doc();
      const merchantTxRef = db.collection("walletTransactions").doc();

      t.set(buyerTxRef,{
        userId: buyerUid,
        type: "merchant_order_created",
        amount: total,
        status: "pending",
        orderId: orderRef.id,
        merchantUid,
        productId,
        productName: product.name || "Produit",
        quantity: qty,
        balanceAfter: newWallet,
        walletLockedAfter: newLocked,
        createdAt: admin.firestore.FieldValue.serverTimestamp()
      });

      t.set(merchantTxRef,{
        userId: merchantUid,
        type: "merchant_sale_pending",
        amount: total,
        status: "pending",
        orderId: orderRef.id,
        buyerId: buyerUid,
        productId,
        productName: product.name || "Produit",
        quantity: qty,
        createdAt: admin.firestore.FieldValue.serverTimestamp()
      });
    });

    return res.json({success:true});
  }catch(e){
    console.log("BUY PRODUCT ERROR:", e);
    return res.status(500).json({error:e.message});
  }
});



app.post("/api/merchant/accept-order", verifyFirebaseToken, async (req,res)=>{
  try{
    const merchantUid = req.user.uid;
    const { orderId } = req.body;

    const orderRef = db.collection("merchantOrders").doc(orderId);

    await db.runTransaction(async (t)=>{
      const orderSnap = await t.get(orderRef);
      if(!orderSnap.exists) throw new Error("Commande introuvable");

      const order = orderSnap.data() || {};

      if(order.merchantUid !== merchantUid){
        throw new Error("Accès refusé");
      }

      if(order.status !== "pending"){
        throw new Error("Commande déjà traitée");
      }

      const merchantRef = db.collection("users").doc(merchantUid);
      const buyerRef = db.collection("users").doc(order.buyerUid);
      const adminRef = db.collection("users").doc(ADMIN_UID);
      const statsRef = db.collection("adminStats").doc("finance"); // 👈 DÉCLARATION ICI

      // 🔥 TOUTES LES LECTURES EN PREMIER 🔥
      const merchantSnap = await t.get(merchantRef);
      const buyerSnap = await t.get(buyerRef);
      const adminSnap = await t.get(adminRef);
      const statsDoc = await t.get(statsRef); // 👈 LECTURE DÉPLACÉE ICI EN HAUT

      if(!merchantSnap.exists) throw new Error("Marchand introuvable");
      if(!buyerSnap.exists) throw new Error("Acheteur introuvable");
      if(!adminSnap.exists) throw new Error("Admin introuvable");

      const merchant = merchantSnap.data() || {};
      const buyer = buyerSnap.data() || {};
      const adminData = adminSnap.data() || {};

      if(
        merchant.merchantEnabled !== true ||
        merchant.merchantStatus !== "active" ||
        isMerchantBlocked(merchant)
      ){
        throw new Error("Accès marchand bloqué");
      }

      const buyerLocked = Number(buyer.walletLocked || 0);
      const total = Number(order.totalAmount || 0);

      if(buyerLocked < total){
        throw new Error("Montant bloqué insuffisant");
      }

      const saleFee = Math.min(MERCHANT_SALE_ADMIN_FEE, total);
      const merchantNet = total - saleFee;

      const newBuyerLocked = buyerLocked - total;

      let newMerchantWallet = Number(merchant.wallet || 0) + merchantNet;
      let newMerchantEarned = Number(merchant.walletEarned || 0) + merchantNet;
      let newAdminWallet = Number(adminData.wallet || 0) + saleFee;
      let newAdminEarned = Number(adminData.walletEarned || 0) + saleFee;

      // 🔥 TOUTES LES ÉCRITURES ENSUITE 🔥
      t.update(buyerRef,{
        walletLocked: newBuyerLocked
      });

      if(merchantUid === ADMIN_UID){
        newMerchantWallet = Number(adminData.wallet || 0) + total;
        newMerchantEarned = Number(adminData.walletEarned || 0) + total;
        newAdminWallet = newMerchantWallet;
        newAdminEarned = newMerchantEarned;

        t.update(adminRef,{
          wallet: newAdminWallet,
          walletEarned: newAdminEarned
        });
      }else{
        t.update(merchantRef,{
          wallet: newMerchantWallet,
          walletEarned: newMerchantEarned
        });

        t.update(adminRef,{
          wallet: newAdminWallet,
          walletEarned: newAdminEarned
        });
      }

      t.update(orderRef,{
        status: "accepted",
        acceptedAt: admin.firestore.FieldValue.serverTimestamp()
      });

      const merchantTxRef = db.collection("walletTransactions").doc();
      const adminTxRef = db.collection("walletTransactions").doc();
      const buyerTxRef = db.collection("walletTransactions").doc();

      t.set(merchantTxRef,{
        userId: merchantUid,
        type: "merchant_sale_received",
        amount: merchantNet,
        feeAmount: saleFee,
        grossAmount: total,
        status: "completed",
        orderId,
        buyerId: order.buyerUid,
        productId: order.productId,
        productName: order.productName || "Produit",
        quantity: Number(order.quantity || 0),
        balanceAfter: newMerchantWallet,
        createdAt: admin.firestore.FieldValue.serverTimestamp()
      });

      t.set(adminTxRef,{
        userId: ADMIN_UID,
        type: "merchant_sale_fee",
        amount: saleFee,
        status: "completed",
        orderId,
        merchantUid,
        productId: order.productId,
        productName: order.productName || "Produit",
        balanceAfter: newAdminWallet,
        createdAt: admin.firestore.FieldValue.serverTimestamp()
      });

      // 🔥 MISE À JOUR ADMIN STATS (SANS FAIRE DE LECTURE ICI) 🔥
      let stats = statsDoc.exists ? statsDoc.data() : {
        totalIn: 0,
        totalOut: 0,
        netTotal: 0,
        transactionsCount: 0,
        typeTotals: {}
      };

      const amountValue = saleFee;
      const type = "merchant_sale_fee";

      if(!stats.typeTotals[type]){
        stats.typeTotals[type] = { in:0, out:0, count:0 };
      }

      stats.totalIn += amountValue;
      stats.transactionsCount += 1;
      stats.typeTotals[type].in += amountValue;
      stats.typeTotals[type].count += 1;
      stats.netTotal = stats.totalIn - stats.totalOut;

      t.set(statsRef, stats); // 👈 ÉCRITURE DES STATS

      t.set(buyerTxRef,{
        userId: order.buyerUid,
        type: "merchant_order_paid",
        amount: total,
        status: "completed",
        orderId,
        merchantUid,
        productId: order.productId,
        productName: order.productName || "Produit",
        quantity: Number(order.quantity || 0),
        balanceAfter: Number(buyer.wallet || 0),
        walletLockedAfter: newBuyerLocked,
        createdAt: admin.firestore.FieldValue.serverTimestamp()
      });
    });

    return res.json({success:true});
  }catch(e){
    console.log("ACCEPT ORDER ERROR:", e);
    return res.status(500).json({error:e.message});
  }
});


app.post("/api/merchant/reject-order", verifyFirebaseToken, async (req,res)=>{
  try{
    const merchantUid = req.user.uid;
    const { orderId } = req.body;

    const orderRef = db.collection("merchantOrders").doc(orderId);

    await db.runTransaction(async (t)=>{
      const orderSnap = await t.get(orderRef);
      if(!orderSnap.exists) throw new Error("Commande introuvable");

      const order = orderSnap.data() || {};

      if(order.merchantUid !== merchantUid){
        throw new Error("Accès refusé");
      }

      if(order.status !== "pending"){
        throw new Error("Commande déjà traitée");
      }

      const buyerRef = db.collection("users").doc(order.buyerUid);
      const productRef = db.collection("merchantProducts").doc(order.productId);

      const buyerSnap = await t.get(buyerRef);
      const productSnap = await t.get(productRef);

      if(!buyerSnap.exists) throw new Error("Acheteur introuvable");

      const buyer = buyerSnap.data() || {};
      const total = Number(order.totalAmount || 0);
      const qty = Number(order.quantity || 0);
      const buyerWallet = Number(buyer.wallet || 0);
      const buyerLocked = Number(buyer.walletLocked || 0);

      const newBuyerWallet = buyerWallet + total;
      const newBuyerLocked = Math.max(0, buyerLocked - total);

      t.update(buyerRef,{
        wallet: newBuyerWallet,
        walletLocked: newBuyerLocked
      });

      if(productSnap.exists){
        const product = productSnap.data() || {};
        t.update(productRef,{
          stock: Number(product.stock || 0) + qty
        });
      }

      t.update(orderRef,{
        status: "rejected",
        rejectedAt: admin.firestore.FieldValue.serverTimestamp()
      });

      const buyerTxRef = db.collection("walletTransactions").doc();
      const merchantTxRef = db.collection("walletTransactions").doc();

      t.set(buyerTxRef,{
        userId: order.buyerUid,
        type: "merchant_order_refund",
        amount: total,
        status: "completed",
        orderId,
        merchantUid,
        productId: order.productId,
        productName: order.productName || "Produit",
        quantity: qty,
        balanceAfter: newBuyerWallet,
        walletLockedAfter: newBuyerLocked,
        createdAt: admin.firestore.FieldValue.serverTimestamp()
      });

      t.set(merchantTxRef,{
        userId: merchantUid,
        type: "merchant_sale_rejected",
        amount: total,
        status: "failed",
        orderId,
        buyerId: order.buyerUid,
        productId: order.productId,
        productName: order.productName || "Produit",
        quantity: qty,
        createdAt: admin.firestore.FieldValue.serverTimestamp()
      });
    });

    return res.json({success:true});
  }catch(e){
    console.log("REJECT ORDER ERROR:", e);
    return res.status(500).json({error:e.message});
  }
});



app.post("/api/merchant/complete-order", verifyFirebaseToken, async (req,res)=>{
  try{
    const merchantUid = req.user.uid;
    const { orderId } = req.body;

    const orderRef = db.collection("merchantOrders").doc(orderId);

    await db.runTransaction(async (t)=>{
      const orderSnap = await t.get(orderRef);

      if(!orderSnap.exists){
        throw new Error("Commande introuvable");
      }

      const order = orderSnap.data() || {};

      if(order.merchantUid !== merchantUid){
        throw new Error("Accès refusé");
      }

      if(order.status === "completed"){
        throw new Error("Commande déjà terminée");
      }

      const merchantRef = db.collection("users").doc(merchantUid);
      const buyerRef = db.collection("users").doc(order.buyerUid);
      const adminRef = db.collection("users").doc(ADMIN_UID);
      const statsRef = db.collection("adminStats").doc("finance"); // 👈 DÉCLARATION ICI

      // 🔥 TOUTES LES LECTURES EN PREMIER 🔥
      const merchantSnap = await t.get(merchantRef);
      const buyerSnap = await t.get(buyerRef);
      const adminSnap = await t.get(adminRef);
      const statsDoc = await t.get(statsRef); // 👈 LECTURE DÉPLACÉE ICI EN HAUT

      if(!merchantSnap.exists) throw new Error("Marchand introuvable");
      if(!buyerSnap.exists) throw new Error("Acheteur introuvable");
      if(!adminSnap.exists) throw new Error("Admin introuvable");

      const merchant = merchantSnap.data() || {};
      const buyer = buyerSnap.data() || {};
      const adminData = adminSnap.data() || {};
      const total = Number(order.totalAmount || 0);

      if(
        merchant.merchantEnabled !== true ||
        merchant.merchantStatus !== "active" ||
        isMerchantBlocked(merchant)
      ){
        throw new Error("Accès marchand bloqué");
      }

      if(order.status === "pending"){
        const buyerLocked = Number(buyer.walletLocked || 0);

        if(buyerLocked < total){
          throw new Error("Montant bloqué insuffisant");
        }

        const saleFee = Math.min(MERCHANT_SALE_ADMIN_FEE, total);
        const merchantNet = total - saleFee;

        const newBuyerLocked = buyerLocked - total;

        let newMerchantWallet = Number(merchant.wallet || 0) + merchantNet;
        let newMerchantEarned = Number(merchant.walletEarned || 0) + merchantNet;
        let newAdminWallet = Number(adminData.wallet || 0) + saleFee;
        let newAdminEarned = Number(adminData.walletEarned || 0) + saleFee;

        // 🔥 TOUTES LES ÉCRITURES ENSUITE 🔥
        t.update(buyerRef,{
          walletLocked: newBuyerLocked
        });

        if(merchantUid === ADMIN_UID){
          newMerchantWallet = Number(adminData.wallet || 0) + total;
          newMerchantEarned = Number(adminData.walletEarned || 0) + total;
          newAdminWallet = newMerchantWallet;
          newAdminEarned = newMerchantEarned;

          t.update(adminRef,{
            wallet: newAdminWallet,
            walletEarned: newAdminEarned
          });
        }else{
          t.update(merchantRef,{
            wallet: newMerchantWallet,
            walletEarned: newMerchantEarned
          });

          t.update(adminRef,{
            wallet: newAdminWallet,
            walletEarned: newAdminEarned
          });
        }

        const merchantTxRef = db.collection("walletTransactions").doc();
        const adminTxRef = db.collection("walletTransactions").doc();
        const buyerTxRef = db.collection("walletTransactions").doc();

        t.set(merchantTxRef,{
          userId: merchantUid,
          type: "merchant_sale_received",
          amount: merchantNet,
          feeAmount: saleFee,
          grossAmount: total,
          status: "completed",
          orderId,
          buyerId: order.buyerUid,
          productId: order.productId,
          productName: order.productName || "Produit",
          quantity: Number(order.quantity || 0),
          balanceAfter: newMerchantWallet,
          createdAt: admin.firestore.FieldValue.serverTimestamp()
        });

        t.set(adminTxRef,{
          userId: ADMIN_UID,
          type: "merchant_sale_fee",
          amount: saleFee,
          status: "completed",
          orderId,
          merchantUid,
          productId: order.productId,
          productName: order.productName || "Produit",
          balanceAfter: newAdminWallet,
          createdAt: admin.firestore.FieldValue.serverTimestamp()
        });

        // 🔥 MISE À JOUR ADMIN STATS (SANS LECTURE ICI) 🔥
        let stats = statsDoc.exists ? statsDoc.data() : {
          totalIn: 0,
          totalOut: 0,
          netTotal: 0,
          transactionsCount: 0,
          typeTotals: {}
        };

        const amountValue = saleFee;
        const type = "merchant_sale_fee";

        if(!stats.typeTotals[type]){
          stats.typeTotals[type] = { in:0, out:0, count:0 };
        }

        stats.totalIn += amountValue;
        stats.transactionsCount += 1;
        stats.typeTotals[type].in += amountValue;
        stats.typeTotals[type].count += 1;
        stats.netTotal = stats.totalIn - stats.totalOut;

        t.set(statsRef, stats);

        t.set(buyerTxRef,{
          userId: order.buyerUid,
          type: "merchant_order_paid",
          amount: total,
          status: "completed",
          orderId,
          merchantUid,
          productId: order.productId,
          productName: order.productName || "Produit",
          quantity: Number(order.quantity || 0),
          balanceAfter: Number(buyer.wallet || 0),
          walletLockedAfter: newBuyerLocked,
          createdAt: admin.firestore.FieldValue.serverTimestamp()
        });
      }

      t.update(orderRef,{
        status: "completed",
        completedAt: admin.firestore.FieldValue.serverTimestamp()
      });
    });

    return res.json({success:true});
  }catch(e){
    console.log("COMPLETE ORDER ERROR:", e);
    return res.status(500).json({error:e.message});
  }
});



app.post("/api/merchant/renew-access", verifyFirebaseToken, async (req,res)=>{
  try{
    const uid = req.user.uid;
    const userRef = db.collection("users").doc(uid);
    const adminRef = db.collection("users").doc(ADMIN_UID);

    await db.runTransaction(async (t)=>{
      const userSnap = await t.get(userRef);
      const adminSnap = await t.get(adminRef);

      if(!userSnap.exists) throw new Error("Utilisateur introuvable");
      if(!adminSnap.exists) throw new Error("Admin introuvable");

      const user = userSnap.data() || {};
      const adminData = adminSnap.data() || {};

      if(!user.merchantReferrerUid){
        throw new Error("Parrain introuvable");
      }

      const referrerRef = db.collection("users").doc(user.merchantReferrerUid);
      const referrerSnap = await t.get(referrerRef);

      if(!referrerSnap.exists){
        throw new Error("Parrain introuvable");
      }

      const referrerData = referrerSnap.data() || {};
const { wallet, available } = getMoneyState(user);

if(available < MERCHANT_RENEWAL_AMOUNT){
  throw new Error("Solde disponible insuffisant");
}

const newUserWallet = wallet - MERCHANT_RENEWAL_AMOUNT;

      const newReferrerWallet = Number(referrerData.wallet || 0) + MERCHANT_RENEWAL_REFERRER_REWARD;
      const newReferrerEarned = Number(referrerData.walletEarned || 0) + MERCHANT_RENEWAL_REFERRER_REWARD;
      const newAdminWallet = Number(adminData.wallet || 0) + MERCHANT_RENEWAL_ADMIN_FEE;
      const newAdminEarned = Number(adminData.walletEarned || 0) + MERCHANT_RENEWAL_ADMIN_FEE;
      const expiresAt = addDaysTimestamp(MERCHANT_RENEWAL_DAYS);

      t.update(userRef,{
        wallet: newUserWallet,
        merchantEnabled: true,
        merchantStatus: "active",
        merchantRenewalBlocked: false,
        merchantExpiresAt: expiresAt,
        merchantLastRenewedAt: admin.firestore.FieldValue.serverTimestamp(),
        merchantRenewalAmount: MERCHANT_RENEWAL_AMOUNT,
        merchantRenewalDays: MERCHANT_RENEWAL_DAYS
      });

      t.update(referrerRef,{
        wallet: newReferrerWallet,
        walletEarned: newReferrerEarned
      });

      t.update(adminRef,{
        wallet: newAdminWallet,
        walletEarned: newAdminEarned
      });

      const userTxRef = db.collection("walletTransactions").doc();
      const refTxRef = db.collection("walletTransactions").doc();
      const adminTxRef = db.collection("walletTransactions").doc();

      t.set(userTxRef,{
        userId: uid,
        type: "merchant_renewal_payment",
        amount: MERCHANT_RENEWAL_AMOUNT,
        status: "completed",
        balanceAfter: newUserWallet,
        createdAt: admin.firestore.FieldValue.serverTimestamp()
      });

      t.set(refTxRef,{
        userId: user.merchantReferrerUid,
        type: "merchant_renewal_referral_reward",
        amount: MERCHANT_RENEWAL_REFERRER_REWARD,
        status: "completed",
        merchantUid: uid,
        balanceAfter: newReferrerWallet,
        createdAt: admin.firestore.FieldValue.serverTimestamp()
      });

      t.set(adminTxRef,{
        userId: ADMIN_UID,
        type: "merchant_renewal_admin_fee",
        amount: MERCHANT_RENEWAL_ADMIN_FEE,
        status: "completed",
        merchantUid: uid,
        balanceAfter: newAdminWallet,
        createdAt: admin.firestore.FieldValue.serverTimestamp()
      });
    });

    return res.json({success:true});
  }catch(e){
    console.log("MERCHANT RENEW ERROR:", e);
    return res.status(500).json({error:e.message});
  }
});


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
    });

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



app.post("/api/compress-video", compressLimiter, verifyFirebaseToken, upload.single("video"), async (req, res) => {
  let inputPath = null;
  let outputPath = null;
  let framePath = null; // Ajout du chemin pour l'image extraite

  try {
    if (!req.file) {
      return res.status(400).json({ error: "Aucune vidéo reçue" });
    }

    inputPath = req.file.path;
    outputPath = `uploads/compressed_${Date.now()}.mp4`;
    framePath = `uploads/frame_${Date.now()}.jpg`;

    let modStatus = "clean"; // Par défaut, la vidéo est considérée propre

    // =========================================================================
    // 🧠 1️⃣ IA MODÉRATION : Extraction d'une image et analyse locale (NSFWJS)
    // =========================================================================
    try {
      console.log("🤖 [IA] Extraction d'une image pour analyse...");
      
      await new Promise((resolve, reject) => {
        ffmpeg(inputPath)
          .screenshots({
            timestamps: ['50%'], 
            filename: framePath.split('/').pop(),
            folder: 'uploads/',
            size: '320x240' // Résolution basse = analyse très rapide
          })
          .on("end", resolve)
          .on("error", reject);
      });

      console.log("🤖 [IA] Analyse locale de l'image par NSFWJS...");

      if (nsfwModel && fs.existsSync(framePath)) {
        
        const imageBuffer = fs.readFileSync(framePath);
        const image = await tf.node.decodeImage(imageBuffer, 3);
        
        const predictions = await nsfwModel.classify(image);
        
        // ⚠️ CRUCIAL : Libérer la mémoire RAM immédiatement
        image.dispose(); 

        predictions.forEach(p => {
          // Détection de nudité/contenu suggestif > 60%
          if ((p.className === 'Porn' || p.className === 'Hentai' || p.className === 'Sexy') && p.probability > 0.6) {
            modStatus = "flagged";
          }
        });

        if (modStatus === "flagged") {
          console.log("🚫 [IA] Contenu inapproprié détecté, mise en quarantaine ! Détails :", predictions);
        } else {
          console.log("✅ [IA] Vidéo propre.");
        }

      } else {
        console.log("⚠️ [IA] Modèle non prêt, la vidéo passe en clean par sécurité.");
      }

    } catch (aiError) {
      console.error("⚠️ [IA] Erreur d'analyse (la vidéo passe en clean) :", aiError.message);
    } finally {
      // Nettoyage immédiat de l'image extraite
      if (fs.existsSync(framePath)) fs.unlinkSync(framePath);
    }
    // =========================================================================

    // 2️⃣ COMPRESSION VIDÉO EXTRÊME
    await new Promise((resolve, reject) => {
      ffmpeg(inputPath)
        .outputOptions([
          "-vf scale='min(480,iw)':-2",
          "-r 24",
          "-c:v libx264",
          "-preset ultrafast",
          "-crf 30",
          "-threads 0",
          "-c:a aac",
          "-b:a 64k",
          "-movflags +faststart",
          "-max_muxing_queue_size 1024"
        ])
        .save(outputPath)
        .on("end", resolve)
        .on("error", (err) => {
          console.error("FFMPEG FATAL ERROR:", err);
          reject(err);
        });
    });

    // 3️⃣ RENVOI AU FRONTEND AVEC LE STATUT IA DANS LES HEADERS
    res.set('Access-Control-Expose-Headers', 'X-Moderation-Status'); 
    res.set('X-Moderation-Status', modStatus);

    res.download(outputPath, "hayticlips_compressed.mp4", () => {
      if (inputPath && fs.existsSync(inputPath)) fs.unlinkSync(inputPath);
      if (outputPath && fs.existsSync(outputPath)) fs.unlinkSync(outputPath);
    });

  } catch (e) {
    console.log("COMPRESS VIDEO ERROR:", e);

    if (inputPath && fs.existsSync(inputPath)) fs.unlinkSync(inputPath);
    if (outputPath && fs.existsSync(outputPath)) fs.unlinkSync(outputPath);
    if (framePath && fs.existsSync(framePath)) fs.unlinkSync(framePath); // Nettoyage de sécurité

    res.status(500).json({ error: "Compression impossible ou délai dépassé." });
  }
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


/* =======================================================
   TRANSFERTS MANUELS (RECHARGE & RETRAIT P2P)
======================================================= */

// 1. SOUMISSION PAR L'UTILISATEUR (Utilisation de uploadMemory !)
app.post("/api/manual-transfer", verifyFirebaseToken, uploadMemory.single("receipt"), async (req, res) => {
  try {
    const uid = req.user.uid;
    const body = req.body; 
    const type = body.type;

    const userRef = db.collection("users").doc(uid);

    // --- LOGIQUE DE RETRAIT (AVEC SÉCURITÉ PIN) ---
    if (type === "withdraw") {
      const amountReq = Number(body.amountRequested);
      const pin = body.pin; // 👈 1. Récupération du PIN

      // Vérification rapide du format
      if (!/^\d{6}$/.test(pin || "")) {
        return res.status(400).json({ error: "Code PIN invalide. Il doit contenir 6 chiffres." });
      }
      
      await db.runTransaction(async (t) => {
        const userSnap = await t.get(userRef);
        if (!userSnap.exists) throw new Error("Utilisateur introuvable");
        
        const userData = userSnap.data();

        // 👈 2. VÉRIFICATION BCRYPT DU PIN
        if (!userData.security || !userData.security.pinHash) {
          throw new Error("Veuillez d'abord configurer un code PIN dans vos paramètres de sécurité.");
        }
        
        const isValidPin = await bcrypt.compare(pin, userData.security.pinHash);
        if (!isValidPin) {
          throw new Error("Code PIN incorrect. Opération refusée.");
        }

        // 3. Suite si le PIN est bon
        const wallet = Number(userData.wallet || 0);
        const locked = Number(userData.walletLocked || 0);
        const available = Math.max(0, wallet - locked);

        if (available < amountReq) {
          throw new Error("Solde disponible insuffisant");
        }

        // Bloquer l'argent
        t.update(userRef, {
          walletLocked: locked + amountReq
        });

        const transferRef = db.collection("manualTransfers").doc();
        t.set(transferRef, {
          userId: uid,
          type: "withdraw",
          method: body.method,
          amountRequested: amountReq,
          feeApplied: Number(body.feeApplied),
          amountToReceive: Number(body.amountToReceive),
          phone: body.phone,
          accountName: body.accountName,
          status: "pending",
          createdAt: admin.firestore.FieldValue.serverTimestamp()
        });
      });

      return res.json({ success: true });

    } 
    // --- LOGIQUE DE RECHARGE (AVEC SUPABASE VIA MÉMOIRE) ---
    else if (type === "deposit") {
      let receiptUrl = "";
      let uploadedFileName = "";

      if (req.file) {
        // Nettoyer le nom du fichier pour éviter les bugs
        const safeName = req.file.originalname.replace(/[^a-zA-Z0-9_.-]/g, '');
        const fileName = `${uid}_${Date.now()}_${safeName}`;
        uploadedFileName = fileName;

        // 👈 UPLOAD DIRECT DEPUIS LA MÉMOIRE (req.file.buffer)
        const { data, error } = await supabase.storage
          .from('media') 
          .upload(fileName, req.file.buffer, {
            contentType: req.file.mimetype,
            upsert: false
          });

        if (error) {
          throw new Error("Erreur Supabase: " + error.message);
        }

        // Récupérer le lien public
        const { data: publicUrlData } = supabase.storage
          .from('media')
          .getPublicUrl(fileName);

        receiptUrl = publicUrlData.publicUrl;
      } else {
        throw new Error("Capture d'écran obligatoire");
      }

      await db.collection("manualTransfers").add({
        userId: uid,
        type: "deposit",
        method: body.method,
        amount: Number(body.amount),
        phone: body.phone,
        receiptUrl: receiptUrl,
        receiptFileName: uploadedFileName, 
        status: "pending",
        createdAt: admin.firestore.FieldValue.serverTimestamp()
      });

      return res.json({ success: true });
    }

  } catch (e) {
    console.log("MANUAL TRANSFER REQUEST ERROR:", e);
    return res.status(500).json({ error: e.message });
  }
});

// 2. VALIDATION / REFUS PAR L'ADMIN
app.post("/api/admin/process-manual-transfer", verifyFirebaseToken, async (req, res) => {
  try {
    const adminUid = req.user.uid;
    const { transferId, status, reason } = req.body;

    const adminRef = db.collection("users").doc(adminUid);
    const adminSnap = await adminRef.get();
    if (!adminSnap.exists || adminSnap.data().role !== "admin") {
      return res.status(403).json({ error: "Accès refusé" });
    }

    const transferRef = db.collection("manualTransfers").doc(transferId);
    let imageToDelete = null; 

    await db.runTransaction(async (t) => {
      const transferSnap = await t.get(transferRef);
      if (!transferSnap.exists) throw new Error("Transfert introuvable");

      const transferData = transferSnap.data();
      if (transferData.status !== "pending") {
        throw new Error("Ce transfert a déjà été traité");
      }

      const userRef = db.collection("users").doc(transferData.userId);
      const userSnap = await t.get(userRef);
      if (!userSnap.exists) throw new Error("Utilisateur introuvable");
      
      const userData = userSnap.data();
      const wallet = Number(userData.wallet || 0);
      const locked = Number(userData.walletLocked || 0);

      // --- LOGIQUE RETRAIT ---
      if (transferData.type === "withdraw") {
        const amountReq = Number(transferData.amountRequested);
        
        if (status === "approved") {
          t.update(userRef, {
            wallet: Math.max(0, wallet - amountReq),
            walletLocked: Math.max(0, locked - amountReq)
          });
          
          const txRef = db.collection("walletTransactions").doc();
          t.set(txRef, {
            userId: transferData.userId,
            type: "manual_withdraw_approved",
            amount: amountReq,
            status: "completed",
            balanceAfter: Math.max(0, wallet - amountReq),
            createdAt: admin.firestore.FieldValue.serverTimestamp()
          });

        } else if (status === "rejected") {
          t.update(userRef, {
            walletLocked: Math.max(0, locked - amountReq)
          });
          
          const notifRef = db.collection("notifications").doc();
          t.set(notifRef, {
            to: transferData.userId,
            from: adminUid,
            type: "transfer_rejected",
            message: `Votre demande de retrait de ${amountReq} HTG a été refusée. Motif : ${reason}`,
            read: false,
            createdAt: admin.firestore.FieldValue.serverTimestamp()
          });
        }
      } 
      // --- LOGIQUE RECHARGE ---
      else if (transferData.type === "deposit") {
        const amount = Number(transferData.amount);
        
        if (status === "approved") {
          const newWallet = wallet + amount;
          t.update(userRef, { wallet: newWallet });
          
          const txRef = db.collection("walletTransactions").doc();
          t.set(txRef, {
            userId: transferData.userId,
            type: "manual_deposit_approved",
            amount: amount,
            status: "completed",
            balanceAfter: newWallet,
            createdAt: admin.firestore.FieldValue.serverTimestamp()
          });

        } else if (status === "rejected") {
          const notifRef = db.collection("notifications").doc();
          t.set(notifRef, {
            to: transferData.userId,
            from: adminUid,
            type: "transfer_rejected",
            message: `Votre recharge de ${amount} HTG a été refusée. Motif : ${reason}`,
            read: false,
            createdAt: admin.firestore.FieldValue.serverTimestamp()
          });
        }

        if (transferData.receiptFileName) {
          imageToDelete = transferData.receiptFileName;
        } else if (transferData.receiptUrl) {
          const urlParts = transferData.receiptUrl.split('/media/');
          if (urlParts.length > 1) {
            imageToDelete = decodeURIComponent(urlParts[1]);
          }
        }
      }

      t.update(transferRef, {
        status: status,
        processedAt: admin.firestore.FieldValue.serverTimestamp(),
        processedBy: adminUid,
        rejectReason: status === "rejected" ? reason : ""
      });
    });

    if (imageToDelete) {
      try {
        await supabase.storage.from('media').remove([imageToDelete]);
        console.log("✅ Image supprimée de Supabase :", imageToDelete);
      } catch (deleteErr) {
        console.error("❌ Erreur lors de la suppression Supabase :", deleteErr.message);
      }
    }

    return res.json({ success: true });
    
  } catch (e) {
    console.error("ADMIN PROCESS TRANSFER ERROR:", e);
    return res.status(500).json({ error: e.message });
  }
});

const PORT = process.env.PORT || 3000;

app.listen(PORT, "0.0.0.0", () => {
  console.log("HaytiClips backend running on port " + PORT);
});
