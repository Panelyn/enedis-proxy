// server.js - VERSION AGGRÉGÉE (CUMUL DES PRM)
require('dotenv').config();
const express = require('express');
const axios = require('axios');
const Joi = require('joi');
const { parseISO, startOfDay, addDays, format, subYears } = require('date-fns');
const cors = require('cors');
const rateLimit = require('express-rate-limit');

const app = express();
app.use(express.json());
app.use(cors());
app.set('trust proxy', 1);

// --- CONFIGURATION ---
const ENEDIS_CLIENT_ID = process.env.ENEDIS_CLIENT_ID;
const ENEDIS_CLIENT_SECRET = process.env.ENEDIS_CLIENT_SECRET;
const ENEDIS_BASE_URL = 'https://gw.ext.prod.api.enedis.fr';
const TOKEN_ENDPOINT = `${ENEDIS_BASE_URL}/oauth2/v3/token`;

let appTokenCache = { token: null, expiresAt: 0 };

const secondLimiter = rateLimit({ windowMs: 1000, max: 10 });
const hourLimiter = rateLimit({ windowMs: 60 * 60 * 1000, max: 10000 });

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Liste des mois fixe
const MOIS_FR = [
  "Janvier", "Février", "Mars", "Avril", "Mai", "Juin",
  "Juillet", "Août", "Septembre", "Octobre", "Novembre", "Décembre"
];

// Fonction pour créer une structure vide de 12 mois
function getEmptyMonthsStructure() {
  const months = {};
  MOIS_FR.forEach(mois => {
    months[mois] = { total_wh: 0, hp_wh: 0, hc_wh: 0 };
  });
  return months;
}

// Fonction pour additionner les données d'un mois source vers une cible
function aggregateMonths(target, source) {
    Object.keys(source).forEach(mois => {
        if (target[mois] && source[mois]) {
            target[mois].total_wh += source[mois].total_wh;
            target[mois].hp_wh += source[mois].hp_wh;
            target[mois].hc_wh += source[mois].hc_wh;
        }
    });
}

// --- SCHEMA VALIDATION ---
const getAllSchema = Joi.object({
  usage_point_ids: Joi.array().items(Joi.string().pattern(/^\d{14}$/)).min(1).required(),
  access_token: Joi.string().optional().allow('')
});

// --- GESTION TOKEN ---
async function getToken(providedToken = null) {
  if (providedToken?.trim()) return providedToken;
  const now = Date.now();
  if (appTokenCache.token && now < appTokenCache.expiresAt) return appTokenCache.token;

  try {
    const response = await axios.post(TOKEN_ENDPOINT,
      new URLSearchParams({ 
        grant_type: 'client_credentials', 
        client_id: ENEDIS_CLIENT_ID, 
        client_secret: ENEDIS_CLIENT_SECRET, 
        scope: 'metering_data metering_data_contract_details' 
      }),
      { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 10000 }
    );
    appTokenCache.token = response.data.access_token;
    appTokenCache.expiresAt = now + (response.data.expires_in - 300) * 1000;
    return appTokenCache.token;
  } catch (error) {
    console.error("Erreur récupération token app:", error.message);
    throw new Error("Impossible d'obtenir un token Enedis");
  }
}

// --- LOGIQUE HEURES CREUSES ---
function parseOffpeakHours(str) {
  if (!str) return [];
  let s = str.toLowerCase()
    .replace(/hc\s*[:=]?\s*/g, '')
    .replace(/h00/g, 'h')
    .replace(/:/g, 'h')
    .trim();
  return s.split(/;|,|et/).map(p => p.trim()).filter(p => p.includes('-'));
}

function isTimeInOffpeak(date, periods) {
  if (!periods || !periods.length) return false;
  const t = date.getHours() * 60 + date.getMinutes();
  
  for (let range of periods) {
    const [startPart, endPart] = range.split('-').map(p => p.trim());
    if (!startPart || !endPart) continue;

    let startH = parseInt(startPart.replace(/\D/g, ''));
    let endH = parseInt(endPart.replace(/\D/g, ''));
    
    const startMin = startH * 60;
    const endMin = endH * 60;

    if (endMin < startMin) {
      if (t >= startMin || t < endMin) return true;
    } else {
      if (t >= startMin && t < endMin) return true;
    }
  }
  return false;
}

// --- RECUPERATION INFOS CLIENT ---
async function getUserInfoInternal(usage_point_id, providedToken) {
  const token = await getToken(providedToken);
  let contract = null;

  try {
    const contRes = await axios.get(`${ENEDIS_BASE_URL}/customers_upc/v5/usage_points/contracts`, { headers: { Authorization: `Bearer ${token}` }, params: { usage_point_id }, timeout: 10000 });
    if (contRes.data?.customer?.usage_points?.[0]) {
      const up = contRes.data.customer.usage_points[0];
      contract = { offpeak_hours: up.contracts?.offpeak_hours };
    }
    return { contract };
  } catch (e) {
    return { contract: null };
  }
}

// --- RECUPERATION COURBES ---
async function fetchMeteringInternal(usage_point_id, providedToken, type, apiSuffix, startStr, endStr, offpeakStr) {
  const token = await getToken(providedToken);
  const allData = [];
  
  let current = parseISO(startStr);
  const finalEnd = parseISO(endStr);

  while (current <= finalEnd) {
    const chunkStart = format(current, 'yyyy-MM-dd');
    let chunkEndDate = addDays(current, 6);
    if (chunkEndDate > finalEnd) chunkEndDate = finalEnd;
    const chunkEnd = format(chunkEndDate, 'yyyy-MM-dd');

    try {
      await sleep(150);
      const res = await axios.get(`${ENEDIS_BASE_URL}/metering_data_${apiSuffix}/v5/${type}_load_curve`, { 
          headers: { Authorization: `Bearer ${token}` }, 
          params: { usage_point_id, start: chunkStart, end: chunkEnd }, 
          timeout: 15000 
      });
      if (res.data?.meter_reading?.interval_reading) {
        allData.push(...res.data.meter_reading.interval_reading);
      }
    } catch (err) {
      if (err.response && (err.response.status === 401 || err.response.status === 403)) break;
    }
    current = startOfDay(addDays(chunkEndDate, 1));
  }

  const monthly = getEmptyMonthsStructure();
  const periods = parseOffpeakHours(offpeakStr);

  allData.forEach(entry => {
    if (!entry.date || !entry.value) return;
    const date = parseISO(entry.date);
    const monthIndex = date.getMonth(); 
    const monthName = MOIS_FR[monthIndex];
    
    const wh = parseFloat(entry.value) * 0.5;
    
    monthly[monthName].total_wh += wh;
    if (isTimeInOffpeak(date, periods)) monthly[monthName].hc_wh += wh;
    else monthly[monthName].hp_wh += wh;
  });

  // Arrondis
  Object.keys(monthly).forEach(m => {
    monthly[m].total_wh = Math.round(monthly[m].total_wh);
    monthly[m].hp_wh = Math.round(monthly[m].hp_wh);
    monthly[m].hc_wh = Math.round(monthly[m].hc_wh);
  });

  const total = Math.round(Object.values(monthly).reduce((s, m) => s + m.total_wh, 0));
  return { total_wh: total, monthly };
}

// --- ROUTE PRINCIPALE ---
app.post('/get-all', secondLimiter, hourLimiter, async (req, res) => {
  const { error } = getAllSchema.validate(req.body);
  if (error) return res.status(400).json({ success: false, error: error.details[0].message });

  const { usage_point_ids, access_token: providedToken } = req.body;
  const startStr = format(subYears(new Date(), 1), 'yyyy-MM-dd');
  const endStr = format(new Date(), 'yyyy-MM-dd');

  // --- INITIALISATION DES ACCUMULATEURS GLOBAUX ---
  const globalConso = {
      total_wh: 0,
      hp_wh: 0,
      hc_wh: 0,
      monthly: getEmptyMonthsStructure(),
      offpeak_hours: new Set() // Pour lister toutes les plages uniques rencontrées
  };

  const globalProd = {
      total_wh: 0,
      monthly: getEmptyMonthsStructure()
  };

  // --- BOUCLE SUR LES PRM ---
  for (const pdl of usage_point_ids) {
    // 1. Infos Client & Contrat
    const userInfo = await getUserInfoInternal(pdl, providedToken);
    
    let offpeakStr = userInfo.contract?.offpeak_hours;
    if (!offpeakStr) {
        offpeakStr = "HC (22H00-06H00)"; // Valeur par défaut si API contrat bloque
    }

    // Ajout de la plage horaire à la liste globale
    globalConso.offpeak_hours.add(offpeakStr);

    // 2. Consommation
    const conso = await fetchMeteringInternal(pdl, providedToken, 'consumption', 'clc', startStr, endStr, offpeakStr);
    
    // --> AGGREGATION CONSOMMATION
    globalConso.total_wh += conso.total_wh;
    // On recalcule les totaux HP/HC depuis les mois pour être précis
    const hp = Object.values(conso.monthly).reduce((s, m) => s + m.hp_wh, 0);
    const hc = Object.values(conso.monthly).reduce((s, m) => s + m.hc_wh, 0);
    globalConso.hp_wh += hp;
    globalConso.hc_wh += hc;
    
    // Fusion des mois (Janvier PRM1 + Janvier PRM2...)
    aggregateMonths(globalConso.monthly, conso.monthly);

    // 3. Production
    let prod;
    try {
        prod = await fetchMeteringInternal(pdl, providedToken, 'production', 'plc', startStr, endStr, '');
    } catch (e) {
        prod = { total_wh: 0, monthly: getEmptyMonthsStructure() };
    }
    if (!prod.monthly || Object.keys(prod.monthly).length === 0) {
        prod.monthly = getEmptyMonthsStructure();
    }

    // --> AGGREGATION PRODUCTION
    globalProd.total_wh += prod.total_wh;
    aggregateMonths(globalProd.monthly, prod.monthly);
  }

  // Conversion du Set d'heures creuses en tableau pour le JSON
  const allOffpeakHours = Array.from(globalConso.offpeak_hours);

  // Construction de la réponse simplifiée
  res.json({
    success: true,
    period: { start: startStr, end: endStr },
    // On renvoie directement l'objet global
    global_data: {
        consumption: {
            total_wh: globalConso.total_wh,
            hp_wh: globalConso.hp_wh,
            hc_wh: globalConso.hc_wh,
            offpeak_hours_detected: allOffpeakHours,
            monthly: globalConso.monthly
        },
        production: {
            total_wh: globalProd.total_wh,
            monthly: globalProd.monthly
        }
    }
  });
});

app.get('/health', (req, res) => res.json({ status: 'OK' }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Serveur prêt sur port ${PORT}`));