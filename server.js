// server.js - VERSION FINALE BUBBLE READY
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

// Rate Limiters
const secondLimiter = rateLimit({ windowMs: 1000, max: 10 });
const hourLimiter = rateLimit({ windowMs: 60 * 60 * 1000, max: 10000 });

// Utilitaire pour attendre
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const MOIS_FR = [
  "Janvier", "Février", "Mars", "Avril", "Mai", "Juin",
  "Juillet", "Août", "Septembre", "Octobre", "Novembre", "Décembre"
];

function getEmptyMonthsStructure() {
  const months = {};
  MOIS_FR.forEach(mois => {
    months[mois] = { total_wh: 0, hp_wh: 0, hc_wh: 0 };
  });
  return months;
}

function aggregateMonths(target, source) {
    Object.keys(source).forEach(mois => {
        if (target[mois] && source[mois]) {
            target[mois].total_wh += source[mois].total_wh;
            target[mois].hp_wh += source[mois].hp_wh;
            target[mois].hc_wh += source[mois].hc_wh;
        }
    });
}

// --- SCHEMAS ---
const baseSchema = Joi.object({
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

// --- RECUPERATION COMPLETE INFOS CLIENT (FORMAT BUBBLE) ---
async function getUserInfoInternal(usage_point_id, providedToken) {
  const token = await getToken(providedToken);
  
  // Initialisation FORCEE avec null pour que Bubble détecte les clés
  let address = { street: null, postal_code: null, city: null, country: null };
  let contract = { offpeak_hours: null, subscribed_power: null };
  let identity = { firstname: null, lastname: null };
  let contact = { email: null, phone: null };

  try {
    const [addrRes, contRes, idRes, contactRes] = await Promise.allSettled([
      axios.get(`${ENEDIS_BASE_URL}/customers_upa/v5/usage_points/addresses`, { headers: { Authorization: `Bearer ${token}` }, params: { usage_point_id }, timeout: 8000 }),
      axios.get(`${ENEDIS_BASE_URL}/customers_upc/v5/usage_points/contracts`, { headers: { Authorization: `Bearer ${token}` }, params: { usage_point_id }, timeout: 8000 }),
      axios.get(`${ENEDIS_BASE_URL}/customers_i/v5/identity`, { headers: { Authorization: `Bearer ${token}` }, params: { usage_point_id }, timeout: 8000 }),
      axios.get(`${ENEDIS_BASE_URL}/customers_cd/v5/contact_data`, { headers: { Authorization: `Bearer ${token}` }, params: { usage_point_id }, timeout: 8000 })
    ]);

    // Remplissage des objets si succès
    if (addrRes.status === 'fulfilled') {
      const d = addrRes.value.data?.customer?.usage_points?.[0]?.usage_point?.address;
      if (d) {
        address.street = d.street || null;
        address.postal_code = d.postal_code || null;
        address.city = d.city || null;
        address.country = d.country || null;
      }
    }

    if (contRes.status === 'fulfilled') {
      const up = contRes.value.data?.customer?.usage_points?.[0];
      if (up && up.contracts) {
        contract.offpeak_hours = up.contracts.offpeak_hours || null;
        contract.subscribed_power = up.contracts.subscribed_power || null;
      }
    }

    if (idRes.status === 'fulfilled') {
      const p = idRes.value.data?.identity?.natural_person;
      if (p) {
        identity.firstname = p.firstname || null;
        identity.lastname = p.lastname || null;
      }
    }

    if (contactRes.status === 'fulfilled') {
      const c = contactRes.value.data?.contact_data;
      if (c) {
        contact.email = c.email || null;
        contact.phone = c.phone || null;
      }
    }

  } catch (e) {
    // En cas d'erreur, on garde les objets initialisés à null
    console.warn(`Info User partielle ou échouée pour ${usage_point_id}: ${e.message}`);
  }

  return { 
      usage_point_id,
      address, 
      contract, 
      identity, 
      contact 
  };
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

  Object.keys(monthly).forEach(m => {
    monthly[m].total_wh = Math.round(monthly[m].total_wh);
    monthly[m].hp_wh = Math.round(monthly[m].hp_wh);
    monthly[m].hc_wh = Math.round(monthly[m].hc_wh);
  });

  const total = Math.round(Object.values(monthly).reduce((s, m) => s + m.total_wh, 0));
  return { total_wh: total, monthly };
}

// =========================================================
// ROUTE 1 : JUSTE LES INFOS UTILISATEURS (Bubble Email Check)
// =========================================================
app.post('/get-user-info', secondLimiter, async (req, res) => {
    const { error } = baseSchema.validate(req.body);
    if (error) return res.status(400).json({ success: false, error: error.details[0].message });
  
    const { usage_point_ids, access_token } = req.body;
    const results = [];
  
    for (const pdl of usage_point_ids) {
        const info = await getUserInfoInternal(pdl, access_token);
        results.push(info);
    }
  
    res.json({
        success: true,
        customers: results
    });
});

// =========================================================
// ROUTE 2 : TOUT (Aggrégation Conso/Prod + Liste Adresses)
// =========================================================
app.post('/get-all', secondLimiter, hourLimiter, async (req, res) => {
  const { error } = baseSchema.validate(req.body);
  if (error) return res.status(400).json({ success: false, error: error.details[0].message });

  const { usage_point_ids, access_token: providedToken } = req.body;
  const startStr = format(subYears(new Date(), 1), 'yyyy-MM-dd');
  const endStr = format(new Date(), 'yyyy-MM-dd');

  const globalConso = {
      total_wh: 0, hp_wh: 0, hc_wh: 0,
      monthly: getEmptyMonthsStructure(),
      offpeak_hours: new Set() 
  };
  const globalProd = {
      total_wh: 0,
      monthly: getEmptyMonthsStructure()
  };
  
  const customersList = [];

  for (const pdl of usage_point_ids) {
    // 1. Infos Client
    const userInfo = await getUserInfoInternal(pdl, providedToken);
    customersList.push(userInfo);

    // 2. Simulation HC si manquantes
    let offpeakStr = userInfo.contract?.offpeak_hours;
    if (!offpeakStr) {
        offpeakStr = "HC (22H00-06H00)";
    }
    globalConso.offpeak_hours.add(offpeakStr);

    // 3. Consommation
    const conso = await fetchMeteringInternal(pdl, providedToken, 'consumption', 'clc', startStr, endStr, offpeakStr);
    
    globalConso.total_wh += conso.total_wh;
    const hp = Object.values(conso.monthly).reduce((s, m) => s + m.hp_wh, 0);
    const hc = Object.values(conso.monthly).reduce((s, m) => s + m.hc_wh, 0);
    globalConso.hp_wh += hp;
    globalConso.hc_wh += hc;
    aggregateMonths(globalConso.monthly, conso.monthly);

    // 4. Production
    let prod;
    try {
        prod = await fetchMeteringInternal(pdl, providedToken, 'production', 'plc', startStr, endStr, '');
    } catch (e) {
        prod = { total_wh: 0, monthly: getEmptyMonthsStructure() };
    }
    if (!prod.monthly || Object.keys(prod.monthly).length === 0) {
        prod.monthly = getEmptyMonthsStructure();
    }
    
    globalProd.total_wh += prod.total_wh;
    aggregateMonths(globalProd.monthly, prod.monthly);
  }

  res.json({
    success: true,
    period: { start: startStr, end: endStr },
    customers_list: customersList,
    global_data: {
        consumption: {
            total_wh: globalConso.total_wh,
            hp_wh: globalConso.hp_wh,
            hc_wh: globalConso.hc_wh,
            offpeak_hours_detected: Array.from(globalConso.offpeak_hours),
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