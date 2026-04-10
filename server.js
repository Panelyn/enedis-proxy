// server.js - VERSION V6 (2026) + PREREQUIS + LOGS ERREURS VISIBLES
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
const MOIS_FR = ["Janvier", "Février", "Mars", "Avril", "Mai", "Juin", "Juillet", "Août", "Septembre", "Octobre", "Novembre", "Décembre"];

function getEmptyMonthsStructure() {
  const months = {};
  MOIS_FR.forEach(mois => months[mois] = { total_kwh: 0, hp_kwh: 0, hc_kwh: 0, correction_ratio: 1 });
  return months;
}

function aggregateMonths(target, source) {
    Object.keys(source).forEach(mois => {
        if (target[mois] && source[mois]) {
            target[mois].total_kwh += source[mois].total_kwh;
            target[mois].hp_kwh += source[mois].hp_kwh;
            target[mois].hc_kwh += source[mois].hc_kwh;
        }
    });
}

const baseSchema = Joi.object({
  usage_point_ids: Joi.array().items(Joi.string().pattern(/^\d{14}$/)).min(1).required(),
  access_token: Joi.string().optional().allow('')
});

// --- GESTION TOKEN ---
async function getToken(providedToken = null) {
  if (providedToken && providedToken.trim() !== '') return providedToken;
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
  let s = str.toLowerCase().replace(/hc\s*[:=]?\s*/g, '').replace(/[()]/g, '').replace(/h/g, ':').trim();
  return s.split(/;|,|et/).map(p => p.trim()).filter(p => p.includes('-'));
}

function isTimeInOffpeak(date, periods) {
  if (!periods || !periods.length) return false;
  const currentTotalMinutes = date.getHours() * 60 + date.getMinutes();
  for (let range of periods) {
    const [startStr, endStr] = range.split('-').map(p => p.trim());
    if (!startStr || !endStr) continue;
    const toMinutes = (timeStr) => {
      const parts = timeStr.split(':');
      return (parseInt(parts[0], 10) || 0) * 60 + (parseInt(parts[1], 10) || 0);
    };
    const startMin = toMinutes(startStr);
    const endMin = toMinutes(endStr);

    if (endMin < startMin) {
      if (currentTotalMinutes >= startMin || currentTotalMinutes < endMin) return true;
    } else {
      if (currentTotalMinutes >= startMin && currentTotalMinutes < endMin) return true;
    }
  }
  return false;
}

// --- NOUVEAU PREREQUIS V6: SUBSCRIBED SERVICES ---
async function callSubscribedServices(usage_point_id, token) {
    try {
        const config = { 
            headers: { 'Authorization': `Bearer ${token}`, 'Accept': 'application/json', 'Content-Type': 'application/json' },
            timeout: 10000 
        };
        // L'API attend l'identifiant de l'autorisation
        const body = { usage_point_id: usage_point_id }; 
        await axios.post(`${ENEDIS_BASE_URL}/subscribed_services/v1`, body, config);
        console.log(`✅ [V6] Prérequis subscribed_services OK pour ${usage_point_id}`);
    } catch (e) {
        console.error(`⚠️ [V6] Erreur subscribed_services (${usage_point_id}):`, e.response?.status, JSON.stringify(e.response?.data || e.message));
    }
}

// --- RECUPERATION INFOS CLIENT (API V6) ---
async function getUserInfoInternal(usage_point_id, providedToken) {
  const token = await getToken(providedToken);
  
  let address = { street: null, postal_code: null, city: null, country: null };
  let contract = { offpeak_hours: null, offpeak_hours_list: [], subscribed_power: null };
  let identity = { firstname: null, lastname: null };
  let contact = { email: null, phone: null };

  const config = { 
    headers: { 'Authorization': `Bearer ${token}`, 'Accept': 'application/json' }, 
    params: { usage_point_id }, 
    timeout: 10000 
  };

  try {
    const [genRes, contratRes] = await Promise.allSettled([
      axios.get(`${ENEDIS_BASE_URL}/donnees_generales_auto/v1`, config),
      axios.get(`${ENEDIS_BASE_URL}/situation_contrat_auto/v1`, config)
    ]);

    // 1. DONNEES GENERALES (Adresse + Contact)
    if (genRes.status === 'fulfilled') {
      const data = genRes.value.data;
      console.log(`🔵 [V6] DONNEES GENERALES REÇUES:`, JSON.stringify(data, null, 2));
      
      const up = data.customer?.usage_points?.[0]?.usage_point || data.usage_point || data;
      const d = up?.usage_point_addresses || up?.address || data.adresse; 
      if (d) {
        address.street = d.street || d.rue || null;
        address.postal_code = d.postal_code || d.code_postal || null;
        address.city = d.city || d.ville || null;
        address.country = d.country || d.pays || null;
      }
      
      const idData = data.identity || data.identite || data.customer?.identity;
      if (idData?.natural_person || idData?.personne_physique) {
        const np = idData.natural_person || idData.personne_physique;
        identity.firstname = np.firstname || np.prenom || null;
        identity.lastname = np.lastname || np.nom || null;
      }
      
      const contactData = data.contact_data || data.contact || data.customer?.contact_data;
      if (contactData) {
        contact.email = contactData.email || null;
        contact.phone = contactData.phone || contactData.telephone || null;
      }
    } else {
      console.error(`🔴 [V6] DONNEES GENERALES FAILED:`, genRes.reason?.response?.status, JSON.stringify(genRes.reason?.response?.data || genRes.reason?.message));
    }

    // 2. CONTRAT
    if (contratRes.status === 'fulfilled') {
      const data = contratRes.value.data;
      const up = data.customer?.usage_points?.[0] || data.usage_point || data;
      const ctr = up?.contracts || up?.usage_point?.contracts || data.contrat;
      if (ctr) {
        let rawHc = ctr.offpeak_hours || ctr.heures_creuses || "";
        let cleanHc = rawHc.replace(/HC\s*[:=]?\s*/gi, '').replace(/[()]/g, '').trim();
        contract.offpeak_hours = cleanHc || null;
        contract.offpeak_hours_list = cleanHc ? cleanHc.split(';').map(s => s.trim()) : [];
        contract.subscribed_power = ctr.subscribed_power || ctr.puissance_souscrite || null;
      }
    } else {
      console.error(`🔴 [V6] CONTRAT FAILED:`, contratRes.reason?.response?.status, JSON.stringify(contratRes.reason?.response?.data || contratRes.reason?.message));
    }

  } catch (e) {
    console.error(`Erreur fatale UserInfo V6: ${e.message}`);
  }

  return { usage_point_id, address, contract, identity, contact };
}

// --- RECUPERATION DONNEES QUOTIDIENNES (V6) ---
async function fetchDailyTotals(usage_point_id, token, type, startStr, endStr) {
    const apiType = type === 'consumption' ? 'daily_consumption' : 'daily_production';
    
    try {
        const res = await axios.get(`${ENEDIS_BASE_URL}/metering_data/${apiType}`, { 
            headers: { Authorization: `Bearer ${token}`, 'Accept': 'application/json' },
            params: { usage_point_id, start: startStr, end: endStr }, 
            timeout: 10000 
        });

        const dailyData = getEmptyMonthsStructure();
        const intervals = res.data?.meter_reading?.interval_reading || [];

        intervals.forEach(entry => {
            if (!entry.date || !entry.value) return;
            const date = parseISO(entry.date);
            const monthName = MOIS_FR[date.getMonth()];
            dailyData[monthName].total_kwh += parseFloat(entry.value) / 1000;
        });
        
        return dailyData;

    } catch (e) {
        console.error(`🔴 [V6] DAILY FAILED (${type}):`, e.response?.status, JSON.stringify(e.response?.data || e.message));
        return null; 
    }
}

// --- RECUPERATION COURBES + CALIBRATION (V6) ---
async function fetchMeteringInternal(usage_point_id, providedToken, type, startStr, endStr, offpeakStr) {
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
      const res = await axios.get(`${ENEDIS_BASE_URL}/metering_data/${type}_load_curve`, { 
          headers: { Authorization: `Bearer ${token}`, 'Accept': 'application/json' },
          params: { usage_point_id, start: chunkStart, end: chunkEnd }, 
          timeout: 15000 
      });
      if (res.data?.meter_reading?.interval_reading) {
        allData.push(...res.data.meter_reading.interval_reading);
      }
    } catch (err) {
      console.error(`🔴 [V6] LOAD CURVE FAILED (${type}):`, err.response?.status, JSON.stringify(err.response?.data || err.message));
      if (err.response && (err.response.status === 401 || err.response.status === 403)) break;
    }
    current = startOfDay(addDays(chunkEndDate, 1));
  }

  const monthly = getEmptyMonthsStructure();
  const periods = parseOffpeakHours(offpeakStr);

  allData.forEach(entry => {
    if (!entry.date || !entry.value) return;
    const date = parseISO(entry.date);
    const monthName = MOIS_FR[date.getMonth()];
    
    const kwh = (parseFloat(entry.value) * 0.5) / 1000;
    
    monthly[monthName].total_kwh += kwh;
    if (isTimeInOffpeak(date, periods)) monthly[monthName].hc_kwh += kwh;
    else monthly[monthName].hp_kwh += kwh;
  });

  const officialMonthly = await fetchDailyTotals(usage_point_id, token, type, startStr, endStr);

  if (officialMonthly) {
      Object.keys(monthly).forEach(m => {
          const calculatedTotal = monthly[m].total_kwh;
          const officialTotal = officialMonthly[m].total_kwh;

          if (calculatedTotal > 0 && officialTotal > 0) {
              const ratio = officialTotal / calculatedTotal;
              monthly[m].hp_kwh = monthly[m].hp_kwh * ratio;
              monthly[m].hc_kwh = monthly[m].hc_kwh * ratio;
              monthly[m].total_kwh = officialTotal; 
              monthly[m].correction_ratio = ratio; 
          }
      });
  }

  Object.keys(monthly).forEach(m => {
    monthly[m].total_kwh = Number(monthly[m].total_kwh.toFixed(2));
    monthly[m].hp_kwh = Number(monthly[m].hp_kwh.toFixed(2));
    monthly[m].hc_kwh = Number(monthly[m].hc_kwh.toFixed(2));
  });

  const total = Number(Object.values(monthly).reduce((s, m) => s + m.total_kwh, 0).toFixed(2));
  return { total_kwh: total, monthly };
}

// =========================================================
// ROUTES
// =========================================================
app.post('/get-user-info', secondLimiter, async (req, res) => {
    const { error } = baseSchema.validate(req.body);
    if (error) return res.status(400).json({ success: false, error: error.details[0].message });
    const { usage_point_ids, access_token } = req.body;
    const results = [];
    
    for (const pdl of usage_point_ids) {
        await callSubscribedServices(pdl, access_token);
        const info = await getUserInfoInternal(pdl, access_token);
        results.push(info);
    }
    res.json({ success: true, customers: results });
});

app.post('/get-all', secondLimiter, hourLimiter, async (req, res) => {
  const { error } = baseSchema.validate(req.body);
  if (error) return res.status(400).json({ success: false, error: error.details[0].message });

  const { usage_point_ids, access_token: providedToken } = req.body;
  const startStr = format(subYears(new Date(), 1), 'yyyy-MM-dd');
  const endStr = format(new Date(), 'yyyy-MM-dd');

  const globalConso = { monthly: getEmptyMonthsStructure(), offpeak_hours: new Set() };
  const globalProd = { monthly: getEmptyMonthsStructure() };
  
  const customersList = [];

  for (const pdl of usage_point_ids) {
    // 1. Prérequis V6 OBLIGATOIRE
    await callSubscribedServices(pdl, providedToken);

    const userInfo = await getUserInfoInternal(pdl, providedToken);
    customersList.push(userInfo);

    let offpeakStr = userInfo.contract?.offpeak_hours;
    if (!offpeakStr) offpeakStr = "HC (22H00-06H00)";
    globalConso.offpeak_hours.add(offpeakStr);

    const conso = await fetchMeteringInternal(pdl, providedToken, 'consumption', startStr, endStr, offpeakStr);
    aggregateMonths(globalConso.monthly, conso.monthly);

    let prod;
    try {
        prod = await fetchMeteringInternal(pdl, providedToken, 'production', startStr, endStr, '');
    } catch (e) {
        prod = { total_kwh: 0, monthly: getEmptyMonthsStructure() };
    }
    aggregateMonths(globalProd.monthly, prod.monthly);
  }

  const consoHpList = MOIS_FR.map(m => Number(globalConso.monthly[m].hp_kwh.toFixed(2)));
  const consoHcList = MOIS_FR.map(m => Number(globalConso.monthly[m].hc_kwh.toFixed(2)));
  const prodList = MOIS_FR.map(m => Number((globalProd.monthly[m].total_kwh * -1).toFixed(2)));

  const totalHp = Number(consoHpList.reduce((a, b) => a + b, 0).toFixed(2));
  const totalHc = Number(consoHcList.reduce((a, b) => a + b, 0).toFixed(2));
  const totalProd = Number(prodList.reduce((a, b) => a + b, 0).toFixed(2));

  res.json({
    success: true,
    period: { start: startStr, end: endStr },
    customers_list: customersList,
    global_data: {
        totals_kwh: {
            consumption: Number((totalHp + totalHc).toFixed(2)),
            production: totalProd,
            hp: totalHp,
            hc: totalHc
        },
        lists_kwh: {
            consumption_hp: consoHpList,
            consumption_hc: consoHcList,
            production: prodList
        },
        offpeak_hours_detected: Array.from(globalConso.offpeak_hours)
    }
  });
});

app.get('/callback', (req, res) => {
  const { code, state, usage_point_id, error } = req.query;
  if (error) return res.redirect(`https://panelyn.com/simulateur?error=${error}`);
  res.redirect(`https://panelyn.com/simulateur?consentement=ok&usage_point_id=${usage_point_id || ''}`);
});

app.get('/health', (req, res) => res.json({ status: 'OK' }));
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Serveur prêt sur port ${PORT}`));