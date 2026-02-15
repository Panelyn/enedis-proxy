// server.js - VERSION "RE-CALIBRATION" (Ratio Daily/Curve)
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
    months[mois] = { total_kwh: 0, hp_kwh: 0, hc_kwh: 0, correction_ratio: 1 };
  });
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

// --- SCHEMAS ---
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
  let s = str.toLowerCase()
    .replace(/hc\s*[:=]?\s*/g, '') 
    .replace(/[()]/g, '')          
    .replace(/h/g, ':')            
    .trim();
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
      const h = parseInt(parts[0], 10) || 0;
      const m = parseInt(parts[1], 10) || 0;
      return h * 60 + m;
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

// --- RECUPERATION INFOS CLIENT ---
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
    const [addrRes, contRes, idRes, contactRes] = await Promise.allSettled([
      axios.get(`${ENEDIS_BASE_URL}/customers_upa/v5/usage_points/addresses`, config),
      axios.get(`${ENEDIS_BASE_URL}/customers_upc/v5/usage_points/contracts`, config),
      axios.get(`${ENEDIS_BASE_URL}/customers_i/v5/identity`, config),
      axios.get(`${ENEDIS_BASE_URL}/customers_cd/v5/contact_data`, config)
    ]);

    if (addrRes.status === 'fulfilled') {
      const up = addrRes.value.data?.customer?.usage_points?.[0]?.usage_point;
      const d = up?.usage_point_addresses || up?.address; 
      if (d) {
        address.street = d.street || null;
        address.postal_code = d.postal_code || null;
        address.city = d.city || null;
        address.country = d.country || null;
      }
    }

    if (contRes.status === 'fulfilled') {
      const up = contRes.value.data?.customer?.usage_points?.[0];
      const ctr = up?.contracts || up?.usage_point?.contracts;
      if (ctr) {
        let rawHc = ctr.offpeak_hours || "";
        let cleanHc = rawHc.replace(/HC\s*[:=]?\s*/gi, '').replace(/[()]/g, '').trim();
        contract.offpeak_hours = cleanHc || null;
        contract.offpeak_hours_list = cleanHc ? cleanHc.split(';').map(s => s.trim()) : [];
        contract.subscribed_power = ctr.subscribed_power || null;
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
    console.warn(`Info User partielle: ${e.message}`);
  }

  return { usage_point_id, address, contract, identity, contact };
}

// --- NOUVEAU: RECUPERATION DONNEES QUOTIDIENNES (OFFICIELLES) ---
async function fetchDailyTotals(usage_point_id, token, type, startStr, endStr) {
    // Determine l'URL : daily_consumption ou daily_production
    const apiType = type === 'consumption' ? 'daily_consumption' : 'daily_production';
    const apiSuffix = type === 'consumption' ? 'dc' : 'dp';
    
    // On peut souvent récupérer 1 an d'un coup pour le Daily, mais par sécurité on garde un chunk large (ex: 360 jours)
    // Ici on tente la période entière, si ça fail, Enedis renverra une erreur, mais 1 an passe généralement.
    
    try {
        const res = await axios.get(`${ENEDIS_BASE_URL}/metering_data_${apiSuffix}/v5/${apiType}`, { 
            headers: { Authorization: `Bearer ${token}`, 'Accept': 'application/json' },
            params: { usage_point_id, start: startStr, end: endStr }, 
            timeout: 10000 
        });

        const dailyData = getEmptyMonthsStructure();
        const intervals = res.data?.meter_reading?.interval_reading || [];

        intervals.forEach(entry => {
            if (!entry.date || !entry.value) return;
            const date = parseISO(entry.date);
            const monthIndex = date.getMonth();
            const monthName = MOIS_FR[monthIndex];

            // Valeur officielle en Wh, on convertit en kWh
            const kwh = parseFloat(entry.value) / 1000;
            dailyData[monthName].total_kwh += kwh;
        });
        
        return dailyData;

    } catch (e) {
        console.warn(`Erreur Daily ${type} (Calibration impossible):`, e.message);
        return null; // Si erreur, on ne pourra pas calibrer, on garde les données Load Curve
    }
}


// --- RECUPERATION COURBES + CALIBRATION ---
async function fetchMeteringInternal(usage_point_id, providedToken, type, curveSuffix, startStr, endStr, offpeakStr) {
  const token = await getToken(providedToken);
  const allData = [];
  
  // 1. Fetch Load Curve (Détail HP/HC)
  let current = parseISO(startStr);
  const finalEnd = parseISO(endStr);

  while (current <= finalEnd) {
    const chunkStart = format(current, 'yyyy-MM-dd');
    let chunkEndDate = addDays(current, 6); // Load curve limité à 7 jours souvent
    if (chunkEndDate > finalEnd) chunkEndDate = finalEnd;
    const chunkEnd = format(chunkEndDate, 'yyyy-MM-dd');

    try {
      await sleep(150);
      const res = await axios.get(`${ENEDIS_BASE_URL}/metering_data_${curveSuffix}/v5/${type}_load_curve`, { 
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

  // 2. Aggrégation Load Curve (Calculé)
  const monthly = getEmptyMonthsStructure();
  const periods = parseOffpeakHours(offpeakStr);

  allData.forEach(entry => {
    if (!entry.date || !entry.value) return;
    const date = parseISO(entry.date);
    const monthName = MOIS_FR[date.getMonth()];
    
    // Load Curve est en W moyenné sur 30min -> Wh = W * 0.5. Puis /1000 -> kWh
    const kwh = (parseFloat(entry.value) * 0.5) / 1000;
    
    monthly[monthName].total_kwh += kwh;
    if (isTimeInOffpeak(date, periods)) monthly[monthName].hc_kwh += kwh;
    else monthly[monthName].hp_kwh += kwh;
  });

  // 3. RECUPERATION TOTAL OFFICIEL (Daily) ET CALIBRATION
  const officialMonthly = await fetchDailyTotals(usage_point_id, token, type, startStr, endStr);

  if (officialMonthly) {
      Object.keys(monthly).forEach(m => {
          const calculatedTotal = monthly[m].total_kwh;
          const officialTotal = officialMonthly[m].total_kwh;

          // Si on a des données des deux côtés
          if (calculatedTotal > 0 && officialTotal > 0) {
              const ratio = officialTotal / calculatedTotal;
              
              // On applique le ratio pour que la somme HP+HC soit égale au Total Officiel
              monthly[m].hp_kwh = monthly[m].hp_kwh * ratio;
              monthly[m].hc_kwh = monthly[m].hc_kwh * ratio;
              monthly[m].total_kwh = officialTotal; // On force le total exact
              monthly[m].correction_ratio = ratio; // Pour info
          }
      });
  }

  // Arrondis finaux
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
    const userInfo = await getUserInfoInternal(pdl, providedToken);
    customersList.push(userInfo);

    let offpeakStr = userInfo.contract?.offpeak_hours;
    if (!offpeakStr) offpeakStr = "HC (22H00-06H00)";
    globalConso.offpeak_hours.add(offpeakStr);

    // Conso (avec calibration DC)
    const conso = await fetchMeteringInternal(pdl, providedToken, 'consumption', 'clc', startStr, endStr, offpeakStr);
    aggregateMonths(globalConso.monthly, conso.monthly);

    // Prod (avec calibration DP)
    let prod;
    try {
        prod = await fetchMeteringInternal(pdl, providedToken, 'production', 'plc', startStr, endStr, '');
    } catch (e) {
        prod = { total_kwh: 0, monthly: getEmptyMonthsStructure() };
    }
    aggregateMonths(globalProd.monthly, prod.monthly);
  }

  // Transformation Listes (Prod en négatif)
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