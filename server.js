// server.js - VERSION FINALE + LOGS COMPLETS INFO CLIENT
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

// CONFIG
const ENEDIS_CLIENT_ID = process.env.ENEDIS_CLIENT_ID;
const ENEDIS_CLIENT_SECRET = process.env.ENEDIS_CLIENT_SECRET;
const ENEDIS_BASE_URL = 'https://gw.ext.prod.api.enedis.fr';
const TOKEN_ENDPOINT = `${ENEDIS_BASE_URL}/oauth2/v3/token`;

let appTokenCache = { token: null, expiresAt: 0 };

const secondLimiter = rateLimit({ windowMs: 1000, max: 10 });
const hourLimiter = rateLimit({ windowMs: 60 * 60 * 1000, max: 10000 });

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const MOIS_FR = ["Janvier","Février","Mars","Avril","Mai","Juin","Juillet","Août","Septembre","Octobre","Novembre","Décembre"];

function getEmptyMonthsStructure() {
  const months = {};
  MOIS_FR.forEach(m => months[m] = { total_kwh: 0, hp_kwh: 0, hc_kwh: 0 });
  return months;
}

// SCHEMA
const baseSchema = Joi.object({
  usage_point_ids: Joi.array().items(Joi.string().pattern(/^\d{14}$/)).min(1).required(),
  access_token: Joi.string().optional().allow('')
});

// TOKEN
async function getToken(providedToken = null) {
  if (providedToken?.trim()) return providedToken;
  const now = Date.now();
  if (appTokenCache.token && now < appTokenCache.expiresAt) return appTokenCache.token;

  const response = await axios.post(TOKEN_ENDPOINT,
    new URLSearchParams({ grant_type: 'client_credentials', client_id: ENEDIS_CLIENT_ID, client_secret: ENEDIS_CLIENT_SECRET, scope: 'metering_data metering_data_contract_details' }),
    { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 10000 }
  );
  appTokenCache.token = response.data.access_token;
  appTokenCache.expiresAt = now + (response.data.expires_in - 300) * 1000;
  return appTokenCache.token;
}

// HEURES CREUSES
function parseOffpeakHours(str) {
  if (!str) return [];
  let s = str.toLowerCase().replace(/hc\s*[:=]?\s*/g, '').replace(/h00/g, 'h').replace(/:/g, 'h').trim();
  return s.split(/;|,|et/).map(p => p.trim()).filter(p => p.includes('-'));
}

function isTimeInOffpeak(date, periods) {
  if (!periods.length) return false;
  const t = date.getHours() * 60 + date.getMinutes();
  for (let range of periods) {
    const [startPart, endPart] = range.split('-').map(p => p.trim());
    let startH = parseInt(startPart.replace(/\D/g, ''));
    let endH = parseInt(endPart.replace(/\D/g, ''));
    const startMin = startH * 60;
    const endMin = endH * 60;
    if (endMin < startMin) {
      if (t >= startMin || t < endMin) return true;
    } else if (t >= startMin && t < endMin) return true;
  }
  return false;
}

// === USER INFO - VERSION LOGS COMPLETS ===
async function getUserInfoInternal(usage_point_id, providedToken) {
  const token = await getToken(providedToken);
  let address = { street: null, postal_code: null, city: null };
  let contract = { offpeak_hours: null, subscribed_power: null };
  let identity = { firstname: null, lastname: null };
  let contact = { email: null, phone: null };

  try {
    const [addrRes, contRes, idRes, contactRes] = await Promise.allSettled([
      axios.get(`${ENEDIS_BASE_URL}/customers_upa/v5/usage_points/addresses`, { headers: { Authorization: `Bearer ${token}` }, params: { usage_point_id }, timeout: 10000 }),
      axios.get(`${ENEDIS_BASE_URL}/customers_upc/v5/usage_points/contracts`, { headers: { Authorization: `Bearer ${token}` }, params: { usage_point_id }, timeout: 10000 }),
      axios.get(`${ENEDIS_BASE_URL}/customers_i/v5/identity`, { headers: { Authorization: `Bearer ${token}` }, params: { usage_point_id }, timeout: 10000 }),
      axios.get(`${ENEDIS_BASE_URL}/customers_cd/v5/contact_data`, { headers: { Authorization: `Bearer ${token}` }, params: { usage_point_id }, timeout: 10000 })
    ]);

    // 1. DEBUG CONTRAT
    if (contRes.status === 'fulfilled') {
      const data = contRes.value.data;
      console.log(`\n🔵 [DEBUG] RAW CONTRACT RESPONSE (${usage_point_id}):\n`, JSON.stringify(data, null, 2));
      
      const up = data.customer?.usage_points?.[0];
      const contracts = up?.contracts || up?.usage_point?.contracts;
      if (contracts) {
        contract.offpeak_hours = contracts.offpeak_hours;
        contract.subscribed_power = contracts.subscribed_power;
      }
    } else {
      console.log(`🔴 [DEBUG] CONTRACT FAILED:`, contRes.reason?.message);
    }

    // 2. DEBUG ADRESSE
    if (addrRes.status === 'fulfilled') {
      const data = addrRes.value.data;
      console.log(`\n🔵 [DEBUG] RAW ADDRESS RESPONSE (${usage_point_id}):\n`, JSON.stringify(data, null, 2));

      const up = data.customer?.usage_points?.[0];
      const addr = up?.usage_point?.usage_point_addresses || up?.address;
      if (addr) address = { street: addr.street, postal_code: addr.postal_code, city: addr.city };
    } else {
      console.log(`🔴 [DEBUG] ADDRESS FAILED:`, addrRes.reason?.message);
    }

    // 3. DEBUG IDENTITÉ
    if (idRes.status === 'fulfilled') {
      const data = idRes.value.data;
      console.log(`\n🔵 [DEBUG] RAW IDENTITY RESPONSE (${usage_point_id}):\n`, JSON.stringify(data, null, 2));

      const p = data?.identity?.natural_person;
      if (p) identity = { firstname: p.firstname, lastname: p.lastname };
    } else {
      console.log(`🔴 [DEBUG] IDENTITY FAILED:`, idRes.reason?.message);
    }

    // 4. DEBUG CONTACT
    if (contactRes.status === 'fulfilled') {
      const data = contactRes.value.data;
      console.log(`\n🔵 [DEBUG] RAW CONTACT RESPONSE (${usage_point_id}):\n`, JSON.stringify(data, null, 2));

      const c = data?.contact_data;
      if (c) contact = { email: c.email, phone: c.phone };
    } else {
      console.log(`🔴 [DEBUG] CONTACT FAILED:`, contactRes.reason?.message);
    }
    
    console.log('--------------------------------------------------\n');

  } catch (e) {
    console.error(`Erreur UserInfo ${usage_point_id}:`, e.message);
  }

  return { usage_point_id, address, contract, identity, contact };
}

// === FETCH METERING ===
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
      if (res.data?.meter_reading?.interval_reading) allData.push(...res.data.meter_reading.interval_reading);
    } catch (err) {
      if (err.response?.status === 401 || err.response?.status === 403) break;
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

  Object.keys(monthly).forEach(m => {
    monthly[m].total_kwh = Number(monthly[m].total_kwh.toFixed(2));
    monthly[m].hp_kwh = Number(monthly[m].hp_kwh.toFixed(2));
    monthly[m].hc_kwh = Number(monthly[m].hc_kwh.toFixed(2));
  });

  const total = Number(Object.values(monthly).reduce((s, m) => s + m.total_kwh, 0).toFixed(2));
  return { total_kwh: total, monthly };
}

// CALLBACK
app.get('/callback', (req, res) => {
  const { code, state, usage_point_id, error } = req.query;
  console.log('╔════════════════════════════════════════════╗');
  console.log('║   ✅ CONSENTEMENT ENEDIS REÇU            ║');
  console.log('╠════════════════════════════════════════════╣');
  console.log('║ PRM          →', usage_point_id);
  console.log('║ Code         →', code);
  console.log('║ State        →', state);
  console.log('║ Error        →', error);
  console.log('╚════════════════════════════════════════════╝');

  if (error) return res.redirect(`https://panelyn.com/simulateur?error=${error}`);
  res.redirect(`https://panelyn.com/simulateur?consentement=ok&usage_point_id=${usage_point_id || ''}`);
});

// GET-ALL
app.post('/get-all', secondLimiter, hourLimiter, async (req, res) => {
  const { error } = baseSchema.validate(req.body);
  if (error) return res.status(400).json({ success: false, error: error.details[0].message });

  const { usage_point_ids, access_token: providedToken } = req.body;
  const startStr = format(subYears(new Date(), 1), 'yyyy-MM-dd');
  const endStr = format(new Date(), 'yyyy-MM-dd');

  const customersList = [];
  const globalConso = { monthly: getEmptyMonthsStructure() };
  const globalProd = { monthly: getEmptyMonthsStructure() };

  for (const pdl of usage_point_ids) {
    const userInfo = await getUserInfoInternal(pdl, providedToken);
    customersList.push(userInfo);

    const offpeakStr = userInfo.contract?.offpeak_hours || "HC (22H00-06H00)";

    const conso = await fetchMeteringInternal(pdl, providedToken, 'consumption', 'clc', startStr, endStr, offpeakStr);
    const prod = await fetchMeteringInternal(pdl, providedToken, 'production', 'plc', startStr, endStr, '')
      .catch(() => ({ total_kwh: 0, monthly: getEmptyMonthsStructure() }));

    Object.keys(conso.monthly).forEach(m => {
      globalConso.monthly[m].total_kwh += conso.monthly[m].total_kwh;
      globalConso.monthly[m].hp_kwh += conso.monthly[m].hp_kwh;
      globalConso.monthly[m].hc_kwh += conso.monthly[m].hc_kwh;
    });
    Object.keys(prod.monthly).forEach(m => {
      globalProd.monthly[m].total_kwh += prod.monthly[m].total_kwh;
    });
  }

  const consoHpList = MOIS_FR.map(m => Number(globalConso.monthly[m].hp_kwh.toFixed(2)));
  const consoHcList = MOIS_FR.map(m => Number(globalConso.monthly[m].hc_kwh.toFixed(2)));
  const prodList = MOIS_FR.map(m => Number(globalProd.monthly[m].total_kwh.toFixed(2)));

  res.json({
    success: true,
    period: { start: startStr, end: endStr },
    customers_list: customersList,
    global_data: {
      totals_kwh: {
        consumption: Number((consoHpList.reduce((a,b)=>a+b,0) + consoHcList.reduce((a,b)=>a+b,0)).toFixed(2)),
        production: Number(prodList.reduce((a,b)=>a+b,0).toFixed(2)),
        hp: Number(consoHpList.reduce((a,b)=>a+b,0).toFixed(2)),
        hc: Number(consoHcList.reduce((a,b)=>a+b,0).toFixed(2))
      },
      lists_kwh: {
        consumption_hp: consoHpList,
        consumption_hc: consoHcList,
        production: prodList
      }
    }
  });
});

app.get('/health', (req, res) => res.json({ status: 'OK' }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Serveur prêt sur port ${PORT}`));