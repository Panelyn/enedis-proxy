// server.js - Proxy Linky Enedis pour Production PANELYN (Optimisé + Ajustement Proportionnel via daily_consumption/production + Parsing Corrigé)
require('dotenv').config();
const express = require('express');
const axios = require('axios');
const Joi = require('joi');
const { parseISO, startOfDay, addDays, format, subYears, startOfMonth, endOfMonth } = require('date-fns');
const cors = require('cors');
const rateLimit = require('express-rate-limit');
const app = express();
app.use(express.json());
app.use(cors()); // Active CORS pour tous

// === CONFIG ===
const ENEDIS_CLIENT_ID = process.env.ENEDIS_CLIENT_ID;
const ENEDIS_CLIENT_SECRET = process.env.ENEDIS_CLIENT_SECRET;
const NODE_ENV = process.env.NODE_ENV || 'development';
if (!ENEDIS_CLIENT_ID || !ENEDIS_CLIENT_SECRET) {
  console.error('ERREUR : Client ID/Secret manquants dans .env');
  process.exit(1);
}
const ENEDIS_BASE_URL = 'https://gw.ext.prod.api.enedis.fr';
const TOKEN_ENDPOINT = `${ENEDIS_BASE_URL}/oauth2/v3/token`;

// Cache app token (fallback)
let appTokenCache = { token: null, expiresAt: 0 };

// === RATE LIMITING ===
const secondLimiter = rateLimit({
  windowMs: 1 * 1000,
  max: 10,
  message: { success: false, error: 'Trop de requêtes ! Limite : 10 par seconde.' },
  standardHeaders: true,
  legacyHeaders: false
});
const hourLimiter = rateLimit({
  windowMs: 60 * 60 * 1000,
  max: 10000,
  message: { success: false, error: 'Trop de requêtes ! Limite : 10000 par heure.' },
  standardHeaders: true,
  legacyHeaders: false
});

// === SCHEMAS ===
const meteringSchema = Joi.object({
  usage_point_id: Joi.string().pattern(/^\d{14}$/).required(),
  start_date: Joi.string().isoDate().required(),
  end_date: Joi.string().isoDate().required(),
  aggregate: Joi.string().valid('hourly_monthly').optional(),
  access_token: Joi.string().optional().allow('')
});
const userInfoSchema = Joi.object({
  usage_point_id: Joi.string().pattern(/^\d{14}$/).required(),
  access_token: Joi.string().optional().allow('')
});

// === GET TOKEN (App ou User) ===
async function getToken(accessTokenProvided = null) {
  if (accessTokenProvided && accessTokenProvided.trim() !== '') {
    if (NODE_ENV === 'development') console.log('[AUTH] Utilisation token user fourni');
    return accessTokenProvided;
  }
  // Fallback app token
  const now = Date.now();
  if (appTokenCache.token && now < appTokenCache.expiresAt) {
    if (NODE_ENV === 'development') console.log('App token réutilisé.');
    return appTokenCache.token;
  }
  try {
    if (NODE_ENV === 'development') console.log('Génération app token...');
    const response = await axios.post(
      TOKEN_ENDPOINT,
      new URLSearchParams({
        grant_type: 'client_credentials',
        client_id: ENEDIS_CLIENT_ID,
        client_secret: ENEDIS_CLIENT_SECRET,
        scope: 'metering_data metering_data_contract_details'
      }).toString(),
      { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 10000 }
    );
    appTokenCache.token = response.data.access_token;
    appTokenCache.expiresAt = now + (55 * 60 * 1000);
    if (NODE_ENV === 'development') console.log('App token OK');
    return appTokenCache.token;
  } catch (error) {
    console.error('Erreur app token:', error.response?.data || error.message);
    throw error;
  }
}

// === FETCH USER INFO ===
async function fetchUserInfo(req, res) {
  const { error: validationError } = userInfoSchema.validate(req.body);
  if (validationError) return res.status(400).json({ success: false, error: validationError.details[0].message });
  const { usage_point_id, access_token: providedToken } = req.body;
  let token;
  try {
    token = await getToken(providedToken);
  } catch (error) {
    return res.status(500).json({ success: false, type: 'user_info', error: 'Erreur token' });
  }
  let errorOccurred = false, errorMessage = '', address = null, contract = null;
  try {
    if (NODE_ENV === 'development') console.log(`[USER_INFO] Fetch adresse/contrat pour PDL ${usage_point_id} avec ${providedToken ? 'user token' : 'app token'}`);
    // 1. Adresse + basics contrat
    let detailsResponse;
    try {
      detailsResponse = await axios.get(
        `${ENEDIS_BASE_URL}/metering_data/v5/usage_point_details`,
        {
          headers: { 'Authorization': `Bearer ${token}` },
          params: { usage_point_id },
          timeout: 10000
        }
      );
    } catch (authError) {
      if (authError.response?.status === 401 || authError.response?.status === 403) {
        console.log('[USER_INFO] Token user invalide – retry avec app token');
        token = await getToken();
        detailsResponse = await axios.get(
          `${ENEDIS_BASE_URL}/metering_data/v5/usage_point_details`,
          {
            headers: { 'Authorization': `Bearer ${token}` },
            params: { usage_point_id },
            timeout: 10000
          }
        );
      } else {
        throw authError;
      }
    }
    const usagePoints = detailsResponse.data?.customer?.usage_points || [];
    if (usagePoints.length > 0) {
      const up = usagePoints[0];
      address = up.address || {};
      contract = { usage_point_id: up.usage_point_id, meter_type: up.meter_type, connection_status: up.connection_status };
    } else {
      throw new Error('Aucun usage point trouvé');
    }
    // 2. Contrat full (période 1 an)
    let contractResponse;
    try {
      const contractStart = format(subYears(new Date(), 1), 'yyyy-MM-dd');
      const contractEnd = format(new Date(), 'yyyy-MM-dd');
      contractResponse = await axios.get(
        `${ENEDIS_BASE_URL}/metering_data_contract_details/v5/contract_details`,
        {
          headers: { 'Authorization': `Bearer ${token}` },
          params: { usage_point_id, start: contractStart, end: contractEnd },
          timeout: 10000
        }
      );
    } catch (authError) {
      if (authError.response?.status === 401 || authError.response?.status === 403) {
        console.log('[USER_INFO] Token user invalide pour contrat – skip full details');
      } else {
        throw authError;
      }
    }
    if (contractResponse) {
      const contracts = contractResponse.data?.customer?.contracts || [];
      if (contracts.length > 0) contract = { ...contract, ...contracts[0] };
    }
    if (NODE_ENV === 'development') console.log(`[USER_INFO] OK : ${address ? 'Adresse OK' : "Pas d'adresse"}, contrat basics OK`);
  } catch (error) {
    console.error(`[USER_INFO] Erreur détaillée:`, error.response?.status, error.response?.data || error.message);
    console.error('Full Enedis response:', JSON.stringify(error.response?.data, null, 2));
    errorOccurred = true;
    errorMessage = error.response?.data?.error_description || error.message || `Erreur API Enedis (${error.response?.status || 'unknown'})`;
  }
  res.status(errorOccurred ? 500 : 200).json({
    success: !errorOccurred,
    type: 'user_info',
    usage_point_id,
    address,
    contract,
    error_details: errorOccurred ? errorMessage : null
  });
}

// === GET OFFICIAL MONTHLY TOTAL (générique pour conso/prod via daily_consumption/production) ===
async function getMonthlyOfficialTotal(usage_point_id, token, providedToken, type, monthStart, monthEnd) {
  const suffix = type === 'consumption' ? 'dc' : 'dp';
  let effectiveToken = token;
  try {
    if (NODE_ENV === 'development') console.log(`[${suffix.toUpperCase()}] Fetch total officiel pour ${format(monthStart, 'yyyy-MM')} avec ${providedToken ? 'user token' : 'app token'}`);
    let response;
    try {
      response = await axios.get(
        `${ENEDIS_BASE_URL}/metering_data_${suffix}/v5/daily_${type}`,
        {
          headers: { 'Authorization': `Bearer ${effectiveToken}` },
          params: {
            usage_point_id,
            start: format(monthStart, 'yyyy-MM-dd'),
            end: format(monthEnd, 'yyyy-MM-dd')
          },
          timeout: 10000
        }
      );
    } catch (authError) {
      if (authError.response?.status === 401 || authError.response?.status === 403) {
        console.log(`[${suffix.toUpperCase()}] Token user invalide – retry avec app token`);
        effectiveToken = await getToken();
        response = await axios.get(
          `${ENEDIS_BASE_URL}/metering_data_${suffix}/v5/daily_${type}`,
          {
            headers: { 'Authorization': `Bearer ${effectiveToken}` },
            params: {
              usage_point_id,
              start: format(monthStart, 'yyyy-MM-dd'),
              end: format(monthEnd, 'yyyy-MM-dd')
            },
            timeout: 10000
          }
        );
      } else {
        throw authError;
      }
    }
    // FIX : Parsing correct d'après doc : meter_reading.interval_reading
    const values = response.data?.meter_reading?.interval_reading || [];
    const totalWh = values.reduce((sum, entry) => sum + parseFloat(entry.value || 0), 0);
    if (NODE_ENV === 'development') console.log(`[${suffix.toUpperCase()}] Total officiel: ${totalWh} Wh (${values.length} jours)`);
    return totalWh;
  } catch (error) {
    console.error(`[${suffix.toUpperCase()}] Erreur fetch total ${format(monthStart, 'yyyy-MM')}:`, error.response?.status, error.message);
    throw error;
  }
}

// === AGRÉGATION (CORRIGÉE : *0.5 pour convertir W → Wh sur 30 min) ===
function aggregateData(data) {
  const monthlySums = {};
  data.forEach(entry => {
    if (!entry.date || entry.date === "null") return;
    const dateObj = new Date(entry.date);
    if (isNaN(dateObj.getTime())) return;
    const monthKey = `${dateObj.getFullYear()}-${String(dateObj.getMonth() + 1).padStart(2, '0')}`;
    const hourKey = String(dateObj.getHours()).padStart(2, '0');
    if (!monthlySums[monthKey]) monthlySums[monthKey] = Array(24).fill(0);
    let value = isNaN(parseFloat(entry.value)) ? 0 : parseFloat(entry.value);
    // FIX : Multiplier par 0.5 pour énergie en Wh (intervalle 30 min = 0.5 h)
    monthlySums[monthKey][parseInt(hourKey)] += value * 0.5;
  });
  Object.keys(monthlySums).forEach(month => {
    monthlySums[month] = monthlySums[month].map(v => Math.round(v * 100) / 100);
  });
  return monthlySums;
}

// === METERING GÉNÉRIQUE (optimisé : retry/skip sur 500, détection vide) ===
async function fetchMeteringData(req, res, type, apiSuffix) {
  const { error: validationError } = meteringSchema.validate(req.body);
  if (validationError) return res.status(400).json({ success: false, error: validationError.details[0].message });
  const { usage_point_id, start_date, end_date, aggregate, access_token: providedToken } = req.body;
  const start = parseISO(start_date);
  const end = parseISO(end_date);
  if (start >= end) return res.status(400).json({ success: false, error: 'start_date doit être avant end_date' });
  let token;
  try {
    token = await getToken(providedToken);
  } catch (error) {
    return res.status(500).json({ success: false, type, error: 'Erreur token' });
  }
  const allData = [];
  let current = startOfDay(start);
  let errorOccurred = false, errorMessage = '';
  let skippedChunks = 0;
  let consecutiveEmpty = 0; // Compteur pour abort cascade vides
  try {
    while (current <= end && !errorOccurred) {
      const chunkStart = format(current, 'yyyy-MM-dd');
      let chunkEndDate = addDays(current, 6);
      if (chunkEndDate > end) chunkEndDate = end;
      const chunkEnd = format(chunkEndDate, 'yyyy-MM-dd');
      const chunkTimeout = 10000; // FIX : 10s fixe pour vitesse (prod souvent vide)
      if (NODE_ENV === 'development') console.log(`[${type.toUpperCase()}] Chunk ${chunkStart} → ${chunkEnd} avec ${providedToken ? 'user token' : 'app token'}`);
      let response;
      let chunkError = false;
      let attempts = 0;
      while (attempts < 1 && !chunkError) { // FIX : 1 retry max pour vitesse
        try {
          response = await axios.get(
            `${ENEDIS_BASE_URL}/metering_data_${apiSuffix}/v5/${type}_load_curve`,
            {
              headers: { 'Authorization': `Bearer ${token}` },
              params: { usage_point_id, start: chunkStart, end: chunkEnd },
              timeout: chunkTimeout
            }
          );
          break;
        } catch (err) {
          if ((err.response?.status === 500 || err.response?.status === 429) && attempts < 0) { // Jamais retry si 1 max
            attempts++;
            console.log(`[${type.toUpperCase()}] Retry ${attempts}/1 sur chunk ${chunkStart} → ${chunkEnd} (erreur ${err.response?.status})`);
            await new Promise(resolve => setTimeout(resolve, 1000 * attempts)); // 1s
          } else {
            console.error(`[${type.toUpperCase()}] Chunk échoué après ${attempts + 1} tentatives:`, err.response?.status, err.response?.data || err.message);
            console.error('Full Enedis response:', JSON.stringify(err.response?.data, null, 2));
            chunkError = true;
          }
        }
      }
      if (chunkError) {
        skippedChunks++;
        if (!errorOccurred) errorMessage = `Chunk ${chunkStart} → ${chunkEnd} skipé (erreur technique Enedis).`;
        consecutiveEmpty++;
        if (consecutiveEmpty > 2) { // Abort si >2 chunks vides consécutifs
          errorMessage += ' Arrêt précoce (période vide).';
          break;
        }
        continue;
      }
      const values = response.data?.meter_reading?.interval_reading || [];
      allData.push(...values);
      if (values.length === 0) {
        skippedChunks++; // Compte comme vide
        consecutiveEmpty++;
        if (NODE_ENV === 'development') console.log(`[${type.toUpperCase()}] Chunk vide skipé (${chunkStart} → ${chunkEnd}) – Pas de ${type} détectée.`);
        if (consecutiveEmpty > 2) break;
        continue;
      }
      consecutiveEmpty = 0; // Reset si data OK
      if (NODE_ENV === 'development') console.log(`[${type.toUpperCase()}] ${values.length} points`);
      current = startOfDay(addDays(chunkEndDate, 1));
    }
    if (skippedChunks > Math.floor((end.getTime() - start.getTime()) / (1000 * 60 * 60 * 24 * 7)) * 0.5) { // >50% skipé
      errorOccurred = false; // Garde success true
      if (errorMessage) errorMessage += ' (Période partielle – extrapolation OK pour simu).';
    }
  } catch (error) {
    console.error(`[${type.toUpperCase()}] Erreur globale:`, error.response?.status, error.response?.data || error.message);
    console.error('Full Enedis response:', JSON.stringify(error.response?.data, null, 2));
    errorOccurred = true;
    errorMessage = error.response?.data?.error_description || error.message || `Erreur API (${error.response?.status || 'unknown'})`;
  }
  let data = allData;
  let totalWhRaw = 0;
  let coveragePct = 0;
  let adjustmentInfo = {};
  if (aggregate === 'hourly_monthly' && !errorOccurred) {
    try {
      data = aggregateData(allData);
      // Calcul couverture
      const periodMs = end.getTime() - start.getTime();
      const expectedIntervals = Math.floor(periodMs / (1000 * 60 * 30));  // 30 min
      coveragePct = Math.round((allData.length / expectedIntervals) * 100);
      totalWhRaw = allData.reduce((sum, e) => sum + (parseFloat(e.value) * 0.5), 0);
      // === AJUSTEMENT PROPORTIONNEL VIA DAILY (pour totaux "parfaits") ===
      if (NODE_ENV === 'development') console.log(`[AGG] Début ajustement proportionnel pour ${type}...`);
      const months = Object.keys(data);
      for (const monthKey of months) {
        const [year, month] = monthKey.split('-').map(Number);
        let monthStart = startOfMonth(new Date(year, month - 1, 1));
        let monthEnd = endOfMonth(monthStart);
        // Clip au période demandée si mois partiel
        if (monthStart < start) monthStart = start;
        if (monthEnd > end) monthEnd = end;
        if (monthStart >= monthEnd) continue; // Mois vide
        try {
          const officialTotal = await getMonthlyOfficialTotal(usage_point_id, token, providedToken, type, monthStart, monthEnd);
          const calculatedTotal = data[monthKey].reduce((sum, v) => sum + v, 0);
          const diffPct = calculatedTotal > 0 ? Math.abs(officialTotal - calculatedTotal) / calculatedTotal * 100 : 0;
          adjustmentInfo[monthKey] = { official: Math.round(officialTotal), calculated: Math.round(calculatedTotal), diff_pct: diffPct.toFixed(1) };
          if (calculatedTotal > 0 && officialTotal > 0 && diffPct > 0.5) { // FIX : Seuil + official >0 pour éviter ratio 0
            const ratio = officialTotal / calculatedTotal;
            data[monthKey] = data[monthKey].map(v => Math.round((v * ratio) * 100) / 100);
            adjustmentInfo[monthKey].ratio = ratio.toFixed(3);
            adjustmentInfo[monthKey].skipped = false;
            if (NODE_ENV === 'development') console.log(`[AGG] ${monthKey} ajusté: *${ratio.toFixed(3)} (officiel ${Math.round(officialTotal)} vs calc ${Math.round(calculatedTotal)} Wh, diff ${diffPct.toFixed(1)}%)`);
          } else if (officialTotal === 0 && calculatedTotal > 0) {
            adjustmentInfo[monthKey].skipped = true;
            adjustmentInfo[monthKey].skipped_reason = "Daily vide – probable limit app token; user token requis pour ajustement";
            if (NODE_ENV === 'development') console.log(`[AGG] Skip ajustement ${monthKey}: ${adjustmentInfo[monthKey].skipped_reason}`);
          } else {
            adjustmentInfo[monthKey].skipped = false;
            if (NODE_ENV === 'development') console.log(`[AGG] ${monthKey} OK (match: ${Math.round(officialTotal)} Wh, diff ${diffPct.toFixed(1)}%)`);
          }
        } catch (err) {
          adjustmentInfo[monthKey] = { error: err.message };
          if (NODE_ENV === 'development') console.log(`[AGG] Skip ajustement ${monthKey}: ${err.message}`);
        }
      }
      if (NODE_ENV === 'development') console.log(`[AGG] Ajustement terminé pour ${type}.`);
      // Recalc total après ajustement
      totalWhRaw = Object.values(data).reduce((sum, month) => sum + month.reduce((s, v) => s + v, 0), 0);
    } catch (err) {
      console.error('Erreur agrégation:', err);
      errorOccurred = true;
      errorMessage = 'Erreur agrégation';
      data = allData;
    }
  }
  res.status(errorOccurred ? 500 : 200).json({
    success: !errorOccurred,
    type,
    period: { start: start_date, end: end_date },
    total_points: allData.length,
    skipped_chunks: skippedChunks,
    coverage_pct: coveragePct,
    total_wh: Math.round(totalWhRaw),  // Total ajusté (ou brut si skip)
    adjustment_info: adjustmentInfo,  // Détails par mois pour debug Bubble
    data,
    error_details: errorOccurred ? errorMessage : (skippedChunks > 0 ? errorMessage : null)
  });
}

// === ENDPOINTS ===
app.post('/get-linky', secondLimiter, hourLimiter, (req, res) => fetchMeteringData(req, res, 'consumption', 'clc'));
app.post('/get-production', secondLimiter, hourLimiter, (req, res) => fetchMeteringData(req, res, 'production', 'plc'));
app.post('/get-user-info', secondLimiter, hourLimiter, fetchUserInfo);
// Health
app.get('/health', (req, res) => res.status(200).json({ status: 'OK', timestamp: new Date().toISOString() }));

// === START ===
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Proxy PANELYN sur http://localhost:${PORT} (env: ${NODE_ENV})`);
  console.log(`Endpoints : /get-linky, /get-production, /get-user-info, /health`);
  console.log(`Support user token pour prod Enedis !`);
  if (NODE_ENV === 'production') console.log('Mode PROD : Logs minimisés');
});