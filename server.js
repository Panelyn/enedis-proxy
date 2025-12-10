// server.js - Proxy Linky Enedis pour Production PANELYN (Optimisé + Fix User Info Nested Parse + Logs JSON Full)
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

// FIX Render : Trust proxy pour X-Forwarded-For (rate-limit IP client vraie)
app.set('trust proxy', 1);

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

// === FETCH USER INFO (FIX : Parse Imbriqué usage_point.usage_point_addresses) ===
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
      // Calls parallèles pour perf (Promise.allSettled)
      const [addressResponse, contractResponse, identityResponse, contactResponse] = await Promise.allSettled([
        // 1. Adresse (customers_upa/v5/usage_points/addresses)
        axios.get(`${ENEDIS_BASE_URL}/customers_upa/v5/usage_points/addresses`, {
          headers: { 'Authorization': `Bearer ${token}`, 'Accept': 'application/json' },
          params: { usage_point_id },
          timeout: 10000
        }).catch(async (authError) => {
          if (authError.response?.status === 401 || authError.response?.status === 403) {
            const fallbackToken = await getToken();
            return axios.get(`${ENEDIS_BASE_URL}/customers_upa/v5/usage_points/addresses`, {
              headers: { 'Authorization': `Bearer ${fallbackToken}`, 'Accept': 'application/json' },
              params: { usage_point_id },
              timeout: 10000
            });
          }
          throw authError;
        }),
        // 2. Contrat (customers_upc/v5/usage_points/contracts)
        axios.get(`${ENEDIS_BASE_URL}/customers_upc/v5/usage_points/contracts`, {
          headers: { 'Authorization': `Bearer ${token}`, 'Accept': 'application/json' },
          params: { usage_point_id },
          timeout: 10000
        }).catch(async (authError) => {
          if (authError.response?.status === 401 || authError.response?.status === 403) {
            const fallbackToken = await getToken();
            return axios.get(`${ENEDIS_BASE_URL}/customers_upc/v5/usage_points/contracts`, {
              headers: { 'Authorization': `Bearer ${fallbackToken}`, 'Accept': 'application/json' },
              params: { usage_point_id },
              timeout: 10000
            });
          }
          throw authError;
        }),
        // 3. Identity (customers_i/v5/identity)
        axios.get(`${ENEDIS_BASE_URL}/customers_i/v5/identity`, {
          headers: { 'Authorization': `Bearer ${token}`, 'Accept': 'application/json' },
          params: { usage_point_id },
          timeout: 10000
        }).catch(async (authError) => {
          if (authError.response?.status === 401 || authError.response?.status === 403) {
            const fallbackToken = await getToken();
            return axios.get(`${ENEDIS_BASE_URL}/customers_i/v5/identity`, {
              headers: { 'Authorization': `Bearer ${fallbackToken}`, 'Accept': 'application/json' },
              params: { usage_point_id },
              timeout: 10000
            });
          }
          throw authError;
        }),
        // 4. Contact (customers_cd/v5/contact_data)
        axios.get(`${ENEDIS_BASE_URL}/customers_cd/v5/contact_data`, {
          headers: { 'Authorization': `Bearer ${token}`, 'Accept': 'application/json' },
          params: { usage_point_id },
          timeout: 10000
        }).catch(async (authError) => {
          if (authError.response?.status === 401 || authError.response?.status === 403) {
            const fallbackToken = await getToken();
            return axios.get(`${ENEDIS_BASE_URL}/customers_cd/v5/contact_data`, {
              headers: { 'Authorization': `Bearer ${fallbackToken}`, 'Accept': 'application/json' },
              params: { usage_point_id },
              timeout: 10000
            });
          }
          throw authError;
        })
      ]);
  
      // Parse Adresse (FIX : Imbriqué up.usage_point.usage_point_addresses)
      if (addressResponse.status === 'fulfilled') {
        const data = addressResponse.value.data;
        if (NODE_ENV === 'development') console.log(`[USER_INFO] Adresse full response: ${JSON.stringify(data, null, 2)}`);
        const customer = data.customer || {};
        const innerUsagePoints = customer.usage_points || [];
        if (NODE_ENV === 'development') console.log(`[USER_INFO] Adresse customer.usage_points length: ${innerUsagePoints.length}`);
        if (innerUsagePoints.length > 0) {
          const up = innerUsagePoints[0];
          const upAddresses = up.usage_point?.usage_point_addresses || {};  // FIX : up.usage_point.usage_point_addresses
          address = {
            street: upAddresses.street,
            locality: upAddresses.locality,
            postal_code: upAddresses.postal_code,
            insee_code: upAddresses.insee_code,
            city: upAddresses.city,
            country: upAddresses.country,
            geo_points: {
              latitude: upAddresses.geo_points?.latitude,
              longitude: upAddresses.geo_points?.longitude,
              altitude: upAddresses.geo_points?.altitude
            }
          };
          if (NODE_ENV === 'development') console.log(`[USER_INFO] Adresse OK : ${address.street || 'N/A'}, lat ${address.geo_points?.latitude}`);
        } else {
          if (NODE_ENV === 'development') console.log('[USER_INFO] Pas d\'adresse trouvée (usage_points vide)');
        }
      } else {
        if (NODE_ENV === 'development') console.log('[USER_INFO] Erreur adresse:', addressResponse.reason?.response?.status || addressResponse.reason?.message);
      }
  
      // Parse Contrat (FIX : up.contracts direct)
      if (contractResponse.status === 'fulfilled') {
        const data = contractResponse.value.data;
        if (NODE_ENV === 'development') console.log(`[USER_INFO] Contrat full response: ${JSON.stringify(data, null, 2)}`);
        const customer = data.customer || {};
        const innerUsagePoints = customer.usage_points || [];
        if (NODE_ENV === 'development') console.log(`[USER_INFO] Contrat customer.usage_points length: ${innerUsagePoints.length}`);
        if (innerUsagePoints.length > 0) {
          const up = innerUsagePoints[0];
          contract = {
            usage_point_id: up.usage_point?.usage_point_id || usage_point_id,
            meter_type: up.usage_point?.meter_type,
            connection_status: up.usage_point?.usage_point_status,
            segment: up.contracts?.segment,
            subscribed_power: up.contracts?.subscribed_power,
            last_activation_date: up.contracts?.last_activation_date,
            distribution_tariff: up.contracts?.distribution_tariff,
            last_distribution_tariff_change_date: up.contracts?.last_distribution_tariff_change_date,
            offpeak_hours: up.contracts?.offpeak_hours,
            contract_status: up.contracts?.contract_status,
            contract_type: up.contracts?.contract_type
          };
          if (NODE_ENV === 'development') console.log(`[USER_INFO] Contrat OK : segment ${contract.segment}, power ${contract.subscribed_power}`);
        } else {
          if (NODE_ENV === 'development') console.log('[USER_INFO] Pas de contrat trouvé (usage_points vide)');
        }
      } else {
        if (NODE_ENV === 'development') console.log('[USER_INFO] Erreur contrat:', contractResponse.reason?.response?.status || contractResponse.reason?.message);
      }
  
      // Parse Identity (ajoute à contract)
      if (identityResponse.status === 'fulfilled') {
        const identityData = identityResponse.value.data || {};
        if (identityData.identity?.natural_person) {
          contract = { ...contract, firstname: identityData.identity.natural_person.firstname, lastname: identityData.identity.natural_person.lastname };
          if (NODE_ENV === 'development') console.log(`[USER_INFO] Identity OK : ${contract.firstname} ${contract.lastname}`);
        } else {
          if (NODE_ENV === 'development') console.log('[USER_INFO] Identity vide');
        }
      } else {
        if (NODE_ENV === 'development') console.log('[USER_INFO] Skip identity:', identityResponse.reason?.response?.status || identityResponse.reason?.message);
      }
  
      // Parse Contact (ajoute à contract)
      if (contactResponse.status === 'fulfilled') {
        const contactData = contactResponse.value.data || {};
        if (contactData.contact_data) {
          contract = { ...contract, email: contactData.contact_data.email, phone: contactData.contact_data.phone };
          if (NODE_ENV === 'development') console.log(`[USER_INFO] Contact OK : ${contract.email}`);
        } else {
          if (NODE_ENV === 'development') console.log('[USER_INFO] Contact vide');
        }
      } else {
        if (NODE_ENV === 'development') console.log('[USER_INFO] Skip contact:', contactResponse.reason?.response?.status || contactResponse.reason?.message);
      }
  
      if (NODE_ENV === 'development') console.log(`[USER_INFO] Résumé : ${address ? 'Adresse OK' : "Pas d'adresse"}, contrat ${Object.keys(contract || {}).length} champs`);
    } catch (error) {
      console.error(`[USER_INFO] Erreur globale:`, error.response?.status, error.response?.data || error.message);
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
const PORT = process.env.PORT || 3000;  // Local 3000 (Render override)
app.listen(PORT, () => {
  console.log(`Proxy PANELYN sur http://localhost:${PORT} (env: ${NODE_ENV})`);
  console.log(`Endpoints : /get-linky, /get-production, /get-user-info, /health`);
  console.log(`Support user token pour prod Enedis !`);
  if (NODE_ENV === 'production') console.log('Mode PROD : Logs minimisés');
});