// server.js - Proxy Linky Enedis pour Production PANELYN (OAuth User Token + Joi Fix)
require('dotenv').config();
const express = require('express');
const axios = require('axios');
const Joi = require('joi');
const { parseISO, startOfDay, addDays, format, subYears } = require('date-fns');
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

// === SCHEMAS (FIX : access_token ajouté) ===
const meteringSchema = Joi.object({
  usage_point_id: Joi.string().pattern(/^\d{14}$/).required(),
  start_date: Joi.string().isoDate().required(),
  end_date: Joi.string().isoDate().required(),
  aggregate: Joi.string().valid('hourly_monthly').optional(),
  access_token: Joi.string().optional().allow('') // FIX : User token optionnel
});

const userInfoSchema = Joi.object({
  usage_point_id: Joi.string().pattern(/^\d{14}$/).required(),
  access_token: Joi.string().optional().allow('') // FIX : User token optionnel pour auth perso
});

// === GET TOKEN (App ou User) ===
async function getToken(accessTokenProvided = null) {
  if (accessTokenProvided && accessTokenProvided.trim() !== '') {
    if (NODE_ENV === 'development') console.log('[AUTH] Utilisation token user fourni');
    return accessTokenProvided; // User token direct
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
        scope: 'metering_data metering_data_contract_details' // Scopes explicites
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

// === FETCH USER INFO (avec retry sur auth error) ===
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
        token = await getToken(); // Fallback app si user token expire
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
        // Garde basics, skip full
      } else {
        throw authError;
      }
    }
    if (contractResponse) {
      const contracts = contractResponse.data?.customer?.contracts || [];
      if (contracts.length > 0) contract = { ...contract, ...contracts[0] };
    }

    if (NODE_ENV === 'development') console.log(`[USER_INFO] OK : ${address ? 'Adresse OK' : "Pas d'adresse"}, contrat basics OK`); // FIX : Escape supprimé
  } catch (error) {
    console.error(`[USER_INFO] Erreur détaillée:`, error.response?.status, error.response?.data || error.message);
    console.error('Full Enedis response:', JSON.stringify(error.response?.data, null, 2)); // NOUVEAU : Logs full pour debug 500
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

// === AGRÉGATION (inchangée) ===
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
    monthlySums[monthKey][parseInt(hourKey)] += value;
  });
  Object.keys(monthlySums).forEach(month => {
    monthlySums[month] = monthlySums[month].map(v => Math.round(v * 100) / 100);
  });
  return monthlySums;
}

// === METERING GÉNÉRIQUE (updaté avec retry) ===
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
  try {
    while (current <= end && !errorOccurred) {
      const chunkStart = format(current, 'yyyy-MM-dd');
      let chunkEndDate = addDays(current, 6);
      if (chunkEndDate > end) chunkEndDate = end;
      const chunkEnd = format(chunkEndDate, 'yyyy-MM-dd');
      if (NODE_ENV === 'development') console.log(`[${type.toUpperCase()}] Chunk ${chunkStart} → ${chunkEnd} avec ${providedToken ? 'user token' : 'app token'}`);
      let response;
      try {
        response = await axios.get(
          `${ENEDIS_BASE_URL}/metering_data_${apiSuffix}/v5/${type}_load_curve`,
          {
            headers: { 'Authorization': `Bearer ${token}` },
            params: { usage_point_id, start: chunkStart, end: chunkEnd },
            timeout: 15000
          }
        );
      } catch (authError) {
        if (authError.response?.status === 401 || authError.response?.status === 403) {
          console.log(`[${type.toUpperCase()}] Token user invalide – retry avec app token`);
          token = await getToken();
          response = await axios.get(
            `${ENEDIS_BASE_URL}/metering_data_${apiSuffix}/v5/${type}_load_curve`,
            {
              headers: { 'Authorization': `Bearer ${token}` },
              params: { usage_point_id, start: chunkStart, end: chunkEnd },
              timeout: 15000
            }
          );
        } else {
          throw authError;
        }
      }
      const values = response.data?.meter_reading?.interval_reading || [];
      allData.push(...values);
      if (NODE_ENV === 'development') console.log(`[${type.toUpperCase()}] ${values.length} points`);
      current = startOfDay(addDays(chunkEndDate, 1));
    }
  } catch (error) {
    console.error(`[${type.toUpperCase()}] Erreur:`, error.response?.status, error.response?.data || error.message);
    console.error('Full Enedis response:', JSON.stringify(error.response?.data, null, 2)); // NOUVEAU : Logs full pour debug
    errorOccurred = true;
    errorMessage = error.response?.data?.error_description || error.message || `Erreur API (${error.response?.status || 'unknown'})`;
  }

  let data = allData;
  if (aggregate === 'hourly_monthly' && !errorOccurred) {
    try {
      data = aggregateData(allData);
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
    data,
    error_details: errorOccurred ? errorMessage : null
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