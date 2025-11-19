// server.js - Proxy Linky Enedis amélioré
require('dotenv').config();
const express = require('express');
const axios = require('axios');
const Joi = require('joi');
const { parseISO, startOfDay, addDays, format } = require('date-fns');
const cors = require('cors');
const app = express();
app.use(express.json());
app.use(cors()); // Active CORS pour tous
app.use(express.json());
// === CONFIG ===
const ENEDIS_CLIENT_ID = process.env.ENEDIS_CLIENT_ID;
const ENEDIS_CLIENT_SECRET = process.env.ENEDIS_CLIENT_SECRET;
if (!ENEDIS_CLIENT_ID || !ENEDIS_CLIENT_SECRET) {
console.error('ERREUR : Client ID/Secret manquants dans .env');
process.exit(1);
}
// Cache token (simple in-memory)
let tokenCache = { token: null, expiresAt: 0 };
// === VALIDATION SCHEMA ===
const schema = Joi.object({
usage_point_id: Joi.string().pattern(/^\d{14}$/).required(),
start_date: Joi.string().isoDate().required(),
end_date: Joi.string().isoDate().required(),
aggregate: Joi.string().valid('hourly_monthly').optional()
});
// === TOKEN CACHING ===
async function getAccessToken() {
const now = Date.now();
if (tokenCache.token && now < tokenCache.expiresAt) {
console.log('Token réutilisé (valide encore).');
return tokenCache.token;
  }
try {
console.log('Génération nouveau token...');
const response = await axios.post(
'https://gw.ext.prod-sandbox.api.enedis.fr/oauth2/v3/token',
new URLSearchParams({
grant_type: 'client_credentials',
client_id: ENEDIS_CLIENT_ID,
client_secret: ENEDIS_CLIENT_SECRET
      }).toString(),
      {
headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Accept': 'application/json' },
timeout: 10000
      }
    );
tokenCache.token = response.data.access_token;
tokenCache.expiresAt = now + (55 * 60 * 1000); // 55min pour safety
console.log('Token obtenu ! Valable ~1h');
return tokenCache.token;
  } catch (error) {
console.error('Erreur token:', error.response?.data || error.message);
throw error;
  }
}
// === AGRÉGATION OPTIMISÉE ===
function aggregateData(data) {
const monthlySums = {};
data.forEach(entry => {
// Protection contre date null / invalide
if (!entry.date || entry.date === "null") return;
const dateObj = new Date(entry.date);
if (isNaN(dateObj.getTime())) return; // date invalide → on skip
const monthKey = `${dateObj.getFullYear()}-${String(dateObj.getMonth() + 1).padStart(2, '0')}`;
const hourKey = String(dateObj.getHours()).padStart(2, '0');
if (!monthlySums[monthKey]) {
monthlySums[monthKey] = Array(24).fill(0);
      }
const value = isNaN(parseFloat(entry.value)) ? 0 : parseFloat(entry.value);
monthlySums[monthKey][parseInt(hourKey)] += value;
    });
// Optionnel : arrondi
Object.keys(monthlySums).forEach(month => {
monthlySums[month] = monthlySums[month].map(v => Math.round(v * 100) / 100);
    });
return monthlySums;
  }
// === FONCTION GÉNÉRIQUE POUR METERING ===
async function fetchMeteringData(req, res, type, apiSuffix) {
const { error: validationError } = schema.validate(req.body);
if (validationError) {
return res.status(400).json({ success: false, error: validationError.details[0].message });
  }
const { usage_point_id, start_date, end_date, aggregate } = req.body;
const start = parseISO(start_date);
const end = parseISO(end_date);
if (start >= end) return res.status(400).json({ success: false, error: 'start_date doit être avant end_date' });
let accessToken;
try {
accessToken = await getAccessToken();
  } catch (error) {
return res.status(500).json({ success: false, type, error: 'Erreur génération token' });
  }
const allData = [];
let current = startOfDay(start);
let errorOccurred = false;
let errorMessage = '';
try {
while (current <= end && !errorOccurred) {
const chunkStart = format(current, 'yyyy-MM-dd');
let chunkEndDate = addDays(current, 6);
if (chunkEndDate > end) chunkEndDate = end;
const chunkEnd = format(chunkEndDate, 'yyyy-MM-dd');
console.log(`[${type.toUpperCase()}] Récupération ${chunkStart} → ${chunkEnd}`);
const response = await axios.get(
`https://gw.ext.prod-sandbox.api.enedis.fr/metering_data_${apiSuffix}/v5/${type}_load_curve`,
        {
headers: { 'Authorization': `Bearer ${accessToken}`, 'Content-Type': 'application/json' },
params: { usage_point_id, start: chunkStart, end: chunkEnd },
timeout: 15000
        }
      );
const values = response.data?.meter_reading?.interval_reading || [];
allData.push(...values);
console.log(`[${type.toUpperCase()}] OK : ${values.length} mesures`);
current = startOfDay(addDays(chunkEndDate, 1)); // Avance d'1 jour après chunk
    }
  } catch (error) {
console.error(`[${type.toUpperCase()}] Erreur boucle:`, error.response?.data || error.message);
errorOccurred = true;
errorMessage = error.response?.data?.error_description || error.message;
  }
let data = allData;
if (aggregate === 'hourly_monthly' && !errorOccurred) {
  try {
    data = aggregateData(allData);
  } catch (err) {
    console.error('Erreur agrégation:', err);
    errorOccurred = true;
    errorMessage = 'Erreur lors de l’agrégation des données';
    data = allData; // on renvoie les données brutes en secours
  }
}
const status = errorOccurred ? 500 : 200;
res.status(status).json({
success: !errorOccurred,
type,
period: { start: start_date, end: end_date },
total_points: allData.length,
data,
error_details: errorOccurred ? errorMessage : null
  });
}
// === ENDPOINTS ===
app.post('/get-linky', (req, res) => fetchMeteringData(req, res, 'consumption', 'clc'));
app.post('/get-production', (req, res) => fetchMeteringData(req, res, 'production', 'plc'));
// Health check
app.get('/health', (req, res) => res.status(200).json({ status: 'OK', timestamp: new Date().toISOString() }));
// === DÉMARRAGE ===
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
console.log(`Proxy démarré sur http://localhost:${PORT}`);
console.log(`Endpoints : /get-linky, /get-production, /health`);
});