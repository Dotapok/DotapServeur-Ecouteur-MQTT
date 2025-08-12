require('dotenv').config();
const express = require('express');
const mqtt = require('mqtt');
const Redis = require('ioredis');
const cors = require('cors');
const axios = require('axios');
const { createLogger, format, transports } = require('winston');
require('winston-daily-rotate-file');

// ---------------------- Logger ----------------------
const LOG_LEVEL = process.env.LOG_LEVEL || 'info';

const fileRotateTransport = new transports.DailyRotateFile({
  dirname: './logs',
  filename: '%DATE%.log',
  datePattern: 'YYYY-MM-DD',
  maxFiles: '14d',
  level: LOG_LEVEL,
});

const logger = createLogger({
  level: LOG_LEVEL,
  format: format.combine(
    format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
    format.printf(({ timestamp, level, message, ...meta }) =>
      `${timestamp} [${level.toUpperCase()}] ${message} ${Object.keys(meta).length ? JSON.stringify(meta) : ''}`
    )
  ),
  transports: [
    new transports.Console({ level: LOG_LEVEL }),
    fileRotateTransport
  ]
});

// ---------------------- App / Config ----------------------
const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;
const NODE_ENV = process.env.NODE_ENV || 'development';

const MQTT_ENABLED = process.env.MQTT_ENABLED !== 'false';
const MQTT_PUBLISHER_ENABLED = process.env.MQTT_PUBLISHER_ENABLED !== 'false';

const MQTT_BROKER_URL = process.env.MQTT_BROKER_URL || 'mqtt://test.mosquitto.org:1883';
const MQTT_USERNAME = process.env.MQTT_USERNAME || '';
const MQTT_PASSWORD = process.env.MQTT_PASSWORD || '';

const POSITION_THROTTLE_MS = parseInt(process.env.POSITION_THROTTLE_MS || '1000', 10); // min interval
const INACTIVITY_THRESHOLD_MS = parseInt(process.env.INACTIVITY_THRESHOLD_MS || (5 * 60 * 1000), 10);
const MAX_PENDING_MESSAGES = parseInt(process.env.MAX_PENDING_MESSAGES || '500', 10);

// ---------------------- Redis ----------------------
const redis = new Redis(process.env.REDIS_URL);
redis.on('connect', () => logger.info('Connecté à Redis'));
redis.on('error', err => logger.error('Erreur Redis:', err.message));

// ---------------------- MQTT / State ----------------------
let mqttClient = null;
let mqttPublisher = null;
const pendingMessages = []; // FIFO
const subscribedReservationTopics = new Set(); // on track uniquement des topics de réservation spécifiques

// caches en mémoire pour limiter accès Redis
const lastPositionCache = new Map(); // chauffeurId -> { lat, lng, ts }
const lastStatusPublishTs = new Map(); // chauffeurId -> timestamp

// Topics (centralisé, pas de doublons)
const RESERVATIONS_RECENTES_TOPIC = 'ktur/reservations/recentes';
const RESERVATION_TOPIC_PREFIX = 'ktur/reservations/'; // + id
const STATUS_TOPIC_WILDCARD = 'chauffeur/+/status';
const POSITION_TOPIC_WILDCARD = 'chauffeur/+/position';
const RESERVATION_POSITION_TOPIC_WILDCARD = 'ktur/reservations/+/position';

// ---------------------- Helpers ----------------------
function safeJsonParse(buf) {
  try {
    return JSON.parse(buf.toString());
  } catch (e) {
    return null;
  }
}

function distanceChanged(a, b) {
  // compare stringified numbers to avoid float jitter; can be improved (haversine) if besoin
  return (!a || a.lat !== b.lat || a.lng !== b.lng);
}

async function scanKeys(matchPattern) {
  const found = [];
  let cursor = '0';
  do {
    const [nextCursor, keys] = await redis.scan(cursor, 'MATCH', matchPattern, 'COUNT', 100);
    cursor = nextCursor;
    if (keys && keys.length) found.push(...keys);
  } while (cursor !== '0');
  return found;
}

// Redis write helper (pipeline)
async function redisHSetMulti(key, obj) {
  const pipeline = redis.multi();
  pipeline.hmset(key, obj);
  await pipeline.exec();
}

// ---------------------- MQTT publisher queue ----------------------
function enqueuePending(topic, payload, options = { qos: 1, retain: false }) {
  if (pendingMessages.length >= MAX_PENDING_MESSAGES) {
    // drop oldest to keep memory bounded
    pendingMessages.shift();
    logger.warn('File d\'attente MQTT pleine - suppression du plus ancien');
  }
  pendingMessages.push({ topic, payload, options });
}

function processPendingMessages() {
  if (!mqttPublisher || !mqttPublisher.connected) return;
  while (pendingMessages.length) {
    const m = pendingMessages.shift();
    try {
      mqttPublisher.publish(m.topic, m.payload, m.options);
    } catch (err) {
      logger.error('Erreur publication message différé:', err.message);
      // remonter en tête et sortir (retry plus tard)
      pendingMessages.unshift(m);
      break;
    }
  }
}

// ---------------------- MQTT init ----------------------
function createMqttClient(clientIdSuffix, extra = {}) {
  return mqtt.connect(MQTT_BROKER_URL, {
    username: MQTT_USERNAME || undefined,
    password: MQTT_PASSWORD || undefined,
    reconnectPeriod: 5000,
    connectTimeout: 30000,
    keepalive: 60,
    clean: true,
    clientId: `ktur_${clientIdSuffix}_${Math.random().toString(16).slice(2,8)}`,
    ...extra
  });
}

function initializeMQTT() {
  if (!MQTT_ENABLED) {
    logger.warn('MQTT désactivé');
    return;
  }

  // Listener (s'abonne aux wildcards utiles)
  mqttClient = createMqttClient('listener');
  mqttClient.on('connect', () => {
    logger.info('MQTT Listener connecté');
    // abonnements "génériques"
    mqttClient.subscribe([RESERVATIONS_RECENTES_TOPIC, STATUS_TOPIC_WILDCARD, POSITION_TOPIC_WILDCARD], { qos: 1 }, (err) => {
      if (err) logger.error('Erreur abonnement wildcard MQTT:', err.message);
      else logger.info('Abonnements wildcard MQTT effectués');
    });
    processPendingMessages();
  });

  mqttClient.on('message', onMqttMessage);
  mqttClient.on('error', e => logger.error('MQTT Listener err:', e.message));
  mqttClient.on('close', () => logger.info('MQTT Listener fermé'));
  mqttClient.on('offline', () => logger.warn('MQTT Listener hors ligne'));

  // Publisher (séparé pour statut/position)
  if (MQTT_PUBLISHER_ENABLED) {
    mqttPublisher = createMqttClient('publisher', {
      will: {
        topic: 'ktur/server/status',
        payload: JSON.stringify({ status: 'offline', timestamp: Date.now() }),
        qos: 1,
        retain: true
      }
    });

    mqttPublisher.on('connect', () => {
      logger.info('MQTT Publisher connecté');
      mqttPublisher.publish('ktur/server/status', JSON.stringify({ status: 'online', timestamp: Date.now() }), { qos: 1, retain: true });
      processPendingMessages();
    });

    mqttPublisher.on('error', e => logger.error('MQTT Publisher err:', e.message));
    mqttPublisher.on('close', () => logger.info('MQTT Publisher fermé'));
    mqttPublisher.on('offline', () => logger.warn('MQTT Publisher hors ligne'));
  } else {
    logger.warn('Publisher MQTT désactivé');
  }
}

// ---------------------- Message handling ----------------------
async function onMqttMessage(topic, messageBuf) {
  const data = safeJsonParse(messageBuf);
  if (!data) {
    logger.warn('Message MQTT non JSON ignoré', { topic });
    return;
  }

  try {
    // shortcuts to avoid repeated work
    if (topic === RESERVATIONS_RECENTES_TOPIC) {
      await handleNewReservation(data);
      return;
    }

    if (topic.startsWith(RESERVATION_TOPIC_PREFIX)) {
      const parts = topic.split('/');
      const reservationId = parts[2];
      await handleReservationMessage(reservationId, data);
      return;
    }

    // chauffeur topics (wildcards cover many cases)
    if (/^chauffeur\/.+\/status$/.test(topic)) {
      const chauffeurId = topic.split('/')[1];
      await handleChauffeurStatusUpdate(chauffeurId, data);
      return;
    }

    if (/^chauffeur\/.+\/position$/.test(topic)) {
      const chauffeurId = topic.split('/')[1];
      const position = data.data || data;
      await handlePosition(chauffeurId, position);
      return;
    }

    if (/^ktur\/reservations\/.+\/position$/.test(topic)) {
      const reservationId = topic.split('/')[2];
      await handleReservationPosition(reservationId, data);
      return;
    }

    // fallback: log as debug only
    logger.debug('Topic MQTT inconnu reçu', { topic });
  } catch (err) {
    logger.error('Erreur traitement message MQTT', err.message || err);
  }
}

// ---------------------- Handlers (essentiels) ----------------------
async function handleNewReservation(data) {
  logger.info(`Nouvelle réservation reçue: ${data.reservation_id || 'unknown'}`);
  // conserve comportement minimal pour compatibilité
}

async function handleReservationMessage(reservationId, data) {
  switch (data.type) {
    case 'chat': await handleChatMessage(reservationId, data); break;
    case 'position':
    case 'reservation_position': await handleReservationPosition(reservationId, data); break;
    case 'acceptation': await handleReservationAcceptance(reservationId, data); break;
    case 'debut':
    case 'fin': await handleReservationStatusChange(reservationId, data); break;
    default:
      logger.warn('Type message réservation non géré', { reservationId, type: data.type });
  }
}

async function handleChatMessage(reservationId, data) {
  const key = `chat:${reservationId}:messages`;
  // push en tête (fast), on archive plus tard si trop long
  await redis.lpush(key, JSON.stringify({
    from: data.from,
    content: data.content,
    timestamp: Date.now()
  }));
  const len = await redis.llen(key);
  if (len >= 100) {
    await archiveChatMessages(reservationId);
  }
}

async function archiveChatMessages(reservationId) {
  const key = `chat:${reservationId}:messages`;
  const messages = await redis.lrange(key, 0, -1);
  try {
    await axios.post(`${process.env.LARAVEL_API_URL}/api/chat/archive`, {
      reservation_id: reservationId,
      messages: messages.map(m => JSON.parse(m))
    }, { headers: { 'Content-Type': 'application/json' }});
    await redis.del(key);
    logger.info('Chat archivé', { reservationId });
  } catch (err) {
    logger.error('Erreur archivage chat', err.message || err);
  }
}

async function handleReservationPosition(reservationId, data) {
  const key = `reservation:${reservationId}:position`;
  const positionData = {
    lat: data.lat,
    lng: data.lng,
    chauffeur_id: data.chauffeur_id,
    reservation_status: data.reservation_status || 'active',
    is_in_reservation: '1',
    updated_at: Date.now(),
    accuracy: data.accuracy || '',
    speed: data.speed || '',
    heading: data.heading || ''
  };
  await redisHSetMulti(key, positionData);

  // publier (si possible) mais sans flooding de logs
  const topic = `ktur/reservations/${reservationId}/position`;
  const payload = JSON.stringify({
    type: 'reservation_position',
    reservation_id: reservationId,
    chauffeur_id: data.chauffeur_id,
    position: { lat: data.lat, lng: data.lng, accuracy: data.accuracy },
    timestamp: Date.now()
  });
  if (!mqttPublisher || !mqttPublisher.connected) {
    enqueuePending(topic, payload);
  } else {
    mqttPublisher.publish(topic, payload, { qos: 1 });
  }
}

async function handleReservationAcceptance(reservationId, data) {
  try {
    const reservationTopic = `${RESERVATION_TOPIC_PREFIX}${reservationId}`;
    if (!subscribedReservationTopics.has(reservationTopic)) {
      // on garde ces subscriptions uniquement pour chats/reservations spécifiques
      mqttClient.subscribe(reservationTopic, { qos: 1 }, (err) => {
        if (!err) {
          subscribedReservationTopics.add(reservationTopic);
          logger.info('Abonné au topic réservation', reservationTopic);
        } else logger.error('Erreur abonnement reservation topic', err.message);
      });
    }

    // notifier Laravel
    await notifyLaravel('/reservation/acceptation', { reservation_id: reservationId, chauffeur_id: data.chauffeur_id });

    // mettre à jour statut
    await updateStatut(data.chauffeur_id, { en_ligne: true, en_course: true, disponible: false });

    // publication serveur (évite boucle car publish handler ignore source:server)
    await publishChauffeurStatus(data.chauffeur_id, { source: 'server' });

    logger.info('Réservation acceptée', { reservationId, chauffeur: data.chauffeur_id });
  } catch (err) {
    logger.error('Erreur acceptation réservation', err.message);
  }
}

async function handleReservationStatusChange(reservationId, data) {
  const endpoint = data.type === 'debut' ? '/reservation/debut' : '/reservation/fin';
  await notifyLaravel(endpoint, { reservation_id: reservationId });
  if (data.type === 'fin') {
    await cleanupReservation(reservationId);
  }
}

async function cleanupReservation(reservationId) {
  const chatKey = `chat:${reservationId}:messages`;
  if (await redis.exists(chatKey)) await archiveChatMessages(reservationId);
  await redis.del(`reservation:${reservationId}:position`);
  const topic = `${RESERVATION_TOPIC_PREFIX}${reservationId}`;
  if (subscribedReservationTopics.has(topic)) {
    mqttClient.unsubscribe(topic, () => {});
    subscribedReservationTopics.delete(topic);
  }
}

// ---------------------- notify/update/publish helpers ----------------------
async function notifyLaravel(endpoint, payload) {
  try {
    await axios.post(`${process.env.LARAVEL_API_URL}${endpoint}`, payload, {
      headers: { 'Content-Type': 'application/json' }
    });
    logger.debug('Notification envoyée à Laravel', { endpoint });
  } catch (err) {
    logger.error('Erreur appel Laravel', err.response ? err.response.data : err.message);
  }
}

async function updateStatut(chauffeurId, fields) {
  const key = `chauffeur:${chauffeurId}`;
  const mapping = {
    disponible: fields.disponible ? '1' : (fields.disponible === false ? '0' : undefined),
    en_ligne: fields.en_ligne ? '1' : (fields.en_ligne === false ? '0' : undefined),
    en_course: fields.en_course ? '1' : (fields.en_course === false ? '0' : undefined),
    updated_at: Date.now()
  };
  // clean undefined
  Object.keys(mapping).forEach(k => mapping[k] === undefined && delete mapping[k]);
  await redisHSetMulti(key, mapping);
  await publishChauffeurStatus(chauffeurId, { source: 'server' });
}

async function publishChauffeurStatus(chauffeurId, options = {}) {
  if (!MQTT_ENABLED) return;
  const now = Date.now();
  const lastTs = lastStatusPublishTs.get(chauffeurId) || 0;
  // throttle status publications (ex: 1s)
  if (now - lastTs < 500) return;
  lastStatusPublishTs.set(chauffeurId, now);

  const key = `chauffeur:${chauffeurId}`;
  const statut = await redis.hgetall(key);
  if (!statut || Object.keys(statut).length === 0) return;

  const statusData = {
    chauffeur_id: chauffeurId,
    disponible: statut.disponible === '1',
    en_ligne: statut.en_ligne === '1',
    en_course: statut.en_course === '1',
    latitude: parseFloat(statut.latitude) || null,
    longitude: parseFloat(statut.longitude) || null,
    updated_at: parseInt(statut.updated_at) || Date.now(),
    timestamp: new Date().toISOString(),
    source: options.source || 'client'
  };

  const topic = `chauffeur/${chauffeurId}/status`;
  const payload = JSON.stringify(statusData);
  if (!mqttPublisher || !mqttPublisher.connected) {
    enqueuePending(topic, payload);
  } else {
    mqttPublisher.publish(topic, payload, { qos: 1 });
  }
}

// Generic publisher used elsewhere
async function publishMQTTMessage(topic, payload, options = { qos: 1, retain: false }) {
  if (!mqttPublisher || !mqttPublisher.connected) {
    enqueuePending(topic, payload, options);
  } else {
    mqttPublisher.publish(topic, payload, options);
  }
}

// Publier position générique (avec throttling en mémoire)
async function publishChauffeurPosition(chauffeurId, lat, lng) {
  const now = Date.now();
  const cached = lastPositionCache.get(chauffeurId);
  const newPos = { lat: String(lat), lng: String(lng), ts: now };

  if (cached) {
    if (!distanceChanged(cached, newPos) && (now - cached.ts) < POSITION_THROTTLE_MS) {
      // pas de changement significatif, on ignorer
      return;
    }
  }
  lastPositionCache.set(chauffeurId, newPos);

  const topic = `chauffeur/${chauffeurId}/position`;
  const payload = JSON.stringify({ type: 'general_position', chauffeur_id: chauffeurId, data: { lat, lng, timestamp: now } });
  await publishMQTTMessage(topic, payload);
}

// ---------------------- Position handler ----------------------
async function handlePosition(id, positionData) {
  if (!positionData || typeof positionData.lat !== 'number' || typeof positionData.lng !== 'number') {
    logger.warn('Position invalide ignorée', { id });
    return;
  }

  try {
    const prev = lastPositionCache.get(id);
    const newPos = { lat: String(positionData.lat), lng: String(positionData.lng) };
    if (prev && !distanceChanged(prev, newPos) && (Date.now() - prev.ts) < POSITION_THROTTLE_MS) {
      // éviter écritures/redondances
      return;
    }

    // pipeline Redis
    const key = `chauffeur:${id}`;
    await redisHSetMulti(key, {
      latitude: positionData.lat,
      longitude: positionData.lng,
      accuracy: positionData.accuracy || '',
      speed: positionData.speed || '',
      heading: positionData.heading || '',
      is_in_reservation: '0',
      updated_at: Date.now()
    });

    // mettre cache et publier
    lastPositionCache.set(id, { ...newPos, ts: Date.now() });
    await publishChauffeurPosition(id, positionData.lat, positionData.lng);
    await publishChauffeurStatus(id, { source: 'server' });
  } catch (err) {
    logger.error('Erreur handlePosition', err.message || err);
  }
}

// ---------------------- Status update from chauffeurs ----------------------
async function handleChauffeurStatusUpdate(chauffeurId, data) {
  try {
    // ignorer messages provenant du serveur
    if (data.source === 'server' || data.is_server_message) return;

    const isOnline = data.statut === 1 || data.en_ligne === true;
    await redisHSetMulti(`chauffeur:${chauffeurId}`, {
      en_ligne: isOnline ? '1' : '0',
      disponible: isOnline ? '1' : '0',
      en_course: '0',
      updated_at: Date.now()
    });

    if (data.position && data.position.latitude && data.position.longitude) {
      await redisHSetMulti(`chauffeur:${chauffeurId}`, {
        latitude: data.position.latitude,
        longitude: data.position.longitude,
        updated_at: Date.now()
      });
    }

    // republier depuis le server pour synchronisation (throttled inside)
    await publishChauffeurStatus(chauffeurId, { source: 'server' });
  } catch (err) {
    logger.error('Erreur handleChauffeurStatusUpdate', err.message || err);
  }
}

// ---------------------- Inactive check (scan-based, non bloquant) ----------------------
async function checkInactiveChauffeurs() {
  try {
    const keys = await scanKeys('chauffeur:*');
    const now = Date.now();
    for (const key of keys) {
      const [, chauffeurId] = key.split(':');
      const chauffeur = await redis.hgetall(key);
      if (!chauffeur || Object.keys(chauffeur).length === 0) continue;
      const last = parseInt(chauffeur.updated_at || '0', 10);
      if (chauffeur.en_ligne === '1' && (now - last) > INACTIVITY_THRESHOLD_MS) {
        await redisHSetMulti(key, { en_ligne: '0', disponible: '0', updated_at: now });
        await publishChauffeurStatus(chauffeurId, { source: 'server' });
        logger.info('Chauffeur passé hors ligne pour inactivité', { chauffeurId });
      }
    }
  } catch (err) {
    logger.error('Erreur checkInactiveChauffeurs', err.message || err);
  }
}

setInterval(checkInactiveChauffeurs, 60 * 1000);

// ---------------------- Endpoints ----------------------
app.post('/api/ecouter-topic', (req, res) => {
  const topic = req.body.topic;
  if (!MQTT_ENABLED || !mqttClient) return res.status(503).json({ message: 'MQTT non disponible' });
  if (!topic) return res.status(400).json({ message: 'Topic invalide' });

  // seulement pour des topics non-reservation (reservation gérés ailleurs) : abonnement unique
  mqttClient.subscribe(topic, { qos: 1 }, (err) => {
    if (err) return res.status(500).json({ message: 'Erreur abonnement' });
    return res.json({ message: `Abonné à ${topic}` });
  });
});

app.get('/api/mqtt/status', (req, res) => {
  res.json({
    mqtt_enabled: MQTT_ENABLED,
    publisher_enabled: MQTT_PUBLISHER_ENABLED,
    listener_connected: !!(mqttClient && mqttClient.connected),
    publisher_connected: !!(mqttPublisher && mqttPublisher.connected),
    pending_messages: pendingMessages.length,
    subscribed_reservations: Array.from(subscribedReservationTopics)
  });
});

app.post('/api/reconnect-publisher', (req, res) => {
  try {
    if (!MQTT_ENABLED) return res.status(400).json({ message: 'MQTT désactivé' });
    if (mqttPublisher) mqttPublisher.end(true);
    mqttPublisher = null;
    // recreate
    mqttPublisher = createMqttClient('publisher', {
      will: {
        topic: 'ktur/server/status',
        payload: JSON.stringify({ status: 'offline', timestamp: Date.now() }),
        qos: 1,
        retain: true
      }
    });
    return res.json({ message: 'Reconnexion publisher initiée' });
  } catch (err) {
    logger.error('Erreur reconnexion publisher', err.message || err);
    return res.status(500).json({ message: 'Erreur' });
  }
});

app.post('/api/desabonner-topic', async (req, res) => {
  const { topic } = req.body;
  if (!MQTT_ENABLED || !mqttClient) return res.status(503).json({ message: 'MQTT non disponible' });
  if (!topic) return res.status(400).json({ message: 'Topic invalide' });

  mqttClient.unsubscribe(topic, {}, async (err) => {
    if (err) return res.status(500).json({ message: 'Erreur désabonnement' });
    // si c'était un topic de réservation, on l'enlève du set
    if (topic.startsWith(RESERVATION_TOPIC_PREFIX)) subscribedReservationTopics.delete(topic);
    // si c'est un chauffeur/:id/status, on peut mettre à jour le statut
    const parts = topic.split('/');
    if (parts.length >= 3 && parts[0] === 'chauffeur') {
      const chauffeurId = parts[1];
      await updateStatut(chauffeurId, { disponible: false, en_ligne: false, en_course: false });
    }
    return res.json({ message: `Désabonné de ${topic}` });
  });
});

app.post('/api/chauffeur/:id/subscribe-status', (req, res) => {
  const { id } = req.params;
  const statusTopic = `chauffeur/${id}/status`;
  if (!MQTT_ENABLED || !mqttClient) return res.status(503).json({ message: 'MQTT non disponible' });

  mqttClient.subscribe(statusTopic, { qos: 1 }, (err) => {
    if (err) return res.status(500).json({ message: 'Erreur abonnement' });
    return res.json({ message: `Abonné au topic ${statusTopic}` });
  });
});

app.get('/api/chauffeurs/status', async (req, res) => {
  try {
    const keys = await scanKeys('chauffeur:*');
    const chauffeurs = [];
    for (const k of keys) {
      const id = k.split(':')[1];
      const s = await redis.hgetall(k);
      if (!s || Object.keys(s).length === 0) continue;
      chauffeurs.push({
        id,
        ...s,
        disponible: s.disponible === '1',
        en_ligne: s.en_ligne === '1',
        en_course: s.en_course === '1'
      });
    }
    res.json({ chauffeurs });
  } catch (err) {
    logger.error('Erreur récupération statuts', err.message || err);
    res.status(500).json({ error: 'Erreur serveur' });
  }
});

app.post('/api/chauffeurs/:id/status', async (req, res) => {
  const { id } = req.params;
  const status = req.body;
  if (typeof status.disponible !== 'boolean' || typeof status.en_ligne !== 'boolean') {
    return res.status(400).json({ error: 'Statut invalide' });
  }
  try {
    await updateStatut(id, status);
    const current = await redis.hgetall(`chauffeur:${id}`);
    res.json(current);
  } catch (err) {
    logger.error('Erreur statut', err.message || err);
    res.status(500).json({ error: 'Erreur serveur' });
  }
});

app.post('/api/reservation/subscribe', (req, res) => {
  const { reservation_id } = req.body;
  if (!reservation_id) return res.status(400).json({ message: 'reservation_id manquant' });
  const topic = `${RESERVATION_TOPIC_PREFIX}${reservation_id}`;
  if (!MQTT_ENABLED || !mqttClient) return res.status(503).json({ message: 'MQTT non disponible' });

  mqttClient.subscribe(topic, { qos: 1 }, (err) => {
    if (!err) {
      subscribedReservationTopics.add(topic);
      return res.json({ message: `Abonné au topic ${topic}` });
    } else {
      return res.status(500).json({ error: 'Erreur abonnement topic' });
    }
  });
});

app.post('/api/reservation/send-message', async (req, res) => {
  const { reservation_id, message } = req.body;
  const topic = `${RESERVATION_TOPIC_PREFIX}${reservation_id}`;
  if (!MQTT_PUBLISHER_ENABLED) return res.status(503).json({ error: 'Publisher MQTT non disponible' });
  try {
    const payload = JSON.stringify({ type: 'chat', from: message.from, content: message.content });
    await publishMQTTMessage(topic, payload, { qos: 1 });
    return res.json({ success: true });
  } catch (err) {
    logger.error('Erreur envoi message', err.message || err);
    return res.status(500).json({ error: 'Erreur envoi message' });
  }
});

app.get('/api/chat/history/:reservationId', async (req, res) => {
  const { reservationId } = req.params;
  try {
    const key = `chat:${reservationId}:messages`;
    const messages = await redis.lrange(key, 0, -1);
    const formatted = messages.map(m => {
      try { return JSON.parse(m); } catch (e) { return null; }
    }).filter(Boolean);
    logger.debug('Historique chat récupéré', { reservationId, count: formatted.length });
    res.json({ messages: formatted, reservation_id: reservationId, count: formatted.length });
  } catch (err) {
    logger.error('Erreur récupération historique chat', err.message || err);
    res.status(500).json({ error: 'Erreur serveur' });
  }
});

// ---------------------- Start ----------------------
initializeMQTT();

const server = app.listen(PORT, () => {
  logger.info(`Serveur en écoute sur le port ${PORT}`);
});

// ---------------------- Cleanup ----------------------
function shutdown() {
  logger.info('Arrêt du serveur...');
  if (mqttClient) mqttClient.end(true);
  if (mqttPublisher) mqttPublisher.end(true);
  redis.disconnect();
  server.close(() => process.exit(0));
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

process.on('uncaughtException', (err) => {
  logger.error('Erreur non capturée', err.message || err);
  process.exit(1);
});
process.on('unhandledRejection', (reason) => {
  logger.error('Promesse rejetée non gérée', reason);
  process.exit(1);
});
