require('dotenv').config();
const express = require('express');
const mqtt = require('mqtt');
const Redis = require('ioredis');
const cors = require('cors');
const axios = require('axios');
const { createLogger, format, transports } = require('winston');
require('winston-daily-rotate-file');

// ---------------------- Logger Configuration ----------------------
const LOG_LEVEL = process.env.LOG_LEVEL || 'info';
const IS_DEBUG = LOG_LEVEL.toLowerCase() === 'debug';

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

// ---------------------- App Configuration ----------------------
const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;
const MQTT_ENABLED = process.env.MQTT_ENABLED !== 'false';
const MQTT_PUBLISHER_ENABLED = process.env.MQTT_PUBLISHER_ENABLED !== 'false';
const MQTT_BROKER_URL = 'mqtts://test.mosquitto.org:8883';

const POSITION_THROTTLE_MS = parseInt(process.env.POSITION_THROTTLE_MS || '250', 10);
const INACTIVITY_THRESHOLD_MS = parseInt(process.env.INACTIVITY_THRESHOLD_MS || (5 * 60 * 1000), 10);
const MAX_PENDING_MESSAGES = parseInt(process.env.MAX_PENDING_MESSAGES || '500', 10);

// ---------------------- Redis Configuration ----------------------
const redis = new Redis(process.env.REDIS_URL);
redis.on('connect', () => logger.info('Connecté à Redis'));
redis.on('error', err => logger.error('Erreur Redis:', err.message));

// ---------------------- Global State ----------------------
let mqttClient = null;
let mqttPublisher = null;
const pendingMessages = [];
const subscribedReservationTopics = new Set();
const lastPositionCache = new Map();
const lastStatusPublishTs = new Map();

// Topics constants
const RESERVATIONS_RECENTES_TOPIC = 'ktur/reservations/recentes';
const RESERVATION_TOPIC_PREFIX = 'ktur/reservations/';
const STATUS_TOPIC_WILDCARD = 'chauffeur/+/status';
const POSITION_TOPIC_WILDCARD = 'chauffeur/+/position';
const PASSAGER_STATUS_TOPIC = 'passager_mobile/status';
const PASSAGER_POSITION_TOPIC = 'passager_mobile/position';

// ---------------------- Utility Functions ----------------------
function safeJsonParse(buffer) {
  try {
    return JSON.parse(buffer.toString());
  } catch (e) {
    logger.warn('Message MQTT non-JSON ignoré');
    return null;
  }
}

function hasPositionChanged(previous, current) {
  return !previous || previous.lat !== current.lat || previous.lng !== current.lng;
}

async function scanRedisKeys(pattern) {
  const keys = [];
  let cursor = '0';
  do {
    const [nextCursor, foundKeys] = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
    cursor = nextCursor;
    if (foundKeys?.length) keys.push(...foundKeys);
  } while (cursor !== '0');
  return keys;
}

async function redisUpdate(key, data) {
  const pipeline = redis.multi();
  pipeline.hmset(key, data);
  await pipeline.exec();
}

// ---------------------- MQTT Message Queue ----------------------
function enqueuePendingMessage(topic, payload, options = { qos: 1, retain: false }) {
  if (pendingMessages.length >= MAX_PENDING_MESSAGES) {
    const removed = pendingMessages.shift();
    logger.warn('File d\'attente MQTT pleine, suppression du plus ancien', { removedTopic: removed.topic });
  }

  pendingMessages.push({ topic, payload, options });
  if (IS_DEBUG) {
    logger.debug('Message mis en file d\'attente', { topic, queueSize: pendingMessages.length });
  }
}

function processPendingMessages() {
  if (!mqttPublisher?.connected) return;

  logger.info(`Traitement de ${pendingMessages.length} messages en attente`);

  while (pendingMessages.length > 0) {
    const message = pendingMessages.shift();
    try {
      mqttPublisher.publish(message.topic, message.payload, message.options);
    } catch (err) {
      logger.error('Erreur publication message différé', { topic: message.topic, error: err.message });
      pendingMessages.unshift(message);
      break;
    }
  }
}

// ---------------------- MQTT Client Creation ----------------------
function createMqttClient(role, extraOptions = {}) {
  const clientId = `ktur_${role}_${Math.random().toString(16).slice(2, 8)}`;

  const options = {
    username: '',
    password: '',
    reconnectPeriod: 5000,
    connectTimeout: 30000,
    keepalive: 60,
    clean: true,
    clientId,
    rejectUnauthorized: false,
    protocolVersion: 4,
    protocolId: 'MQTT',
    ...extraOptions
  };

  logger.info('Création client MQTT', { role, clientId });
  return mqtt.connect(MQTT_BROKER_URL, options);
}

// ---------------------- MQTT Initialization ----------------------
function initializeMQTT() {
  if (!MQTT_ENABLED) {
    logger.warn('MQTT désactivé');
    return;
  }

  // Listener client
  mqttClient = createMqttClient('listener');

  mqttClient.on('connect', () => {
    logger.info('MQTT Listener connecté');

    const topics = [
      RESERVATIONS_RECENTES_TOPIC,
      STATUS_TOPIC_WILDCARD,
      POSITION_TOPIC_WILDCARD,
      PASSAGER_STATUS_TOPIC,
      PASSAGER_POSITION_TOPIC,
    ];

    mqttClient.subscribe(topics, { qos: 1 }, (err) => {
      if (err) {
        logger.error('Erreur abonnement topics principaux:', err.message);
      } else {
        logger.info('Abonnements MQTT effectués', { topics });
      }
    });

    processPendingMessages();
  });

  mqttClient.on('message', handleMqttMessage);
  mqttClient.on('error', err => logger.error('MQTT Listener erreur:', err.message));
  mqttClient.on('close', () => logger.info('MQTT Listener fermé'));
  mqttClient.on('offline', () => logger.warn('MQTT Listener hors ligne'));

  // Publisher client
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
      mqttPublisher.publish('ktur/server/status',
        JSON.stringify({ status: 'online', timestamp: Date.now() }),
        { qos: 1, retain: true }
      );
      processPendingMessages();
    });

    mqttPublisher.on('error', err => logger.error('MQTT Publisher erreur:', err.message));
    mqttPublisher.on('close', () => logger.info('MQTT Publisher fermé'));
    mqttPublisher.on('offline', () => logger.warn('MQTT Publisher hors ligne'));
  }
}

// ---------------------- Message Handlers ----------------------
async function handleMqttMessage(topic, messageBuffer) {
  // Log raw reception with topic and payload size
  try {
    logger.info('MQTT message reçu', { topic, bytes: messageBuffer?.length || 0 });
  } catch (_) {}

  const data = safeJsonParse(messageBuffer);
  if (!data) return;

  try {
    // Optional: log parsed message meta
    if (typeof data === 'object') {
      const meta = {
        topic,
        type: data.type || null,
        keys: Object.keys(data || {}).slice(0, 6)
      };
      logger.info('MQTT message parsé', meta);
    }

    // Route messages based on topic patterns
    if (topic === RESERVATIONS_RECENTES_TOPIC) {
      await handleNewReservation(data);
    } else if (topic === PASSAGER_STATUS_TOPIC) {
      await handlePassagerStatus(data);
    } else if (topic === PASSAGER_POSITION_TOPIC) {
      await handlePassagerPosition(data);
    } else if (topic.startsWith(RESERVATION_TOPIC_PREFIX)) {
      const reservationId = topic.split('/')[2];
      await handleReservationMessage(reservationId, data);
    } else if (/^chauffeur\/.+\/status$/.test(topic)) {
      const chauffeurId = topic.split('/')[1];
      await handleChauffeurStatusUpdate(chauffeurId, data);
    } else if (/^chauffeur\/.+\/position$/.test(topic)) {
      const chauffeurId = topic.split('/')[1];
      await handleChauffeurPosition(chauffeurId, data.data || data);
    } else if (/^ktur\/reservations\/.+\/position$/.test(topic)) {
      const reservationId = topic.split('/')[2];
      await handleReservationPosition(reservationId, data);
    }
  } catch (err) {
    logger.error('Erreur traitement message MQTT', { topic, error: err.message });
  }
}

async function handleNewReservation(data) {
  logger.info('Nouvelle réservation reçue', { reservationId: data.reservation_id });
}

async function handleReservationMessage(reservationId, data) {
  switch (data.type) {
    case 'chat':
      await handleChatMessage(reservationId, data);
      break;
    case 'position':
    case 'reservation_position':
      await handleReservationPosition(reservationId, data);
      break;
    case 'acceptation':
      if (data.action === 'start') {
        await handleReservationStart(reservationId, data);
      } else {
        await handleReservationAcceptance(reservationId, data);
      }
      break;
    case 'fin':
      await handleReservationEnd(reservationId, data);
      break;
    default:
      if (IS_DEBUG) {
        logger.debug('Type message réservation non géré', { reservationId, type: data.type });
      }
  }
}

// ---------------------- Chauffeur Handlers ----------------------
async function handleChauffeurPosition(chauffeurId, positionData) {
  if (!positionData || typeof positionData.lat !== 'number' || typeof positionData.lng !== 'number') {
    return;
  }

  const now = Date.now();
  const cached = lastPositionCache.get(chauffeurId);
  const newPosition = { lat: positionData.lat, lng: positionData.lng, ts: now };

  // Throttling: skip if position unchanged and within throttle period
  if (cached && !hasPositionChanged(cached, newPosition) && (now - cached.ts) < POSITION_THROTTLE_MS) {
    try {
      await redisUpdate(`chauffeur:${chauffeurId}`, {
        en_ligne: '1',
        updated_at: now
      });
      await publishChauffeurStatus(chauffeurId, { source: 'server' });
    } catch (err) {
      logger.error('Erreur mise à jour throttle position chauffeur', { chauffeurId, error: err.message });
    }
    return;
  }

  try {
    // Update position and set driver online; do not force availability here
    const key = `chauffeur:${chauffeurId}`;
    const positionUpdate = {
      latitude: positionData.lat,
      longitude: positionData.lng,
      accuracy: positionData.accuracy || '',
      speed: positionData.speed || '',
      heading: positionData.heading || '',
      en_ligne: '1',
      updated_at: now
    };
    logger.info('Préparation mise à jour position chauffeur', { key, chauffeurId, update: positionUpdate });
    await redisUpdate(key, positionUpdate);
    const after = await redis.hgetall(key);
    logger.info('Mise à jour Redis position chauffeur effectuée', { key, fields: Object.keys(after) });

    lastPositionCache.set(chauffeurId, newPosition);

    // Publish position and status updates
    await publishChauffeurPosition(chauffeurId, positionData.lat, positionData.lng);
    await publishChauffeurStatus(chauffeurId, { source: 'server' });

    if (IS_DEBUG) {
      logger.debug('Position chauffeur mise à jour', { chauffeurId });
    }
  } catch (err) {
    logger.error('Erreur mise à jour position chauffeur', { chauffeurId, error: err.message });
  }
}

async function handleChauffeurStatusUpdate(chauffeurId, data) {
  // Ignore server-generated messages to prevent loops
  if (data.source === 'server' || data.is_server_message) {
    return;
  }

  try {
    const toBool = (v) => v === true || v === 1 || v === '1' || v === 'true' || v === 'online';
    const isOnline = toBool(data.statut) || toBool(data.en_ligne) || toBool(data.status) || toBool(data.online) || toBool(data.enLigne);

    // Read current state to avoid overwriting unspecified fields
    const current = await redis.hgetall(`chauffeur:${chauffeurId}`);

    // Determine en_course and disponible from payload with sensible defaults
    const hasEnCourse = (data.en_course !== undefined) || (data.in_course !== undefined) || (data.busy !== undefined) || (data.state !== undefined);
    const enCourseFromPayload = toBool(data.en_course) || toBool(data.in_course) || toBool(data.busy) || (String(data.state).toLowerCase() === 'in_course');

    let enCourse = hasEnCourse ? enCourseFromPayload : (current.en_course === '1');

    let disponible;
    if (data.disponible !== undefined) {
      disponible = toBool(data.disponible);
    } else if (hasEnCourse) {
      disponible = isOnline && !enCourse;
    } else {
      // default: if online and not explicitly in course, keep current disponible if exists else true
      disponible = (current.disponible !== undefined) ? (current.disponible === '1') : isOnline;
    }

    const statusUpdate = {
      en_ligne: isOnline ? '1' : '0',
      disponible: disponible ? '1' : '0',
      en_course: enCourse ? '1' : '0',
      updated_at: Date.now()
    };
    logger.info('Statut chauffeur calculé', { chauffeurId, isOnline, disponible, enCourse, statusUpdate });

    // Update position if provided
    if (data.position) {
      if (typeof data.position.latitude === 'number' && typeof data.position.longitude === 'number') {
        statusUpdate.latitude = data.position.latitude;
        statusUpdate.longitude = data.position.longitude;
      } else if (typeof data.position.lat === 'number' && typeof data.position.lng === 'number') {
        statusUpdate.latitude = data.position.lat;
        statusUpdate.longitude = data.position.lng;
      }
    } else if (typeof data.latitude === 'number' && typeof data.longitude === 'number') {
      statusUpdate.latitude = data.latitude;
      statusUpdate.longitude = data.longitude;
    } else if (typeof data.lat === 'number' && typeof data.lng === 'number') {
      statusUpdate.latitude = data.lat;
      statusUpdate.longitude = data.lng;
    }

    const key = `chauffeur:${chauffeurId}`;
    logger.info('Préparation mise à jour statut chauffeur', { key, chauffeurId, update: statusUpdate });
    await redisUpdate(key, statusUpdate);
    const after = await redis.hgetall(key);
    logger.info('Mise à jour Redis statut chauffeur effectuée', { key, fields: Object.keys(after), en_ligne: after.en_ligne, disponible: after.disponible, en_course: after.en_course });
    await publishChauffeurStatus(chauffeurId, { source: 'server' });

    logger.info('Statut chauffeur mis à jour', { chauffeurId, en_ligne: isOnline });
  } catch (err) {
    logger.error('Erreur mise à jour statut chauffeur', { chauffeurId, error: err.message });
  }
}

// ---------------------- Reservation Handlers ----------------------
async function handleReservationAcceptance(reservationId, data) {
  try {
    const reservationTopic = `${RESERVATION_TOPIC_PREFIX}${reservationId}`;

    if (!subscribedReservationTopics.has(reservationTopic)) {
      mqttClient.subscribe(reservationTopic, { qos: 1 }, (err) => {
        if (!err) {
          subscribedReservationTopics.add(reservationTopic);
          logger.info('Abonné au topic réservation', { topic: reservationTopic });
        }
      });
    }

    // Notify Laravel
    await notifyLaravel('/reservation/acceptation', {
      resa_id: reservationId,
      chauffeur_id: data.chauffeur_id
    });

    // Update driver status: online, in course, not available
    await updateChauffeurStatus(data.chauffeur_id, {
      en_ligne: true,
      en_course: true,
      disponible: false
    });

    logger.info('Réservation acceptée', { reservationId, chauffeur: data.chauffeur_id });
  } catch (err) {
    logger.error('Erreur acceptation réservation', { reservationId, error: err.message });
  }
}

async function handleReservationStart(reservationId, data) {
  try {
    await notifyLaravel('/reservation/acceptation', {
      resa_id: reservationId,
      chauffeur_id: data.chauffeur_id,
      action: 'start'
    });

    await updateChauffeurStatus(data.chauffeur_id, {
      en_ligne: true,
      en_course: true,
      disponible: false
    });

    const topic = `${RESERVATION_TOPIC_PREFIX}${reservationId}`;
    const payload = JSON.stringify({
      type: 'course_started',
      reservation_id: reservationId,
      chauffeur_id: data.chauffeur_id,
      timestamp: Date.now()
    });

    await publishMqttMessage(topic, payload, { qos: 1 });
    logger.info('Course démarrée', { reservationId, chauffeur: data.chauffeur_id });
  } catch (err) {
    logger.error('Erreur démarrage course', { reservationId, error: err.message });
  }
}

async function handleReservationEnd(reservationId, data) {
  try {
    await notifyLaravel('/reservation/fin', { resa_id: reservationId });

    // Set driver back to available
    if (data.chauffeur_id) {
      await updateChauffeurStatus(data.chauffeur_id, {
        en_ligne: true,
        en_course: false,
        disponible: true
      });
    }

    await cleanupReservation(reservationId);
    logger.info('Réservation terminée', { reservationId });
  } catch (err) {
    logger.error('Erreur fin réservation', { reservationId, error: err.message });
  }
}

async function handleReservationPosition(reservationId, data) {
  const position = data.position || data.data || data;

  if (!position?.lat || !position?.lng) {
    return;
  }

  try {
    const key = `reservation:${reservationId}:position`;
    const positionData = {
      lat: position.lat,
      lng: position.lng,
      chauffeur_id: data.chauffeur_id,
      reservation_status: data.reservation_status || 'active',
      is_in_reservation: '1',
      updated_at: Date.now(),
      accuracy: position.accuracy || '',
      speed: position.speed || '',
      heading: position.heading || ''
    };

    await redisUpdate(key, positionData);

    // Update driver position and status
    if (positionData.chauffeur_id) {
      await redisUpdate(`chauffeur:${positionData.chauffeur_id}`, {
        latitude: positionData.lat,
        longitude: positionData.lng,
        en_ligne: '1',
        disponible: '0',
        en_course: '1',
        updated_at: Date.now()
      });

      await publishChauffeurStatus(positionData.chauffeur_id, { source: 'server' });
    }

    // Publish reservation position update
    const topic = `ktur/reservations/${reservationId}/position`;
    const payload = JSON.stringify({
      type: 'reservation_position',
      reservation_id: reservationId,
      chauffeur_id: positionData.chauffeur_id,
      position: { lat: positionData.lat, lng: positionData.lng, accuracy: positionData.accuracy },
      timestamp: new Date().toISOString()
    });

    await publishMqttMessage(topic, payload, { qos: 1 });

    if (IS_DEBUG) {
      logger.debug('Position réservation enregistrée', { reservationId });
    }
  } catch (err) {
    logger.error('Erreur position réservation', { reservationId, error: err.message });
  }
}

// ---------------------- Passager Handlers ----------------------
async function handlePassagerStatus(data) {
  try {
    const passagerId = data.passager_id || data.client_id || 'unknown';
    const key = `passager:${passagerId}`;

    await redisUpdate(key, {
      en_ligne: data.status === 'online' ? '1' : '0',
      last_status: data.status || 'unknown',
      updated_at: Date.now()
    });

    if (IS_DEBUG) {
      logger.debug('Statut passager enregistré', { passagerId, status: data.status });
    }
  } catch (err) {
    logger.error('Erreur statut passager', { error: err.message });
  }
}

async function handlePassagerPosition(data) {
  try {
    const passagerId = data.passager_id || data.client_id || 'unknown';
    const position = data.data || data.position || data;

    if (!position?.lat || !position?.lng) return;

    const key = `passager:${passagerId}:position`;
    await redisUpdate(key, {
      latitude: position.lat,
      longitude: position.lng,
      accuracy: position.accuracy || '',
      speed: position.speed || '',
      heading: position.heading || '',
      updated_at: Date.now()
    });

    if (IS_DEBUG) {
      logger.debug('Position passager enregistrée', { passagerId });
    }
  } catch (err) {
    logger.error('Erreur position passager', { error: err.message });
  }
}

// ---------------------- Chat Handlers ----------------------
async function handleChatMessage(reservationId, data) {
  try {
    const key = `chat:${reservationId}:messages`;
    await redis.lpush(key, JSON.stringify({
      from: data.from,
      content: data.content,
      timestamp: Date.now()
    }));

    const messageCount = await redis.llen(key);
    if (messageCount >= 100) {
      await archiveChatMessages(reservationId);
    }
  } catch (err) {
    logger.error('Erreur message chat', { reservationId, error: err.message });
  }
}

async function archiveChatMessages(reservationId) {
  const key = `chat:${reservationId}:messages`;
  try {
    const messages = await redis.lrange(key, 0, -1);
    await axios.post(`${process.env.LARAVEL_API_URL}/api/chat/archive`, {
      reservation_id: reservationId,
      messages: messages.map(m => JSON.parse(m))
    }, { headers: { 'Content-Type': 'application/json' } });

    await redis.del(key);
    logger.info('Chat archivé', { reservationId });
  } catch (err) {
    logger.error('Erreur archivage chat', { reservationId, error: err.message });
  }
}

// ---------------------- Utility Functions ----------------------
async function notifyLaravel(endpoint, payload) {
  try {
    await axios.post(`${process.env.LARAVEL_API_URL}${endpoint}`, payload, {
      headers: { 'Content-Type': 'application/json' }
    });

    if (IS_DEBUG) {
      logger.debug('Notification Laravel envoyée', { endpoint });
    }
  } catch (err) {
    logger.error('Erreur notification Laravel', {
      endpoint,
      error: err.response?.data || err.message
    });
  }
}

async function updateChauffeurStatus(chauffeurId, fields) {
  const key = `chauffeur:${chauffeurId}`;
  const statusData = { updated_at: Date.now() };
  if (fields.disponible !== undefined) statusData.disponible = fields.disponible ? '1' : '0';
  if (fields.en_ligne !== undefined) statusData.en_ligne = fields.en_ligne ? '1' : '0';
  if (fields.en_course !== undefined) statusData.en_course = fields.en_course ? '1' : '0';

  await redisUpdate(key, statusData);
  await publishChauffeurStatus(chauffeurId, { source: 'server' });
}

async function publishChauffeurStatus(chauffeurId, options = {}) {
  if (!MQTT_ENABLED) return;

  const now = Date.now();
  const lastPublish = lastStatusPublishTs.get(chauffeurId) || 0;

  // Throttle status publications (500ms minimum interval)
  if (now - lastPublish < 500) return;
  lastStatusPublishTs.set(chauffeurId, now);

  try {
    const chauffeurData = await redis.hgetall(`chauffeur:${chauffeurId}`);
    if (!chauffeurData || Object.keys(chauffeurData).length === 0) return;

    const statusData = {
      chauffeur_id: chauffeurId,
      disponible: chauffeurData.disponible === '1',
      en_ligne: chauffeurData.en_ligne === '1',
      en_course: chauffeurData.en_course === '1',
      latitude: parseFloat(chauffeurData.latitude) || null,
      longitude: parseFloat(chauffeurData.longitude) || null,
      updated_at: parseInt(chauffeurData.updated_at) || now,
      timestamp: new Date().toISOString(),
      source: options.source || 'client'
    };

    const topic = `chauffeur/${chauffeurId}/status`;
    const payload = JSON.stringify(statusData);

    await publishMqttMessage(topic, payload, { qos: 1 });
  } catch (err) {
    logger.error('Erreur publication statut chauffeur', { chauffeurId, error: err.message });
  }
}

async function publishChauffeurPosition(chauffeurId, lat, lng) {
  const topic = `chauffeur/${chauffeurId}/position`;
  const payload = JSON.stringify({
    type: 'general_position',
    chauffeur_id: chauffeurId,
    data: { lat, lng, timestamp: Date.now() }
  });

  await publishMqttMessage(topic, payload);
}

async function publishMqttMessage(topic, payload, options = { qos: 1, retain: false }) {
  if (!mqttPublisher?.connected) {
    enqueuePendingMessage(topic, payload, options);
  } else {
    mqttPublisher.publish(topic, payload, options);
  }
}

// ---------------------- Cleanup Functions ----------------------
async function cleanupReservation(reservationId) {
  try {
    const chatKey = `chat:${reservationId}:messages`;
    if (await redis.exists(chatKey)) {
      await archiveChatMessages(reservationId);
    }

    await redis.del(`reservation:${reservationId}:position`);

    const topic = `${RESERVATION_TOPIC_PREFIX}${reservationId}`;
    if (subscribedReservationTopics.has(topic)) {
      mqttClient.unsubscribe(topic, () => {
        logger.info('Désabonné du topic réservation', { topic });
      });
      subscribedReservationTopics.delete(topic);
    }
  } catch (err) {
    logger.error('Erreur nettoyage réservation', { reservationId, error: err.message });
  }
}

// ---------------------- Inactive Drivers Check ----------------------
async function checkInactiveChauffeurs() {
  try {
    const keys = await scanRedisKeys('chauffeur:*');
    const now = Date.now();
    let inactiveCount = 0;

    for (const key of keys) {
      const chauffeurId = key.split(':')[1];
      const chauffeur = await redis.hgetall(key);

      if (!chauffeur || Object.keys(chauffeur).length === 0) continue;

      const lastUpdate = parseInt(chauffeur.updated_at || '0', 10);
      const isInactive = (now - lastUpdate) > INACTIVITY_THRESHOLD_MS;

      if (chauffeur.en_ligne === '1' && isInactive) {
        await redisUpdate(key, {
          en_ligne: '0',
          disponible: '0',
          en_course: '0',
          updated_at: now
        });

        await publishChauffeurStatus(chauffeurId, { source: 'server' });
        inactiveCount++;

        logger.info('Chauffeur passé hors ligne (inactivité)', { chauffeurId });
      }
    }

    if (inactiveCount > 0 || IS_DEBUG) {
      logger.info('Vérification inactivité terminée', {
        total: keys.length,
        markedInactive: inactiveCount
      });
    }
  } catch (err) {
    logger.error('Erreur vérification chauffeurs inactifs', err.message);
  }
}

// Check inactive drivers every minute
setInterval(checkInactiveChauffeurs, 60 * 1000);

// ---------------------- API Endpoints ----------------------
app.post('/api/ecouter-topic', (req, res) => {
  const { topic } = req.body;

  if (!MQTT_ENABLED || !mqttClient) {
    return res.status(503).json({ message: 'MQTT non disponible' });
  }

  if (!topic) {
    return res.status(400).json({ message: 'Topic manquant' });
  }

  mqttClient.subscribe(topic, { qos: 1 }, async (err) => {
    if (err) {
      logger.error('Erreur abonnement topic', { topic, error: err.message });
      return res.status(500).json({ message: 'Erreur abonnement' });
    }

    const topicParts = topic.split('/');
    if (topicParts.length >= 3 && topicParts[0] === 'chauffeur') {
      const chauffeurId = topicParts[1];
      await updateChauffeurStatus(chauffeurId, {
        en_ligne: true,
        disponible: true,
        en_course: false
      });
    }
    logger.info('Abonnement topic effectué', { topic });
    return res.json({ message: `Abonné à ${topic}` });
  });
});

app.post('/api/desabonner-topic', async (req, res) => {
  const { topic } = req.body;

  if (!MQTT_ENABLED || !mqttClient) {
    return res.status(503).json({ message: 'MQTT non disponible' });
  }

  if (!topic) {
    return res.status(400).json({ message: 'Topic manquant' });
  }

  mqttClient.unsubscribe(topic, async (err) => {
    if (err) {
      logger.error('Erreur désabonnement topic', { topic, error: err.message });
      return res.status(500).json({ message: 'Erreur désabonnement' });
    }

    // Handle driver disconnection: set all statuses to offline
    const topicParts = topic.split('/');
    if (topicParts.length >= 3 && topicParts[0] === 'chauffeur') {
      const chauffeurId = topicParts[1];
      await updateChauffeurStatus(chauffeurId, {
        en_ligne: false,
        disponible: false,
        en_course: false
      });

      logger.info('Chauffeur déconnecté via désabonnement', { chauffeurId, topic });
    }

    // Clean up reservation subscriptions
    if (topic.startsWith(RESERVATION_TOPIC_PREFIX)) {
      subscribedReservationTopics.delete(topic);
    }

    logger.info('Désabonnement topic effectué', { topic });
    return res.json({ message: `Désabonné de ${topic}` });
  });
});

app.get('/api/mqtt/status', (req, res) => {
  res.json({
    mqtt_enabled: MQTT_ENABLED,
    publisher_enabled: MQTT_PUBLISHER_ENABLED,
    listener_connected: !!(mqttClient?.connected),
    publisher_connected: !!(mqttPublisher?.connected),
    pending_messages: pendingMessages.length,
    subscribed_reservations: Array.from(subscribedReservationTopics),
    cached_positions: lastPositionCache.size
  });
});

app.post('/api/reconnect-publisher', (req, res) => {
  try {
    if (!MQTT_ENABLED) {
      return res.status(400).json({ message: 'MQTT désactivé' });
    }

    if (mqttPublisher) {
      mqttPublisher.end(true);
    }

    mqttPublisher = createMqttClient('publisher', {
      will: {
        topic: 'ktur/server/status',
        payload: JSON.stringify({ status: 'offline', timestamp: Date.now() }),
        qos: 1,
        retain: true
      }
    });

    mqttPublisher.on('connect', () => {
      logger.info('MQTT Publisher reconnecté');
      mqttPublisher.publish('ktur/server/status',
        JSON.stringify({ status: 'online', timestamp: Date.now() }),
        { qos: 1, retain: true }
      );
      processPendingMessages();
    });

    mqttPublisher.on('error', err => logger.error('MQTT Publisher erreur:', err.message));

    return res.json({ message: 'Reconnexion publisher initiée' });
  } catch (err) {
    logger.error('Erreur reconnexion publisher', err.message);
    return res.status(500).json({ message: 'Erreur reconnexion' });
  }
});

app.post('/api/chauffeur/:id/subscribe-status', (req, res) => {
  const { id } = req.params;
  const statusTopic = `chauffeur/${id}/status`;

  if (!MQTT_ENABLED || !mqttClient) {
    return res.status(503).json({ message: 'MQTT non disponible' });
  }

  mqttClient.subscribe(statusTopic, { qos: 1 }, (err, granted) => {
    if (err) {
      logger.error('Erreur abonnement statut chauffeur', { chauffeurId: id, error: err.message });
      return res.status(500).json({ message: 'Erreur abonnement' });
    }

    const qos = Array.isArray(granted) && granted[0]?.qos !== undefined ? granted[0].qos : 1;
    logger.info('Abonnement statut chauffeur effectué', { chauffeurId: id, topic: statusTopic, qos });
    return res.json({ message: `Abonné au statut du chauffeur ${id}` });
  });
});

app.get('/api/chauffeurs/status', async (req, res) => {
  try {
    const keys = await scanRedisKeys('chauffeur:*');
    const chauffeurs = [];

    for (const key of keys) {
      const chauffeurId = key.split(':')[1];
      const data = await redis.hgetall(key);

      if (!data || Object.keys(data).length === 0) continue;

      chauffeurs.push({
        id: chauffeurId,
        disponible: data.disponible === '1',
        en_ligne: data.en_ligne === '1',
        en_course: data.en_course === '1',
        latitude: parseFloat(data.latitude) || null,
        longitude: parseFloat(data.longitude) || null,
        updated_at: parseInt(data.updated_at) || null,
        accuracy: data.accuracy || null,
        speed: data.speed || null,
        heading: data.heading || null
      });
    }

    res.json({ chauffeurs });
  } catch (err) {
    logger.error('Erreur récupération statuts chauffeurs', err.message);
    res.status(500).json({ error: 'Erreur serveur' });
  }
});

app.post('/api/chauffeurs/:id/status', async (req, res) => {
  const { id } = req.params;
  const { disponible, en_ligne, en_course } = req.body;

  if (typeof disponible !== 'boolean' || typeof en_ligne !== 'boolean') {
    return res.status(400).json({ error: 'Statut invalide - disponible et en_ligne requis' });
  }

  try {
    await updateChauffeurStatus(id, { disponible, en_ligne, en_course });

    const currentStatus = await redis.hgetall(`chauffeur:${id}`);
    const responseData = {
      id,
      disponible: currentStatus.disponible === '1',
      en_ligne: currentStatus.en_ligne === '1',
      en_course: currentStatus.en_course === '1',
      updated_at: parseInt(currentStatus.updated_at)
    };

    logger.info('Statut chauffeur mis à jour via API', { chauffeurId: id, status: responseData });
    res.json(responseData);
  } catch (err) {
    logger.error('Erreur mise à jour statut via API', { chauffeurId: id, error: err.message });
    res.status(500).json({ error: 'Erreur serveur' });
  }
});

app.post('/api/reservation/subscribe', (req, res) => {
  const { reservation_id } = req.body;

  if (!reservation_id) {
    return res.status(400).json({ message: 'reservation_id manquant' });
  }

  if (!MQTT_ENABLED || !mqttClient) {
    return res.status(503).json({ message: 'MQTT non disponible' });
  }

  const topic = `${RESERVATION_TOPIC_PREFIX}${reservation_id}`;

  mqttClient.subscribe(topic, { qos: 1 }, (err) => {
    if (err) {
      logger.error('Erreur abonnement réservation', { reservationId: reservation_id, error: err.message });
      return res.status(500).json({ error: 'Erreur abonnement topic' });
    }

    subscribedReservationTopics.add(topic);
    logger.info('Abonnement réservation effectué', { reservationId: reservation_id });
    return res.json({ message: `Abonné à la réservation ${reservation_id}` });
  });
});

app.post('/api/reservation/send-message', async (req, res) => {
  const { reservation_id, message } = req.body;

  if (!MQTT_PUBLISHER_ENABLED) {
    return res.status(503).json({ error: 'Publisher MQTT non disponible' });
  }

  if (!reservation_id || !message?.from || !message?.content) {
    return res.status(400).json({ error: 'Données message incomplètes' });
  }

  try {
    const topic = `${RESERVATION_TOPIC_PREFIX}${reservation_id}`;
    const payload = JSON.stringify({
      type: 'chat',
      from: message.from,
      content: message.content,
      timestamp: Date.now()
    });

    await publishMqttMessage(topic, payload, { qos: 1 });
    logger.info('Message chat envoyé', { reservationId: reservation_id, from: message.from });

    return res.json({ success: true });
  } catch (err) {
    logger.error('Erreur envoi message chat', { reservationId: reservation_id, error: err.message });
    return res.status(500).json({ error: 'Erreur envoi message' });
  }
});

app.get('/api/chat/history/:reservationId', async (req, res) => {
  const { reservationId } = req.params;

  try {
    const key = `chat:${reservationId}:messages`;
    const messages = await redis.lrange(key, 0, -1);

    const formattedMessages = messages
      .map(messageStr => {
        try {
          return JSON.parse(messageStr);
        } catch (e) {
          return null;
        }
      })
      .filter(Boolean)
      .reverse(); // Most recent first

    res.json({
      messages: formattedMessages,
      reservation_id: reservationId,
      count: formattedMessages.length
    });

    if (IS_DEBUG) {
      logger.debug('Historique chat récupéré', { reservationId, count: formattedMessages.length });
    }
  } catch (err) {
    logger.error('Erreur récupération historique chat', { reservationId, error: err.message });
    res.status(500).json({ error: 'Erreur serveur' });
  }
});

app.get('/api/reservation/:id/position', async (req, res) => {
  const { id } = req.params;

  try {
    const position = await redis.hgetall(`reservation:${id}:position`);

    if (!position || Object.keys(position).length === 0) {
      return res.status(404).json({ error: 'Position réservation non trouvée' });
    }

    const responseData = {
      reservation_id: id,
      latitude: parseFloat(position.lat),
      longitude: parseFloat(position.lng),
      chauffeur_id: position.chauffeur_id,
      accuracy: position.accuracy || null,
      speed: position.speed || null,
      heading: position.heading || null,
      updated_at: parseInt(position.updated_at)
    };

    res.json(responseData);
  } catch (err) {
    logger.error('Erreur récupération position réservation', { reservationId: id, error: err.message });
    res.status(500).json({ error: 'Erreur serveur' });
  }
});

app.get('/api/health', (req, res) => {
  const health = {
    status: 'ok',
    timestamp: new Date().toISOString(),
    mqtt: {
      enabled: MQTT_ENABLED,
      listener_connected: !!(mqttClient?.connected),
      publisher_connected: !!(mqttPublisher?.connected),
      pending_messages: pendingMessages.length
    },
    redis: {
      status: redis.status
    },
    cache: {
      positions: lastPositionCache.size,
      status_throttle: lastStatusPublishTs.size
    }
  };

  res.json(health);
});

// Error handling middleware
app.use((err, req, res, next) => {
  logger.error('Erreur Express non gérée', {
    error: err.message,
    stack: err.stack,
    url: req.url,
    method: req.method
  });

  res.status(500).json({ error: 'Erreur serveur interne' });
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({ error: 'Endpoint non trouvé' });
});

// ---------------------- Server Startup ----------------------
initializeMQTT();

const server = app.listen(PORT, () => {
  logger.info(`Serveur KTUR démarré sur le port ${PORT}`, {
    nodeEnv: process.env.NODE_ENV || 'development',
    logLevel: LOG_LEVEL,
    mqttEnabled: MQTT_ENABLED,
    publisherEnabled: MQTT_PUBLISHER_ENABLED
  });
});

// ---------------------- Graceful Shutdown ----------------------
function gracefulShutdown(signal) {
  logger.info(`Signal ${signal} reçu, arrêt en cours...`);

  // Publish offline status
  if (mqttPublisher?.connected) {
    mqttPublisher.publish('ktur/server/status',
      JSON.stringify({ status: 'offline', timestamp: Date.now() }),
      { qos: 1, retain: true }
    );
  }

  // Close connections
  if (mqttClient) mqttClient.end(true);
  if (mqttPublisher) mqttPublisher.end(true);

  redis.disconnect();

  server.close(() => {
    logger.info('Serveur arrêté proprement');
    process.exit(0);
  });

  // Force exit after 10 seconds
  setTimeout(() => {
    logger.error('Arrêt forcé après timeout');
    process.exit(1);
  }, 10000);
}

process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

process.on('uncaughtException', (err) => {
  logger.error('Exception non capturée', { error: err.message, stack: err.stack });
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  logger.error('Promesse rejetée non gérée', { reason, promise });
  process.exit(1);
});