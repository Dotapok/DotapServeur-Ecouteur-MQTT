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
// Aligné avec l'app chauffeur (mosquitto TLS)
const MQTT_BROKER_URL = 'mqtts://test.mosquitto.org:8883';

const MQTT_USERNAME = process.env.MQTT_USERNAME || '';
const MQTT_PASSWORD = process.env.MQTT_PASSWORD || '';

const POSITION_THROTTLE_MS = parseInt(process.env.POSITION_THROTTLE_MS || '250', 10); // min interval (Uber-like)
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
const PASSAGER_STATUS_TOPIC = 'passager_mobile/status';
const PASSAGER_POSITION_TOPIC = 'passager_mobile/position';

// ---------------------- Helpers ----------------------
function safeJsonParse(buf) {
  try {
    return JSON.parse(buf.toString());
  } catch (e) {
    logger.warn('safeJsonParse échec: payload non JSON');
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
  try {
    logger.debug('Redis HMSET', { key, fields: Object.keys(obj) });
  } catch (_) {}
  const pipeline = redis.multi();
  pipeline.hmset(key, obj);
  await pipeline.exec();
}

// ---------------------- MQTT publisher queue ----------------------
function enqueuePending(topic, payload, options = { qos: 1, retain: false }) {
  // LOGGING DÉTAILLÉ : Capture tous les messages en file d'attente
  console.log('⏳ AJOUT EN FILE D\'ATTENTE MQTT:', {
    topic,
    payloadSize: payload.length,
    queueSize: pendingMessages.length,
    maxSize: MAX_PENDING_MESSAGES,
    timestamp: new Date().toISOString()
  });
  
  if (pendingMessages.length >= MAX_PENDING_MESSAGES) {
    // drop oldest to keep memory bounded
    const removed = pendingMessages.shift();
    console.warn('⚠️ FILE D\'ATTENTE MQTT PLEINE - Suppression du plus ancien:', {
      removedTopic: removed.topic,
      newQueueSize: pendingMessages.length
    });
    logger.warn('File d\'attente MQTT pleine - suppression du plus ancien');
  }
  
  pendingMessages.push({ topic, payload, options });
  console.log('✅ MESSAGE AJOUTÉ EN FILE:', { topic, newQueueSize: pendingMessages.length });
  logger.debug('Message mis en file (publisher non connecté)', { topic, pendingSize: pendingMessages.length });
}

function processPendingMessages() {
  if (!mqttPublisher || !mqttPublisher.connected) {
    console.log('⏸️ PROCESSING EN ATTENTE: Publisher non connecté');
    logger.debug('processPendingMessages: publisher non connecté, report');
    return;
  }
  
  console.log('🔄 TRAITEMENT FILE D\'ATTENTE MQTT:', { count: pendingMessages.length });
  logger.debug('processPendingMessages: début traitement', { count: pendingMessages.length });
  
  while (pendingMessages.length) {
    const m = pendingMessages.shift();
    try {
      console.log('📤 PUBLICATION MESSAGE DIFFÉRÉ:', { topic: m.topic, remainingInQueue: pendingMessages.length });
      logger.debug('Publication message différé', { topic: m.topic });
      mqttPublisher.publish(m.topic, m.payload, m.options);
    } catch (err) {
      console.error('💥 ERREUR PUBLICATION MESSAGE DIFFÉRÉ:', {
        topic: m.topic,
        error: err.message,
        stack: err.stack,
        timestamp: new Date().toISOString()
      });
      logger.error('Erreur publication message différé:', err.message);
      // remonter en tête et sortir (retry plus tard)
      pendingMessages.unshift(m);
      break;
    }
  }
  
  console.log('✅ FILE D\'ATTENTE TRAITÉE:', { remainingCount: pendingMessages.length });
}

// ---------------------- MQTT init ----------------------
function createMqttClient(clientIdSuffix, extra = {}) {
  const clientId = `ktur_${clientIdSuffix}_${Math.random().toString(16).slice(2,8)}`;
  // Normaliser l'URL pour WSS Mosquitto: ajouter /mqtt si manquant
  let brokerUrl = MQTT_BROKER_URL;
  if (brokerUrl.startsWith('wss://') && brokerUrl.includes('test.mosquitto.org:8081')) {
    brokerUrl = brokerUrl;
  }
  logger.info('Connexion au broker MQTT...', { url: brokerUrl, clientId: clientId, role: clientIdSuffix });
  const rejectUnauthorized = (process.env.MQTT_REJECT_UNAUTHORIZED || '').toLowerCase() === 'false' ? false : false;
  const isWss = brokerUrl.startsWith('wss://');
  const conn = mqtt.connect(brokerUrl, {
    username: '',
    password: '',
    reconnectPeriod: 5000,
    connectTimeout: 30000,
    keepalive: 60,
    clean: true,
    clientId,
    rejectUnauthorized,
    protocolVersion: 4,
    protocolId: 'MQTT',
    // Pour WSS, certains brokers exigent un chemin explicite et options WS
    ...(isWss ? { 
      path: brokerUrl.endsWith('/mqtt') ? undefined : '/mqtt', 
      wsOptions: { 
        rejectUnauthorized,
        headers: {
          Origin: process.env.MQTT_WS_ORIGIN || 'https://test.mosquitto.org'
        }
      } 
    } : {}),
    ...extra
  });
  // Aide au debug des frames réseau
  conn.on('packetsend', (p) => { try { if (p && p.cmd) logger.debug('MQTT packet send', { role: clientIdSuffix, cmd: p.cmd }); } catch(_){} });
  conn.on('packetreceive', (p) => { try { if (p && p.cmd) logger.debug('MQTT packet recv', { role: clientIdSuffix, cmd: p.cmd }); } catch(_){} });
  return conn;
}

function initializeMQTT() {
  if (!MQTT_ENABLED) {
    logger.warn('MQTT désactivé');
    return;
  }

  // Listener (s'abonne aux wildcards utiles)
  mqttClient = createMqttClient('listener');
  mqttClient.on('connect', () => {
    console.log('🔌 MQTT LISTENER CONNECTÉ');
    logger.info('MQTT Listener connecté');
    // abonnements "génériques"
    mqttClient.subscribe([
      RESERVATIONS_RECENTES_TOPIC,
      STATUS_TOPIC_WILDCARD,
      POSITION_TOPIC_WILDCARD,
      PASSAGER_STATUS_TOPIC,
      PASSAGER_POSITION_TOPIC,
    ], { qos: 1 }, (err) => {
      if (err) {
        console.error('❌ ERREUR ABONNEMENT WILDCARD MQTT:', { error: err.message });
        logger.error('Erreur abonnement wildcard MQTT:', err.message);
      } else {
        console.log('✅ ABONNEMENTS WILDCARD MQTT EFFECTUÉS:', {
          topics: [RESERVATIONS_RECENTES_TOPIC, STATUS_TOPIC_WILDCARD, POSITION_TOPIC_WILDCARD, PASSAGER_STATUS_TOPIC, PASSAGER_POSITION_TOPIC]
        });
        logger.info('Abonnements wildcard MQTT effectués');
      }
    });
    processPendingMessages();
  });

  mqttClient.on('message', onMqttMessage);
  mqttClient.on('error', e => {
    console.error('💥 MQTT Listener error detailed:', {
      message: e && e.message,
      code: e && e.code,
      errno: e && e.errno,
      stack: e && e.stack,
      timestamp: new Date().toISOString()
    });
    logger.error('MQTT Listener err:', e && (e.message || e));
  });
  mqttClient.on('close', () => logger.info('MQTT Listener fermé'));
  mqttClient.on('offline', () => logger.warn('MQTT Listener hors ligne'));
  // Logs détaillés des souscriptions/désabonnements côté client
  mqttClient.on('packetsend', (packet) => {
    try {
      if (packet && packet.cmd === 'subscribe') {
        const topics = (packet.subscriptions || []).map(s => s.topic);
        logger.info('Souscription envoyée', { topics });
      } else if (packet && packet.cmd === 'unsubscribe') {
        const topics = packet.unsubscriptions || [];
        logger.info('Désabonnement envoyé', { topics });
      }
    } catch (_) {}
  });
  mqttClient.on('packetreceive', (packet) => {
    try {
      if (packet && packet.cmd === 'suback') {
        logger.info('Souscription confirmée (SUBACK)');
      } else if (packet && packet.cmd === 'unsuback') {
        logger.info('Désabonnement confirmé (UNSUBACK)');
      }
    } catch (_) {}
  });

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

    mqttPublisher.on('error', e => {
      console.error('💥 MQTT Publisher error detailed:', {
        message: e && e.message,
        code: e && e.code,
        errno: e && e.errno,
        stack: e && e.stack,
        timestamp: new Date().toISOString()
      });
      logger.error('MQTT Publisher err:', e && (e.message || e));
    });
    mqttPublisher.on('close', () => logger.info('MQTT Publisher fermé'));
    mqttPublisher.on('offline', () => logger.warn('MQTT Publisher hors ligne'));
  } else {
    logger.warn('Publisher MQTT désactivé');
  }
}

// ---------------------- Message handling ----------------------
async function onMqttMessage(topic, messageBuf) {
  // LOGGING DÉTAILLÉ : Capture tous les messages MQTT
  console.log('🔍 MQTT MESSAGE RECU:', {
    topic,
    size: messageBuf?.length || 0,
    timestamp: new Date().toISOString(),
    rawData: messageBuf?.toString().substring(0, 300) // Premiers 300 caractères
  });
  
  logger.info('MQTT message reçu', { topic, size: messageBuf?.length || 0 });
  
  const data = safeJsonParse(messageBuf);
  if (!data) {
    console.warn('⚠️ MESSAGE MQTT NON JSON:', { topic, rawData: messageBuf?.toString() });
    logger.warn('Message MQTT non JSON ignoré', { topic });
    return;
  }

  try {
    // LOGGING DÉTAILLÉ : Contenu de chaque message
    console.log('📨 CONTENU MESSAGE MQTT:', {
      topic,
      messageType: data.type || 'non défini',
      dataKeys: Object.keys(data),
      hasPosition: !!(data.position || data.lat || data.lng),
      hasChauffeurId: !!(data.chauffeur_id || data.driver_id),
      timestamp: data.timestamp || 'non défini',
      fullData: data
    });

    // shortcuts to avoid repeated work
    if (topic === RESERVATIONS_RECENTES_TOPIC) {
      console.log('🎯 ROUTAGE: Nouvelles réservations détectées');
      logger.debug('Routage: nouvelles réservations');
      await handleNewReservation(data);
      return;
    }

    // Statut/position passager_mobile (nouvel alignement)
    if (topic === PASSAGER_STATUS_TOPIC) {
      console.log('🎯 ROUTAGE: Statut passager_mobile', { passager_id: data.passager_id, status: data.status });
      await handlePassagerStatus(data);
      return;
    }
    if (topic === PASSAGER_POSITION_TOPIC) {
      console.log('🎯 ROUTAGE: Position passager_mobile', { passager_id: data.passager_id, lat: data?.data?.lat, lng: data?.data?.lng });
      await handlePassagerPosition(data);
      return;
    }

    if (topic.startsWith(RESERVATION_TOPIC_PREFIX)) {
      const parts = topic.split('/');
      const reservationId = parts[2];
      console.log('🎯 ROUTAGE: Message réservation', { reservationId, type: data?.type });
      logger.debug('Routage: message de réservation', { reservationId, type: data?.type });
      await handleReservationMessage(reservationId, data);
      return;
    }

    // chauffeur topics (wildcards cover many cases)
    if (/^chauffeur\/.+\/status$/.test(topic)) {
      const chauffeurId = topic.split('/')[1];
      console.log('🎯 ROUTAGE: Statut chauffeur', { chauffeurId, data });
      logger.debug('Routage: statut chauffeur', { chauffeurId });
      await handleChauffeurStatusUpdate(chauffeurId, data);
      return;
    }

    if (/^chauffeur\/.+\/position$/.test(topic)) {
      const chauffeurId = topic.split('/')[1];
      const position = data.data || data;
      console.log('🎯 ROUTAGE: Position chauffeur', { chauffeurId, hasData: !!position, position });
      logger.debug('Routage: position chauffeur', { chauffeurId, hasData: !!position });
      await handlePosition(chauffeurId, position);
      return;
    }

    if (/^ktur\/reservations\/.+\/position$/.test(topic)) {
      const reservationId = topic.split('/')[2];
      console.log('🎯 ROUTAGE: Position réservation', { reservationId, data });
      logger.debug('Routage: position de réservation', { reservationId });
      await handleReservationPosition(reservationId, data);
      return;
    }

    // fallback: log as debug only
    console.log('❓ TOPIC MQTT INCONNU:', { topic, data });
    logger.debug('Topic MQTT inconnu reçu', { topic });
  } catch (err) {
    // LOGGING DÉTAILLÉ DES ERREURS : Capture toutes les erreurs silencieuses
    console.error('💥 ERREUR MQTT CRITIQUE:', {
      topic,
      error: err.message,
      stack: err.stack,
      rawData: messageBuf?.toString().substring(0, 300),
      timestamp: new Date().toISOString()
    });
    
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
      logger.warn('Type message réservation non géré', { reservationId, type: data.type });
  }
}

// ---------------------- Passager handlers ----------------------
async function handlePassagerStatus(data) {
  try {
    const passagerId = data.passager_id || data.client_id || 'unknown';
    const key = `passager:${passagerId}`;
    // stocker un statut simple en Redis
    await redisHSetMulti(key, {
      en_ligne: data.status === 'online' ? '1' : (data.status === 'offline' ? '0' : '1'),
      last_status: data.status || 'unknown',
      updated_at: Date.now(),
    });
    logger.debug('Passager status enregistré', { passagerId, status: data.status });
  } catch (err) {
    logger.error('Erreur handlePassagerStatus', err.message || err);
  }
}

async function handlePassagerPosition(data) {
  try {
    const passagerId = data.passager_id || data.client_id || 'unknown';
    const pos = data.data || data.position || data;
    if (!pos || typeof pos.lat !== 'number' || typeof pos.lng !== 'number') {
      console.warn('Position passager invalide', { passagerId, pos });
      return;
    }

    const key = `passager:${passagerId}:position`;
    await redisHSetMulti(key, {
      latitude: pos.lat,
      longitude: pos.lng,
      accuracy: pos.accuracy || '',
      speed: pos.speed || '',
      heading: pos.heading || '',
      updated_at: Date.now(),
    });
    logger.debug('Position passager enregistrée', { passagerId });
  } catch (err) {
    logger.error('Erreur handlePassagerPosition', err.message || err);
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
  // LOGGING DÉTAILLÉ : Capture toutes les positions de réservation
  console.log('📍 POSITION RÉSERVATION RECUE:', {
    reservationId,
    chauffeurId: data.chauffeur_id || data.driver_id,
    hasPosition: !!(data.position || data.lat || data.lng),
    dataKeys: Object.keys(data),
    timestamp: new Date().toISOString()
  });
  
  logger.debug('handleReservationPosition: entrée', { reservationId });
  const key = `reservation:${reservationId}:position`;
  
  // Assouplir l'ingestion: accepter { lat,lng } OU { data:{ lat,lng } } OU { position:{ lat,lng } }
  const src = (data && typeof data === 'object') ? data : {};
  const pos = (src.position && typeof src.position === 'object')
    ? src.position
    : (src.data && typeof src.data === 'object')
      ? src.data
      : src;

  // VALIDATION DES DONNÉES : Vérifier que la position est valide
  if (!pos.lat || !pos.lng) {
    console.error('❌ POSITION RÉSERVATION INVALIDE:', {
      reservationId,
      receivedData: data,
      extractedPosition: pos,
      timestamp: new Date().toISOString()
    });
    return;
  }

  const positionData = {
    lat: pos.lat,
    lng: pos.lng,
    chauffeur_id: src.chauffeur_id,
    reservation_status: src.reservation_status || 'active',
    is_in_reservation: '1',
    updated_at: Date.now(),
    accuracy: pos.accuracy || '',
    speed: pos.speed || '',
    heading: pos.heading || ''
  };
  
  // LOGGING AVANT ÉCRITURE REDIS
  console.log('💾 ÉCRITURE POSITION RÉSERVATION REDIS:', {
    reservationId,
    key: key,
    positionData: positionData
  });
  
  try {
    await redisHSetMulti(key, positionData);
    console.log('✅ POSITION RÉSERVATION ENREGISTRÉE:', {
      reservationId,
      key: key,
      timestamp: new Date().toISOString()
    });
    logger.debug('handleReservationPosition: écrit dans Redis', { key });
  } catch (err) {
    console.error('💥 ERREUR ÉCRITURE REDIS POSITION RÉSERVATION:', {
      reservationId,
      error: err.message,
      stack: err.stack,
      timestamp: new Date().toISOString()
    });
    throw err;
  }

  // publier (si possible) mais sans flooding de logs
  const topic = `ktur/reservations/${reservationId}/position`;
  const payload = JSON.stringify({
    type: 'reservation_position',
    reservation_id: reservationId,
    chauffeur_id: positionData.chauffeur_id,
    position: { lat: positionData.lat, lng: positionData.lng, accuracy: positionData.accuracy },
    timestamp: new Date().toISOString()
  });
  
  if (!mqttPublisher || !mqttPublisher.connected) {
    console.log('⏳ MESSAGE EN FILE D\'ATTENTE:', { topic, queueSize: pendingMessages.length });
    enqueuePending(topic, payload, { qos: 1 });
  } else {
    console.log('📤 PUBLICATION POSITION RÉSERVATION:', { topic, payload });
    logger.debug('Publication position réservation', { topic });
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

    // notifier Laravel (paramètre attendu: resa_id)
    await notifyLaravel('/reservation/acceptation', { resa_id: reservationId, chauffeur_id: data.chauffeur_id });

    // mettre à jour statut
    await updateStatut(data.chauffeur_id, { en_ligne: true, en_course: true, disponible: false });

    // publication serveur (évite boucle car publish handler ignore source:server)
    await publishChauffeurStatus(data.chauffeur_id, { source: 'server' });

    logger.info('Réservation acceptée', { reservationId, chauffeur: data.chauffeur_id });
  } catch (err) {
    logger.error('Erreur acceptation réservation', err.message);
  }
}

async function handleReservationStart(reservationId, data) {
  try {
    // 1. Notifier Laravel
    await notifyLaravel('/reservation/acceptation', {
      resa_id: reservationId,
      chauffeur_id: data.chauffeur_id,
      action: 'start'
    });

    // 2. Mettre à jour le statut du chauffeur
    await updateStatut(data.chauffeur_id, {
      en_ligne: true,
      en_course: true,
      disponible: false
    });

    // 3. Publier un message MQTT
    const topic = `${RESERVATION_TOPIC_PREFIX}${reservationId}`;
    const payload = JSON.stringify({
      type: 'course_started',
      reservation_id: reservationId,
      chauffeur_id: data.chauffeur_id,
      timestamp: Date.now()
    });
    await publishMQTTMessage(topic, payload, { qos: 1 });

    logger.info('Course démarrée', { reservationId, chauffeur: data.chauffeur_id });
  } catch (err) {
    logger.error('Erreur démarrage course', err.message);
  }
}

async function handleReservationStatusChange(reservationId, data) {
  const endpoint = data.type === 'debut' ? '/reservation/debut' : '/reservation/fin';
  await notifyLaravel(endpoint, { resa_id: reservationId });
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
    mqttClient.unsubscribe(topic, () => {
      logger.info('Désabonné du topic réservation', { topic });
    });
    subscribedReservationTopics.delete(topic);
  }
}

// ---------------------- notify/update/publish helpers ----------------------
async function notifyLaravel(endpoint, payload) {
  try {
    logger.debug('Appel Laravel', { endpoint, payloadKeys: payload && typeof payload === 'object' ? Object.keys(payload) : [] });
    await axios.post(`${process.env.LARAVEL_API_URL}${endpoint}`, payload, {
      headers: { 'Content-Type': 'application/json' }
    });
    logger.debug('Notification envoyée à Laravel', { endpoint });
  } catch (err) {
    logger.error('Erreur appel Laravel', err.response ? err.response.data : err.message);
  }
}

async function updateStatut(chauffeurId, fields) {
  logger.debug('updateStatut: entrée', { chauffeurId, fields });
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
    logger.debug('Publication statut chauffeur', { chauffeurId, en_ligne: statusData.en_ligne, disponible: statusData.disponible });
    mqttPublisher.publish(topic, payload, { qos: 1 });
  }
}

// Generic publisher used elsewhere
async function publishMQTTMessage(topic, payload, options = { qos: 1, retain: false }) {
  // LOGGING DÉTAILLÉ : Capture toutes les publications MQTT
  console.log('📤 PUBLICATION MQTT:', {
    topic,
    payloadSize: payload.length,
    qos: options.qos,
    retain: options.retain,
    timestamp: new Date().toISOString()
  });
  
  if (!mqttPublisher || !mqttPublisher.connected) {
    console.log('⏳ MESSAGE EN FILE D\'ATTENTE:', { topic, queueSize: pendingMessages.length });
    enqueuePending(topic, payload, options);
  } else {
    console.log('✅ MESSAGE PUBLIÉ:', { topic, payload });
    mqttPublisher.publish(topic, payload, options);
  }
}

// Publier position générique (avec throttling en mémoire)
async function publishChauffeurPosition(chauffeurId, lat, lng) {
  logger.debug('publishChauffeurPosition: entrée', { chauffeurId, lat, lng });
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
  // LOGGING DÉTAILLÉ : Capture toutes les positions de chauffeur
  console.log('🚗 POSITION CHAUFFEUR RECUE:', {
    chauffeurId: id,
    hasPosition: !!positionData,
    lat: positionData?.lat,
    lng: positionData?.lng,
    accuracy: positionData?.accuracy,
    speed: positionData?.speed,
    timestamp: new Date().toISOString()
  });
  
  logger.debug('handlePosition: entrée', { id, hasPosition: !!positionData, lat: positionData?.lat, lng: positionData?.lng });
  
  if (!positionData || typeof positionData.lat !== 'number' || typeof positionData.lng !== 'number') {
    console.error('❌ POSITION CHAUFFEUR INVALIDE:', {
      chauffeurId: id,
      positionData: positionData,
      latType: typeof positionData?.lat,
      lngType: typeof positionData?.lng,
      timestamp: new Date().toISOString()
    });
    logger.warn('Position invalide ignorée', { id });
    return;
  }

  try {
    const prev = lastPositionCache.get(id);
    const newPos = { lat: String(positionData.lat), lng: String(positionData.lng) };
    
    if (prev && !distanceChanged(prev, newPos) && (Date.now() - prev.ts) < POSITION_THROTTLE_MS) {
      console.log('⏸️ POSITION IGNORÉE (throttle ou pas de changement):', {
        chauffeurId: id,
        lastUpdate: new Date(prev.ts).toISOString(),
        timeSinceLastUpdate: Date.now() - prev.ts
      });
      // éviter écritures/redondances
      logger.debug('handlePosition: ignoré (throttle ou pas de changement)', { id });
      return;
    }

    // LOGGING AVANT ÉCRITURE REDIS
    console.log('💾 ÉCRITURE POSITION CHAUFFEUR REDIS:', {
      chauffeurId: id,
      key: `chauffeur:${id}`,
      position: { lat: positionData.lat, lng: positionData.lng }
    });

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
    
    console.log('✅ POSITION CHAUFFEUR ENREGISTRÉE:', {
      chauffeurId: id,
      timestamp: new Date().toISOString()
    });
    
    logger.debug('handlePosition: écrit dans Redis', { key });

    // mettre cache et publier
    lastPositionCache.set(id, { ...newPos, ts: Date.now() });
    await publishChauffeurPosition(id, positionData.lat, positionData.lng);
    await publishChauffeurStatus(id, { source: 'server' });

  } catch (err) {
    // LOGGING DÉTAILLÉ DES ERREURS : Capture toutes les erreurs silencieuses
    console.error('💥 ERREUR HANDLEPOSITION:', {
      chauffeurId: id,
      error: err.message,
      stack: err.stack,
      positionData: positionData,
      timestamp: new Date().toISOString()
    });
    
    logger.error('Erreur handlePosition', err.message || err);
  }
}

// ---------------------- Status update from chauffeurs ----------------------
async function handleChauffeurStatusUpdate(chauffeurId, data) {
  // LOGGING DÉTAILLÉ : Capture tous les statuts de chauffeur
  console.log('📱 STATUT CHAUFFEUR RECU:', {
    chauffeurId,
    keys: Object.keys(data || {}),
    hasPosition: !!(data.position && data.position.latitude && data.position.longitude),
    isOnline: data.statut === 1 || data.en_ligne === true,
    timestamp: new Date().toISOString()
  });
  
  try {
    // ignorer messages provenant du serveur
    if (data.source === 'server' || data.is_server_message) {
      console.log('⏭️ STATUT SERVEUR IGNORÉ:', { chauffeurId });
      return;
    }
    
    logger.debug('handleChauffeurStatusUpdate: entrée', { chauffeurId, keys: Object.keys(data || {}) });

    const isOnline = data.statut === 1 || data.en_ligne === true;
    
    console.log('💾 MISE À JOUR STATUT CHAUFFEUR:', {
      chauffeurId,
      isOnline,
      key: `chauffeur:${chauffeurId}`
    });
    
    await redisHSetMulti(`chauffeur:${chauffeurId}`, {
      en_ligne: isOnline ? '1' : '0',
      disponible: isOnline ? '1' : '0',
      en_course: '0',
      updated_at: Date.now()
    });

    if (data.position && data.position.latitude && data.position.longitude) {
      console.log('📍 POSITION VIA STATUT DÉTECTÉE:', {
        chauffeurId,
        position: data.position
      });
      
      await redisHSetMulti(`chauffeur:${chauffeurId}`, {
        latitude: data.position.latitude,
        longitude: data.position.longitude,
        updated_at: Date.now()
      });
      logger.debug('handleChauffeurStatusUpdate: position mise à jour via statut', { chauffeurId });
    }

    // republier depuis le server pour synchronisation (throttled inside)
    await publishChauffeurStatus(chauffeurId, { source: 'server' });
    
    console.log('✅ STATUT CHAUFFEUR TRAITÉ:', { chauffeurId, isOnline });
    
  } catch (err) {
    // LOGGING DÉTAILLÉ DES ERREURS : Capture toutes les erreurs silencieuses
    console.error('💥 ERREUR STATUT CHAUFFEUR:', {
      chauffeurId,
      error: err.message,
      stack: err.stack,
      data: data,
      timestamp: new Date().toISOString()
    });
    
    logger.error('Erreur handleChauffeurStatusUpdate', err.message || err);
  }
}

// ---------------------- Inactive check (scan-based, non bloquant) ----------------------
async function checkInactiveChauffeurs() {
  try {
    logger.debug('checkInactiveChauffeurs: démarrage');
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
    logger.debug('checkInactiveChauffeurs: terminé', { total: keys.length });
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
    console.log('🔔 API SUBSCRIBE DEMANDÉ:', { topic, timestamp: new Date().toISOString() });
    logger.info('API subscribe', { topic, err: err ? err.message : null });
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
    console.log('🔕 API UNSUBSCRIBE DEMANDÉ:', { topic, timestamp: new Date().toISOString() });
    logger.info('API unsubscribe', { topic, err: err ? err.message : null });
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
    logger.info('API subscribe status', { topic: statusTopic, err: err ? err.message : null });
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
      logger.info('API subscribe reservation', { topic });
      return res.json({ message: `Abonné au topic ${topic}` });
    } else {
      logger.error('API subscribe reservation erreur', err.message);
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
