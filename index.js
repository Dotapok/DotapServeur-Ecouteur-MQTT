require('dotenv').config();
const express = require('express');
const mqtt = require('mqtt');
const Redis = require('ioredis');
const cors = require('cors');
const axios = require('axios');
const { createLogger, format, transports } = require('winston');
require('winston-daily-rotate-file');

// Configuration du logger avec rotation des fichiers
const fileRotateTransport = new transports.DailyRotateFile({
  dirname: './logs',
  filename: '%DATE%.log',
  datePattern: 'YYYY-MM-DD',
  maxFiles: '14d',
  level: 'info',
});

const logger = createLogger({
  format: format.combine(
    format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
    format.printf(({ timestamp, level, message, ...meta }) =>
      `${timestamp} [${level.toUpperCase()}] ${message} ${Object.keys(meta).length ? JSON.stringify(meta) : ''}`
    )
  ),
  transports: [
    new transports.Console({ level: 'debug' }),
    fileRotateTransport
  ]
});

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;
const NODE_ENV = process.env.NODE_ENV || 'development';

// Configuration MQTT simplifiée
const MQTT_BROKER_URL = NODE_ENV === 'development'
  ? (process.env.MQTT_BROKER_URL || 'mqtts://pbb16a10.ala.us-east-1.emqxsl.com:8883')
  : (process.env.MQTT_BROKER_URL_DEV || 'mqtt://test.mosquitto.org:1883');

const MQTT_USERNAME = NODE_ENV === 'development'
  ? (process.env.MQTT_USERNAME || 'Ktur_brocker')
  : (process.env.MQTT_USERNAME_DEV || '');

const MQTT_PASSWORD = NODE_ENV === 'development'
  ? (process.env.MQTT_PASSWORD || 'Ktur_brocker#2025')
  : (process.env.MQTT_PASSWORD_DEV || '');

const MQTT_ENABLED = process.env.MQTT_ENABLED !== 'false';
const MQTT_PUBLISHER_ENABLED = process.env.MQTT_PUBLISHER_ENABLED !== 'false';

console.log('🔧 Configuration MQTT:');
console.log(`   Broker: ${MQTT_BROKER_URL}`);
console.log(`   Activé: ${MQTT_ENABLED}`);
console.log(`   Publisher: ${MQTT_PUBLISHER_ENABLED}`);

// Variables pour les clients MQTT
let mqttClient = null;
let mqttPublisher = null;

// Initialisation Redis
const redis = new Redis(process.env.REDIS_URL);
redis.on('connect', () => console.log('✅ Connecté à Redis'));
redis.on('error', err => console.error('❌ Erreur Redis:', err));

// Store des topics écoutés
const subscribedTopics = new Set();

// File d'attente pour les messages différés
const pendingMessages = [];
const MAX_PENDING_MESSAGES = 100;

// Fonction pour traiter la file d'attente des messages différés
function processPendingMessages() {
  if (!mqttPublisher?.connected) return;
  
  console.log(`📤 Traitement de ${pendingMessages.length} messages en attente...`);
  
  while (pendingMessages.length > 0) {
    const message = pendingMessages.shift();
    try {
      mqttPublisher.publish(message.topic, message.payload, message.options);
      logger.info(`📡 Message différé publié: ${message.type} pour ${message.topic}`);
    } catch (error) {
      logger.error('Erreur publication message différé:', error);
      pendingMessages.unshift(message);
      break;
    }
  }
  
  if (pendingMessages.length === 0) {
    console.log('✅ Tous les messages différés ont été traités');
  }
}

// Fonction pour reconnecter manuellement le publisher
function reconnectPublisher() {
  if (!MQTT_ENABLED || !MQTT_PUBLISHER_ENABLED) {
    console.log('⚠️  Impossible de reconnecter - MQTT ou Publisher désactivé');
    return;
  }
  
  if (mqttPublisher?.connected) {
    console.log('ℹ️  Publisher déjà connecté');
    return;
  }
  
  console.log('🔄 Reconnexion manuelle du Publisher MQTT...');
  
  if (mqttPublisher) {
    mqttPublisher.end();
  }
  
  // Réutiliser la configuration commune
  const mqttConfig = {
    username: MQTT_USERNAME,
    password: MQTT_PASSWORD,
    clientId: 'ktur_status_publisher',
    reconnectPeriod: 5000,
    connectTimeout: 30000,
    clean: true,
    keepalive: 60,
    rejectUnauthorized: false,
    will: {
      topic: 'ktur/server/status',
      payload: JSON.stringify({ status: 'offline', timestamp: new Date().toISOString() }),
      qos: 1,
      retain: false
    }
  };

  mqttPublisher = mqtt.connect(MQTT_BROKER_URL, mqttConfig);

  mqttPublisher.on('connect', () => {
    console.log('✅ Publisher MQTT reconnecté');
    mqttPublisher.publish('ktur/server/status', JSON.stringify({ 
      status: 'online', 
      timestamp: new Date().toISOString() 
    }), { qos: 1, retain: true });
    
    processPendingMessages();
  });

  mqttPublisher.on('error', err => console.error('❌ Erreur Publisher MQTT:', err.message));
  mqttPublisher.on('close', () => console.log('🔌 Publisher MQTT fermé'));
  mqttPublisher.on('offline', () => console.log('📴 Publisher MQTT hors ligne'));

  // Heartbeat pour le publisher reconnecté
  const heartbeatInterval = setInterval(() => {
    if (mqttPublisher?.connected) {
      mqttPublisher.publish('ktur/server/heartbeat', JSON.stringify({ 
        timestamp: new Date().toISOString() 
      }), { qos: 0, retain: false });
    }
  }, 30000);

  mqttPublisher.on('close', () => clearInterval(heartbeatInterval));
}

// Configuration des topics
const RESERVATIONS_RECENTES_TOPIC = 'ktur/reservations/recentes';
const RESERVATION_TOPIC_PREFIX = 'ktur/reservations/';
const STATUS_TOPIC = 'chauffeur/+/status';
const POSITION_TOPIC = 'chauffeur/+/position';
const RESERVATION_POSITION_TOPIC = 'ktur/reservations/+/position';
const CHAUFFEUR_GENERAL_POSITION_TOPIC = 'chauffeur/+/position';

// Fonction d'initialisation MQTT simplifiée
function initializeMQTT() {
  if (!MQTT_ENABLED) {
    console.log('⚠️  MQTT désactivé - les fonctionnalités MQTT ne seront pas disponibles');
    return;
  }

  // Configuration commune MQTT
  const mqttConfig = {
    username: MQTT_USERNAME,
    password: MQTT_PASSWORD,
    reconnectPeriod: 5000,
    connectTimeout: 30000,
    clean: true,
    keepalive: 60,
    rejectUnauthorized: false
  };

  // Initialisation MQTT Client (Listener)
  mqttClient = mqtt.connect(MQTT_BROKER_URL, {
    ...mqttConfig,
    clientId: 'ktur_listener_client'
  });

  mqttClient.on('connect', () => {
    console.log('✅ Connecté à MQTT (Listener)');
    // S'abonner au topic des réservations récentes
    const topic = RESERVATIONS_RECENTES_TOPIC;
    if (!subscribedTopics.has(topic)) {
      mqttClient.subscribe(topic, { qos: 1 }, (err) => {
        if (!err) {
          subscribedTopics.add(topic);
          console.log(`🎧 Écoute du topic: ${topic}`);
        } else {
          console.error(`❌ Erreur abonnement au topic ${topic}:`, err);
        }
      });
    }
  });

  mqttClient.on('error', err => console.error('❌ Erreur MQTT Listener:', err.message));
  mqttClient.on('close', () => console.log('🔌 Connexion MQTT Listener fermée'));
  mqttClient.on('reconnect', () => console.log('🔄 Reconnexion MQTT Listener...'));
  mqttClient.on('offline', () => console.log('📴 MQTT Listener hors ligne'));

  // Initialisation MQTT Publisher
  if (MQTT_PUBLISHER_ENABLED) {
    mqttPublisher = mqtt.connect(MQTT_BROKER_URL, {
      ...mqttConfig,
      clientId: 'ktur_status_publisher',
      will: {
        topic: 'ktur/server/status',
        payload: JSON.stringify({ status: 'offline', timestamp: new Date().toISOString() }),
        qos: 1,
        retain: false
      }
    });

    mqttPublisher.on('connect', () => {
      console.log('✅ Publisher MQTT connecté');
      mqttPublisher.publish('ktur/server/status', JSON.stringify({ 
        status: 'online', 
        timestamp: new Date().toISOString() 
      }), { qos: 1, retain: true });
      
      processPendingMessages();
    });

    mqttPublisher.on('error', err => console.error('❌ Erreur Publisher MQTT:', err.message));
    mqttPublisher.on('close', () => console.log('🔌 Publisher MQTT fermé'));
    mqttPublisher.on('reconnect', () => console.log('🔄 Reconnexion Publisher MQTT...'));
    mqttPublisher.on('offline', () => console.log('📴 Publisher MQTT hors ligne'));

    // Heartbeat simplifié
    const heartbeatInterval = setInterval(() => {
      if (mqttPublisher?.connected) {
        mqttPublisher.publish('ktur/server/heartbeat', JSON.stringify({ 
          timestamp: new Date().toISOString() 
        }), { qos: 0, retain: false });
      }
    }, 30000);

    mqttPublisher.on('close', () => clearInterval(heartbeatInterval));
  } else {
    console.log('⚠️  Publisher MQTT désactivé');
  }
}

// Initialiser MQTT
initializeMQTT();

// Endpoint Écouter un topic via l'API avec QoS 1 pour une meilleure fiabilité
app.post('/api/ecouter-topic', (req, res) => {
  const topic = req.body.topic;

  if (!MQTT_ENABLED || !mqttClient) {
    return res.status(503).json({ message: 'MQTT non disponible' });
  }

  if (!topic || subscribedTopics.has(topic)) {
    return res.status(200).json({ message: 'Déjà en écoute ou invalide' });
  }

  mqttClient.subscribe(topic, { qos: 1 }, (err) => {
    if (!err) {
      subscribedTopics.add(topic);
      console.log(`🎧 Écoute du topic: ${topic}`);
      res.status(200).json({ message: `Topic ${topic} abonné avec succès.` });
    } else {
      res.status(500).json({ message: 'Erreur abonnement topic' });
    }
  });
});

// Endpoint pour vérifier l'état de la connexion MQTT
app.get('/api/mqtt/status', (req, res) => {
  const status = {
    mqtt_enabled: MQTT_ENABLED,
    publisher_enabled: MQTT_PUBLISHER_ENABLED,
    listener_connected: mqttClient?.connected || false,
    publisher_connected: mqttPublisher?.connected || false,
    pending_messages: pendingMessages.length,
    subscribed_topics: Array.from(subscribedTopics)
  };
  
  res.json(status);
});

// Endpoint pour reconnecter manuellement le publisher MQTT
app.post('/api/reconnect-publisher', (req, res) => {
  try {
    reconnectPublisher();
    res.json({ message: 'Reconnexion du publisher initiée' });
  } catch (error) {
    console.error('Erreur reconnexion publisher:', error);
    res.status(500).json({ message: 'Erreur reconnexion publisher' });
  }
});

// Endpoint pour se désabonner d'un topic MQTT
app.post('/api/desabonner-topic', (req, res) => {
  const { topic } = req.body;
  
  if (!MQTT_ENABLED || !mqttClient) {
    return res.status(503).json({ message: 'MQTT non disponible' });
  }
  
  if (!topic || !subscribedTopics.has(topic)) {
    return res.status(200).json({ message: 'Topic non abonné' });
  }

  mqttClient.unsubscribe(topic, {}, async (err) => {
    if (err) {
      console.error('Erreur désabonnement:', err);
      return res.status(500).json({ message: 'Erreur désabonnement MQTT' });
    }
    
    subscribedTopics.delete(topic);
    console.log(`❌ Désabonné de ${topic}`);
    
    // Mettre à jour le statut du chauffeur si c'est un topic de chauffeur
    const parts = topic.split('/');
    if (parts.length >= 3 && parts[0] === 'chauffeur') {
      const chauffeurId = parts[1];
      try {
        await updateStatut(chauffeurId, { 
          disponible: false,
          en_ligne: false,
          en_course: false
        });
      } catch (error) {
        console.error('Erreur mise à jour statut:', error);
      }
    }

    res.json({ message: `Désabonnement de ${topic} réussi` });
  });
});

// Endpoint pour s'abonner au topic de statut d'un chauffeur
app.post('/api/chauffeur/:id/subscribe-status', (req, res) => {
  const { id } = req.params;
  const statusTopic = `chauffeur/${id}/status`;
  
  if (!MQTT_ENABLED || !mqttClient) {
    return res.status(503).json({ message: 'MQTT non disponible' });
  }
  
  if (subscribedTopics.has(statusTopic)) {
    return res.status(200).json({ message: `Déjà abonné au topic de statut de ${id}` });
  }
  
  mqttClient.subscribe(statusTopic, { qos: 1 }, (err) => {
    if (!err) {
      subscribedTopics.add(statusTopic);
      logger.info(`🎧 Abonnement manuel au topic de statut: ${statusTopic}`);
      res.json({ message: `Abonné au topic de statut de ${id}`, topic: statusTopic });
    } else {
      logger.error(`❌ Erreur abonnement au topic de statut ${statusTopic}:`, err);
      res.status(500).json({ message: 'Erreur abonnement topic de statut' });
    }
  });
});

// Endpoints pour la gestion des statuts des chauffeurs
app.get('/api/chauffeurs/status', async (req, res) => {
  try {
    const keys = await redis.keys('chauffeur:*');
    const chauffeurs = [];
    
    for (const key of keys) {
      const chauffeurId = key.split(':')[1];
      const statut = await redis.hgetall(key);
      if (statut && Object.keys(statut).length > 0) {
        chauffeurs.push({
          id: chauffeurId,
          ...statut,
          disponible: statut.disponible === '1',
          en_ligne: statut.en_ligne === '1',
          en_course: statut.en_course === '1'
        });
      }
    }
    
    res.json({ chauffeurs });
  } catch (error) {
    logger.error('Erreur récupération statuts:', error);
    res.status(500).json({ error: 'Erreur serveur' });
  }
});

app.post('/api/chauffeurs/:id/status', async (req, res) => {
  const { id } = req.params;
  const status = req.body;

  // Validation
  if (typeof status.disponible !== 'boolean' || 
      typeof status.en_ligne !== 'boolean') {
    return res.status(400).json({ error: 'Statut invalide' });
  }

  try {
    const key = `chauffeur:${id}`;
    await redis.hset(key, {
      disponible: status.disponible ? '1' : '0',
      en_ligne: status.en_ligne ? '1' : '0',
      en_course: status.en_course ? '1' : '0',
      updated_at: Date.now()
    });

    const current = await redis.hgetall(key);
    logger.debug('Statut confirmé:', current);
    res.json(current);
  } catch (error) {
    logger.error('Erreur statut', error);
    res.status(500).json({ error: 'Erreur serveur' });
  }
});

// Endpoints pour la communication par réservation
app.post('/api/reservation/subscribe', (req, res) => {
  const { reservation_id } = req.body;
  const topic = `${RESERVATION_TOPIC_PREFIX}${reservation_id}`;
  
  if (!MQTT_ENABLED || !mqttClient) {
    return res.status(503).json({ message: 'MQTT non disponible' });
  }
  
  if (subscribedTopics.has(topic)) {
    return res.json({ message: `Déjà abonné au topic de réservation ${reservation_id}` });
  }
  
  mqttClient.subscribe(topic, { qos: 1 }, (err) => {
    if (!err) {
      subscribedTopics.add(topic);
      console.log(`🎧 Écoute du topic: ${topic}`);
      res.json({ message: `Abonné au topic de réservation ${reservation_id}` });
    } else {
      res.status(500).json({ error: 'Erreur abonnement topic' });
    }
  });
});

app.post('/api/reservation/send-message', async (req, res) => {
  const { reservation_id, message } = req.body;
  const topic = `${RESERVATION_TOPIC_PREFIX}${reservation_id}`;
  
  if (!MQTT_PUBLISHER_ENABLED || !mqttPublisher?.connected) {
    return res.status(503).json({ error: 'Publisher MQTT non disponible' });
  }
  
  try {
    mqttPublisher.publish(topic, JSON.stringify({
      type: 'chat',
      from: message.from,
      content: message.content
    }), { qos: 1 });
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: 'Erreur envoi message' });
  }
});

// Capteur de messages MQTT
if (mqttClient) {
  mqttClient.on('message', async (topic, message) => {
    try {
      const payload = message.toString();
      const data = JSON.parse(payload);

      logger.info(`Message reçu sur ${topic}:`, { payload });
      
      // Traitement des différents types de topics
      if (topic === RESERVATIONS_RECENTES_TOPIC) {
        await handleNewReservation(data);
      } else if (topic.startsWith(RESERVATION_TOPIC_PREFIX)) {
        const reservationId = topic.split('/')[2];
        await handleReservationMessage(reservationId, data);
      } else if (topic.match(/^chauffeur\/.+\/status$/)) {
        const chauffeurId = topic.split('/')[1];
        await handleChauffeurStatusUpdate(chauffeurId, data);
      } else if (topic.match(/^chauffeur\/.+\/position$/)) {
        const chauffeurId = topic.split('/')[1];
        await handlePosition(chauffeurId, data.data || data);
      } else if (topic.match(/^ktur\/reservations\/.+\/position$/)) {
        const reservationId = topic.split('/')[2];
        await handleReservationPosition(reservationId, data);
      } else if (topic.match(/^chauffeur\/.+\/.*$/)) {
        // Traitement des messages généraux des chauffeurs
        const chauffeurId = topic.split('/')[1];
        await handleChauffeurGeneralMessage(chauffeurId, data);
      }
    } catch (err) {
      logger.error('Erreur traitement message', { error: err.message, topic });
    }
  });
}

// Nouvelle réservation reçue
async function handleNewReservation(data) {
  console.log(`Nouvelle réservation reçue: ${data.reservation_id}`);
}

// Gestion des messages sur topic de réservation
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
      await handleReservationAcceptance(reservationId, data);
      break;
    case 'debut':
    case 'fin':
      await handleReservationStatusChange(reservationId, data);
      break;
    default:
      logger.warn('Type de message non géré', { type: data.type, reservation_id: reservationId });
  }
}

// Chat par réservation
async function handleChatMessage(reservationId, data) {
  const key = `chat:${reservationId}:messages`;
  await redis.lpush(key, JSON.stringify({
    from: data.from,
    content: data.content,
    timestamp: new Date().toISOString()
  }));
  
  const messageCount = await redis.llen(key);
  if (messageCount >= 100) {
    await archiveChatMessages(reservationId);
  }
}

async function archiveChatMessages(reservationId) {
  const key = `chat:${reservationId}:messages`;
  const messages = await redis.lrange(key, 0, -1);
  
  try {
    await axios.post(`${process.env.LARAVEL_API_URL}/api/chat/archive`, {
      reservation_id: reservationId,
      messages: messages.map(msg => JSON.parse(msg))
    });
    await redis.del(key);
    logger.info(`📦 Chat archivé pour la réservation ${reservationId}`);
  } catch (err) {
    logger.error('Erreur archivage chat', { error: err.message });
  }
}

async function handleReservationPosition(reservationId, data) {
  const key = `reservation:${reservationId}:position`;
  
  const positionData = {
    lat: data.lat,
    lng: data.lng,
    chauffeur_id: data.chauffeur_id,
    reservation_status: data.reservation_status || 'active',
    is_in_reservation: true,
    updated_at: Date.now(),
    accuracy: data.accuracy || null,
    speed: data.speed || null,
    heading: data.heading || null
  };
  
  await redis.hset(key, positionData);
  
  logger.info(`📍 Position de réservation mise à jour`, {
    reservation_id: reservationId,
    chauffeur_id: data.chauffeur_id,
    lat: data.lat,
    lng: data.lng
  });
  
  // Publier la position de réservation
  if (mqttPublisher?.connected) {
    const topic = `ktur/reservations/${reservationId}/position`;
    const payload = JSON.stringify({
      type: 'reservation_position',
      reservation_id: reservationId,
      chauffeur_id: data.chauffeur_id,
      position: {
        lat: data.lat,
        lng: data.lng,
        accuracy: data.accuracy,
        speed: data.speed,
        heading: data.heading
      },
      timestamp: Date.now()
    });
    
    mqttPublisher.publish(topic, payload, { qos: 1 });
    logger.debug(`📡 Position de réservation publiée sur ${topic}`);
  }
}

async function handleReservationAcceptance(reservationId, data) {
  try {
    // Créer le topic dédié à cette réservation
    const reservationTopic = `${RESERVATION_TOPIC_PREFIX}${reservationId}`;
    if (!subscribedTopics.has(reservationTopic)) {
      mqttClient.subscribe(reservationTopic, { qos: 1 }, (err) => {
        if (!err) {
          subscribedTopics.add(reservationTopic);
          console.log(`🎧 Écoute du topic: ${reservationTopic}`);
        } else {
          console.error(`❌ Erreur abonnement ${reservationTopic}:`, err);
        }
      });
    }

    // Publier le message MQTT pour OneSignal
    if (mqttPublisher?.connected) {
      const onesignalTopic = 'ktur/reservations/onesignal/acceptation';
      const onesignalPayload = JSON.stringify({
        type: 'reservation_accepted',
        reservation_id: reservationId,
        chauffeur_id: data.chauffeur_id,
        action: 'hide_notification',
        timestamp: Date.now()
      });
      
      mqttPublisher.publish(onesignalTopic, onesignalPayload, { qos: 1 });
      logger.info(`📱 Message OneSignal publié pour masquer les notifications`);
    }

    // Notifier Laravel et mettre à jour le statut
    await notifyLaravel('/reservation/acceptation', {
      reservation_id: reservationId,
      chauffeur_id: data.chauffeur_id
    });
    
    // Mettre à jour automatiquement le statut du chauffeur
    await updateStatut(data.chauffeur_id, {
      en_ligne: true,
      en_course: true,
      disponible: false
    });

    // Publier le statut mis à jour
    await publishChauffeurStatus(data.chauffeur_id);
    
    logger.info(`✅ Réservation ${reservationId} acceptée par chauffeur ${data.chauffeur_id}`);
  } catch (error) {
    logger.error(`❌ Erreur lors de l'acceptation de réservation ${reservationId}:`, error);
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
  // Archivage final du chat
  const chatKey = `chat:${reservationId}:messages`;
  if (await redis.exists(chatKey)) {
    await archiveChatMessages(reservationId);
  }
  
  // Suppression des données Redis
  await redis.del(`reservation:${reservationId}:position`);
  
  // Désabonnement du topic
  const topic = `${RESERVATION_TOPIC_PREFIX}${reservationId}`;
  if (subscribedTopics.has(topic)) {
    mqttClient.unsubscribe(topic);
    subscribedTopics.delete(topic);
  }
}

// === END AJOUT ===

// Fonctions utilitaires
async function notifyLaravel(endpoint, payload) {
  try {
    await axios.post(`${process.env.LARAVEL_API_URL}${endpoint}`, payload, {
      headers: { 
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      }
    });
    logger.info(`✅ Notification envoyée vers ${endpoint}`);
  } catch (err) {
    logger.error(`❌ Erreur appel Laravel ${endpoint}:`, err.response ? err.response.data : err.message);
  }
}

async function updateStatut(chauffeurId, fields) {
  const key = `chauffeur:${chauffeurId}`;
  
  // Debug: Log avant modification
  const before = await redis.hgetall(key);
  logger.debug('Statut AVANT mise à jour', { chauffeurId, before, newValues: fields });

  const mapping = {
    disponible: fields.disponible ? '1' : '0',
    en_ligne: fields.en_ligne ? '1' : '0',
    en_course: fields.en_course ? '1' : '0',
    updated_at: Date.now()
  };

  await redis.hset(key, mapping);
  
  // Debug: Log après modification
  const after = await redis.hgetall(key);
  logger.debug('Statut APRÈS mise à jour', { chauffeurId, after });

  await publishChauffeurStatus(chauffeurId);
}

async function publishChauffeurStatus(chauffeurId) {
  try {
    if (!MQTT_ENABLED || !mqttPublisher) {
      logger.warn('MQTT non disponible, publication ignorée');
      return;
    }
    
    const key = `chauffeur:${chauffeurId}`;
    const statut = await redis.hgetall(key);
    
    if (statut && Object.keys(statut).length > 0) {
      const statusData = {
        chauffeur_id: chauffeurId,
        disponible: statut.disponible === '1',
        en_ligne: statut.en_ligne === '1',
        en_course: statut.en_course === '1',
        latitude: parseFloat(statut.latitude) || null,
        longitude: parseFloat(statut.longitude) || null,
        updated_at: parseInt(statut.updated_at) || Date.now(),
        timestamp: new Date().toISOString()
      };
      
      const message = {
        topic: `chauffeur/${chauffeurId}/status`,
        payload: JSON.stringify(statusData),
        options: { qos: 1 },
        type: 'status'
      };
      
      if (!mqttPublisher.connected) {
        if (pendingMessages.length < MAX_PENDING_MESSAGES) {
          pendingMessages.push(message);
          logger.warn(`Publisher MQTT non connecté, statut de ${chauffeurId} mis en file d'attente`);
        } else {
          logger.warn('File d\'attente pleine, message ignoré');
        }
        return;
      }
      
      mqttPublisher.publish(message.topic, message.payload, message.options);
      logger.info(`📡 Statut publié pour chauffeur ${chauffeurId}`);
    }
  } catch (error) {
    logger.error('Erreur publication statut MQTT:', error);
  }
}

// Fonction générique pour publier des messages MQTT avec gestion de la file d'attente
async function publishMQTTMessage(topic, payload, options = { qos: 1, retain: false }) {
  if (!mqttPublisher?.connected) {
    if (pendingMessages.length < MAX_PENDING_MESSAGES) {
      pendingMessages.push({ topic, payload, options });
      logger.debug(`📋 Message en attente pour ${topic}`);
    } else {
      logger.warn('File d\'attente pleine, message ignoré');
    }
    return;
  }
  
  mqttPublisher.publish(topic, payload, options);
  logger.debug(`📡 Message publié sur ${topic}`);
}

// Publier la position d'un chauffeur (générale)
async function publishChauffeurPosition(chauffeurId, lat, lng) {
  const topic = `chauffeur/${chauffeurId}/position`;
  const payload = JSON.stringify({
    type: 'general_position',
    chauffeur_id: chauffeurId,
    data: { lat, lng, timestamp: Date.now() }
  });
  
  await publishMQTTMessage(topic, payload);
  logger.info(`📍 Position générale publiée pour chauffeur ${chauffeurId}`);
}

// Publier la position d'un chauffeur pendant une réservation
async function publishReservationPosition(reservationId, chauffeurId, lat, lng, additionalData = {}) {
  const topic = `ktur/reservations/${reservationId}/position`;
  const payload = JSON.stringify({
    type: 'reservation_position',
    reservation_id: reservationId,
    chauffeur_id: chauffeurId,
    position: { lat, lng, ...additionalData },
    timestamp: Date.now()
  });
  
  await publishMQTTMessage(topic, payload);
  logger.info(`📍 Position de réservation publiée pour ${reservationId}`);
}

async function handlePosition(id, positionData) {
  // Validation des données
  if (!positionData || typeof positionData.lat !== 'number' || typeof positionData.lng !== 'number') {
    logger.error('Données de position invalides', { id, received: positionData });
    return;
  }

  const key = `chauffeur:${id}`;
  try {
    const previous = await redis.hgetall(key);
    
    const update = {
      latitude: positionData.lat,
      longitude: positionData.lng,
      accuracy: positionData.accuracy || null,
      speed: positionData.speed || null,
      heading: positionData.heading || null,
      is_in_reservation: false,
      updated_at: Date.now()
    };

    // Éviter les écritures inutiles
    if (previous.latitude === update.latitude.toString() && 
        previous.longitude === update.longitude.toString()) {
      return;
    }

    await redis.hset(key, update);
    logger.debug(`📍 Position générale mise à jour pour ${id}`, {
      lat: positionData.lat,
      lng: positionData.lng,
      accuracy: positionData.accuracy
    });

    // Mettre à jour automatiquement le statut
    const currentStatus = await redis.hgetall(key);
    const isEnCourse = currentStatus.en_course === '1';
    
    await redis.hset(key, {
      en_ligne: '1',
      disponible: isEnCourse ? '0' : '1',
      updated_at: Date.now()
    });
    
    logger.info(`🔄 Statut automatique mis à jour pour ${id}: en_ligne=1, disponible=${isEnCourse ? '0' : '1'}`);

    // S'abonner automatiquement au topic de statut du chauffeur
    const statusTopic = `chauffeur/${id}/status`;
    if (!subscribedTopics.has(statusTopic)) {
      mqttClient.subscribe(statusTopic, { qos: 1 }, (err) => {
        if (!err) {
          subscribedTopics.add(statusTopic);
          logger.info(`🎧 Abonnement automatique au topic de statut: ${statusTopic}`);
        } else {
          logger.error(`❌ Erreur abonnement au topic de statut ${statusTopic}:`, err);
        }
      });
    }

    // Publier la position et le statut
    await publishChauffeurPosition(id, positionData.lat, positionData.lng);
    await publishChauffeurStatus(id);
    
  } catch (err) {
    logger.error('Erreur Redis', { id, error: err.message });
  }
}



// Fonction pour traiter les statuts publiés par les chauffeurs
async function handleChauffeurStatusUpdate(chauffeurId, data) {
  try {
    const key = `chauffeur:${chauffeurId}`;
    const isOnline = data.statut === 1;
    
    await redis.hset(key, {
      en_ligne: isOnline ? '1' : '0',
      disponible: isOnline ? '1' : '0',
      en_course: '0',
      updated_at: Date.now()
    });
    
    // Si une position est fournie, l'enregistrer aussi
    if (data.position?.latitude && data.position?.longitude) {
      await redis.hset(key, {
        latitude: data.position.latitude.toString(),
        longitude: data.position.longitude.toString(),
        updated_at: Date.now()
      });
    }
    
    logger.info(`🔄 Statut chauffeur ${chauffeurId} mis à jour via MQTT: ${isOnline ? 'EN LIGNE' : 'HORS LIGNE'}`);
    await publishChauffeurStatus(chauffeurId);
    
  } catch (error) {
    logger.error(`❌ Erreur lors de la mise à jour du statut chauffeur ${chauffeurId}:`, error);
  }
}

// Fonction pour traiter les messages généraux des chauffeurs
async function handleChauffeurGeneralMessage(chauffeurId, data) {
  try {
    if (data.statut !== undefined) {
      await handleChauffeurStatusUpdate(chauffeurId, data);
    } else if (data.position) {
      await handlePosition(chauffeurId, data.position);
    } else {
      logger.debug(`📨 Message général reçu de ${chauffeurId}:`, data);
    }
  } catch (error) {
    logger.error(`❌ Erreur lors du traitement du message général de ${chauffeurId}:`, error);
  }
}

// Fonction pour gérer automatiquement le statut hors ligne des chauffeurs inactifs
async function checkInactiveChauffeurs() {
  try {
    const keys = await redis.keys('chauffeur:*');
    const now = Date.now();
    const INACTIVITY_THRESHOLD = 5 * 60 * 1000; // 5 minutes d'inactivité
    
    for (const key of keys) {
      const chauffeurId = key.split(':')[1];
      const chauffeur = await redis.hgetall(key);
      
      if (chauffeur.updated_at && chauffeur.en_ligne === '1') {
        const lastUpdate = parseInt(chauffeur.updated_at);
        const timeSinceLastUpdate = now - lastUpdate;
        
        if (timeSinceLastUpdate > INACTIVITY_THRESHOLD) {
          logger.info(`🕐 Chauffeur ${chauffeurId} inactif depuis ${Math.round(timeSinceLastUpdate / 1000)}s - Passage hors ligne`);
          
          await redis.hset(key, {
            en_ligne: '0',
            disponible: '0',
            updated_at: now
          });
          
          await publishChauffeurStatus(chauffeurId);
        }
      }
    }
  } catch (error) {
    logger.error('Erreur lors de la vérification des chauffeurs inactifs:', error);
  }
}

// Démarrer la vérification périodique des chauffeurs inactifs
setInterval(checkInactiveChauffeurs, 60000);

app.listen(PORT, () => {
  console.log(`🚀 Serveur ecouteur MQTT en écoute sur le port ${PORT}`);
  console.log(`📡 Topics de diffusion:`);
  console.log(`   - ${STATUS_TOPIC} : Statut des chauffeurs`);
  console.log(`   - ${CHAUFFEUR_GENERAL_POSITION_TOPIC} : Position générale des chauffeurs`);
  console.log(`   - ${RESERVATION_POSITION_TOPIC} : Position pendant réservation`);
  console.log(`   - ${RESERVATIONS_RECENTES_TOPIC} : Nouvelles réservations`);
  console.log(`   - ${RESERVATION_TOPIC_PREFIX}* : Messages de réservation`);
});

process.on('SIGINT', () => {
  console.log('\n🛑 Arrêt du serveur...');
  if (mqttClient) mqttClient.end();
  if (mqttPublisher) mqttPublisher.end();
  redis.disconnect();
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log('\n🛑 Arrêt du serveur...');
  if (mqttClient) mqttClient.end();
  if (mqttPublisher) mqttPublisher.end();
  redis.disconnect();
  process.exit(0);
});

process.on('uncaughtException', (err) => {
  console.error('❌ Erreur non capturée:', err);
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('❌ Promesse rejetée non gérée:', reason);
  process.exit(1);
});