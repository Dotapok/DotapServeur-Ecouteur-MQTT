// Import des services modulaires
const config = require('./src/config');
const logger = require('./src/utils/logger');
const mqttService = require('./src/services/mqtt');
const redisService = require('./src/services/redis');
const cacheService = require('./src/services/cache');
const queueService = require('./src/services/queue');

// Import des routes API modulaires
const apiRoutes = require('./src/api/routes');

const express = require('express');
const cors = require('cors');
const axios = require('axios');

// ---------------------- Configuration Application ----------------------
const app = express();
app.use(cors());
app.use(express.json());

// ---------------------- Initialisation des Services ----------------------
async function initializeServices() {
  try {
    logger.info('Initialisation des services...');
    
    // Initialiser Redis en premier (dépendance pour les autres services)
    await redisService.initialize();
    logger.info('Service Redis initialisé');
    
    // Initialiser la queue Redis Streams
    await queueService.initialize();
    logger.info('Service Queue initialisé');
    
    // Initialiser MQTT
    if (config.isMQTTEnabled()) {
      mqttService.initialize();
      logger.info('Service MQTT initialisé');
    } else {
      logger.warn('MQTT désactivé');
    }
    
    logger.info('Tous les services initialisés avec succès');
    
  } catch (error) {
    logger.error('Erreur initialisation services:', error.message);
    process.exit(1);
  }
}

// ---------------------- Handlers MQTT Modulaires ----------------------

// Parseur JSON sécurisé
function safeJsonParse(buffer) {
  try {
    return JSON.parse(buffer.toString());
  } catch (e) {
    if (config.IS_DEBUG) {
      logger.warn('Message MQTT non-JSON ignoré');
    }
    return null;
  }
}

// Vérifier si la position a changé (utilise le cache Redis)
async function hasPositionChanged(previous, current) {
  return !previous || previous.lat !== current.lat || previous.lng !== current.lng;
}

// Mise à jour Redis avec pipeline
async function redisUpdate(key, data) {
  try {
    await redisService.hmset(key, data);
    return true;
  } catch (err) {
    logger.error('Erreur mise à jour Redis', { key, error: err.message });
    return false;
  }
}

// Comparer les hash Redis
function isSameHash(current, next) {
  const keys = Object.keys(next || {});
  for (const k of keys) {
    const cur = current?.[k];
    const nxt = next[k] !== undefined && next[k] !== null ? String(next[k]) : '';
    if (String(cur ?? '') !== nxt) return false;
  }
  return true;
}

// Construction complète du hash chauffeur
function buildCompleteChauffeurHash(current, update, nowTs) {
  const result = {};
  result.latitude = update.latitude !== undefined ? update.latitude : (current.latitude !== undefined ? current.latitude : '');
  result.longitude = update.longitude !== undefined ? update.longitude : (current.longitude !== undefined ? current.longitude : '');
  result.accuracy = update.accuracy !== undefined ? update.accuracy : (current.accuracy !== undefined ? current.accuracy : '');
  result.speed = update.speed !== undefined ? update.speed : (current.speed !== undefined ? current.speed : '');
  result.heading = update.heading !== undefined ? update.heading : (current.heading !== undefined ? current.heading : '');
  result.en_ligne = update.en_ligne !== undefined ? update.en_ligne : (current.en_ligne !== undefined ? current.en_ligne : '0');
  result.disponible = update.disponible !== undefined ? update.disponible : (current.disponible !== undefined ? current.disponible : '0');
  result.en_course = update.en_course !== undefined ? update.en_course : (current.en_course !== undefined ? current.en_course : '0');
  result.updated_at = update.updated_at !== undefined ? update.updated_at : (current.updated_at !== undefined ? current.updated_at : nowTs);
  return result;
}

// ---------------------- Handlers MQTT ----------------------

async function handleMqttMessage(topic, messageBuffer) {
  try {
    logger.debug('MQTT message reçu', { topic, bytes: messageBuffer?.length || 0 });
  } catch (_) {}

  const data = safeJsonParse(messageBuffer);
  if (!data) return;

  try {
    if (typeof data === 'object') {
      const meta = {
        topic,
        type: data.type || null,
        keys: Object.keys(data || {}).slice(0, 6)
      };
      logger.debug('MQTT message parsé', meta);
    }

    // Router les messages basés sur les patterns de topics
    if (topic === config.MQTT_TOPICS.RESERVATIONS_RECENTES) {
      await handleNewReservation(data);
    } else if (topic === config.MQTT_TOPICS.PASSAGER_STATUS) {
      await handlePassagerStatus(data);
    } else if (topic === config.MQTT_TOPICS.PASSAGER_POSITION) {
      await handlePassagerPosition(data);
    } else if (topic.startsWith(config.MQTT_TOPICS.RESERVATION_PREFIX)) {
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

// ---------------------- Handlers Spécifiques ----------------------

async function handleChauffeurPosition(chauffeurId, positionData) {
  if (!positionData || typeof positionData.lat !== 'number' || typeof positionData.lng !== 'number') {
    return;
  }

  const now = Date.now();
  
  // Vérifier le cache Redis pour le throttling
  const cachedPosition = await cacheService.getDriverPosition(chauffeurId);
  const newPosition = { lat: positionData.lat, lng: positionData.lng, ts: now };

  // Throttling: skip si position inchangée et dans la période de throttle
  if (cachedPosition && !(await hasPositionChanged(cachedPosition, newPosition)) && 
      (now - cachedPosition.ts) < config.POSITION_THROTTLE_MS) {
    
    try {
      await redisUpdate(config.getChauffeurKey(chauffeurId), {
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
    const key = config.getChauffeurKey(chauffeurId);
    const positionUpdate = {
      latitude: positionData.lat,
      longitude: positionData.lng,
      accuracy: positionData.accuracy || '',
      speed: positionData.speed || '',
      heading: positionData.heading || '',
      updated_at: now
    };
    
    const current = await redisService.hgetall(key);
    const complete = buildCompleteChauffeurHash(current || {}, positionUpdate, now);

    if (!isSameHash(current, complete)) {
      logger.info('Préparation mise à jour position chauffeur', { key, chauffeurId, update: complete });
      await redisUpdate(key, complete);
      const after = await redisService.hgetall(key);
      logger.info('Mise à jour Redis position chauffeur effectuée', { key, fields: Object.keys(after) });
    } else if (config.IS_DEBUG) {
      logger.debug('Position inchangée - mise à jour Redis ignorée', { key, chauffeurId });
    }

    // Mettre à jour le cache Redis
    await cacheService.setDriverPosition(chauffeurId, newPosition);

    // Publier les updates de position et statut
    await publishChauffeurPosition(chauffeurId, positionData.lat, positionData.lng);
    await publishChauffeurStatus(chauffeurId, { source: 'server' });

    if (config.IS_DEBUG) {
      logger.debug('Position chauffeur mise à jour', { chauffeurId });
    }
  } catch (err) {
    logger.error('Erreur mise à jour position chauffeur', { chauffeurId, error: err.message });
  }
}

// ---------------------- Publication MQTT ----------------------

async function publishChauffeurStatus(chauffeurId, data) {
  const topic = config.getChauffeurStatusTopic(chauffeurId);
  const payload = JSON.stringify({
    ...data,
    chauffeur_id: chauffeurId,
    timestamp: Date.now(),
    is_server_message: true
  });

  try {
    const success = mqttService.publish(topic, payload, { qos: 1, retain: true });
    if (success) {
      await cacheService.setStatusPublishTimestamp(chauffeurId);
    }
  } catch (err) {
    logger.error('Erreur publication statut chauffeur', { chauffeurId, error: err.message });
  }
}

async function publishChauffeurPosition(chauffeurId, lat, lng) {
  const topic = config.getChauffeurPositionTopic(chauffeurId);
  const payload = JSON.stringify({
    lat,
    lng,
    chauffeur_id: chauffeurId,
    timestamp: Date.now(),
    source: 'server'
  });

  try {
    mqttService.publish(topic, payload, { qos: 1 });
  } catch (err) {
    logger.error('Erreur publication position chauffeur', { chauffeurId, error: err.message });
  }
}

// ---------------------- API Routes ----------------------

// Montage des routes API modulaires
app.use('/api', apiRoutes);

// ---------------------- Graceful Shutdown ----------------------

async function gracefulShutdown(signal) {
  logger.info(`Reçu ${signal}, fermeture gracieuse...`);
  
  try {
    // Fermer MQTT
    if (config.isMQTTEnabled()) {
      await mqttService.shutdown();
    }
    
    // Fermer Redis
    await redisService.shutdown();
    
    logger.info(' Tous les services fermés proprement');
    process.exit(0);
    
  } catch (error) {
    logger.error(' Erreur lors de la fermeture gracieuse:', error.message);
    process.exit(1);
  }
}

// ---------------------- Démarrage Application ----------------------

async function startServer() {
  try {
    // Initialiser les services
    await initializeServices();
    
    // Démarrer le serveur
    app.listen(config.PORT, () => {
      logger.info(` Serveur démarré sur le port ${config.PORT}`);
      logger.info(` Environnement: ${config.NODE_ENV}`);
      logger.info(` Log level: ${config.LOG_LEVEL}`);
    });
    
    // Gestion des signaux pour le graceful shutdown
    process.on('SIGINT', () => gracefulShutdown('SIGINT'));
    process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
    process.on('SIGUSR2', () => gracefulShutdown('SIGUSR2')); // Pour nodemon
    
  } catch (error) {
    logger.error(' Erreur démarrage serveur:', error.message);
    process.exit(1);
  }
}

// Démarrer l'application
startServer();

// Export pour les tests
module.exports = { app, initializeServices, gracefulShutdown };