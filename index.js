require('dotenv').config();
const express = require('express');
const mqtt = require('mqtt');
const Redis = require('ioredis');
const cors = require('cors');
const axios = require('axios');
const { createLogger, format, transports } = require('winston');
require('winston-daily-rotate-file');

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

// Configuration MQTT avec valeurs par défaut
const MQTT_BROKER_URL = process.env.MQTT_BROKER_URL || 'mqtt://localhost:1883';
const MQTT_USERNAME = process.env.MQTT_USERNAME || '';
const MQTT_PASSWORD = process.env.MQTT_PASSWORD || '';
const MQTT_ENABLED = process.env.MQTT_ENABLED !== 'false'; // Désactiver avec MQTT_ENABLED=false
const MQTT_PUBLISHER_ENABLED = process.env.MQTT_PUBLISHER_ENABLED !== 'false'; // Désactiver publisher avec MQTT_PUBLISHER_ENABLED=false

console.log('🔧 Configuration MQTT:');
console.log(`   Broker: ${MQTT_BROKER_URL}`);
console.log(`   Username: ${MQTT_USERNAME || 'non défini'}`);
console.log(`   Activé: ${MQTT_ENABLED}`);
console.log(`   Publisher: ${MQTT_PUBLISHER_ENABLED}`);

// Variables pour les clients MQTT
let mqttClient = null;
let mqttPublisher = null;

// Initialisation Redis
const redis = new Redis(process.env.REDIS_URL);
redis.on('connect', () => console.log('✅ Connecté à Redis'));
redis.on('error', err => console.error('❌ Erreur Redis:', err));

// Initialisation conditionnelle des clients MQTT
if (MQTT_ENABLED) {
  // Initialisation MQTT Client (pour écouter les chauffeurs)
  mqttClient = mqtt.connect(MQTT_BROKER_URL, {
    username: MQTT_USERNAME,
    password: MQTT_PASSWORD,
    clientId: 'ktur_listener_client',
    reconnectPeriod: 5000,
    connectTimeout: 30000,
    clean: true,
    keepalive: 60,
    rejectUnauthorized: false
  });

  mqttClient.on('connect', () => {
    console.log('✅ Connecté à MQTT (Listener)');
    // S'abonner au topic des réservations récentes si non abonné
    const topic = 'ktur/reservations/recentes';
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

  mqttClient.on('error', err => {
    console.error('❌ Erreur de connexion à MQTT:', err.message);
    console.error('   Code:', err.code);
  });

  mqttClient.on('close', (hadError) => {
    console.log(`🔌 Connexion MQTT fermée${hadError ? ' avec erreur' : ''}`);
  });

  mqttClient.on('reconnect', () => {
    console.log('🔄 Reconnexion MQTT...');
  });

  mqttClient.on('offline', () => {
    console.log('📴 MQTT Listener hors ligne');
  });

  // Initialisation MQTT Publisher (pour propager les statuts vers les clients)
  if (MQTT_PUBLISHER_ENABLED) {
    mqttPublisher = mqtt.connect(MQTT_BROKER_URL, {
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
    });

    mqttPublisher.on('connect', () => {
      console.log('✅ Publisher MQTT connecté');
      mqttPublisher.publish('ktur/server/status', JSON.stringify({ 
        status: 'online', 
        timestamp: new Date().toISOString() 
      }), { qos: 1, retain: true });
      
      processPendingMessages();
    });

    mqttPublisher.on('error', err => {
      console.error('❌ Erreur Publisher MQTT:', err.message);
      console.error('   Code:', err.code);
    });

    mqttPublisher.on('close', (hadError) => {
      console.log(`🔌 Publisher MQTT fermé${hadError ? ' avec erreur' : ''}`);
    });

    mqttPublisher.on('reconnect', () => {
      console.log('🔄 Reconnexion Publisher MQTT...');
    });

    mqttPublisher.on('offline', () => {
      console.log('📴 Publisher MQTT hors ligne');
    });

    const heartbeatInterval = setInterval(() => {
      if (mqttPublisher && mqttPublisher.connected) {
        mqttPublisher.publish('ktur/server/heartbeat', JSON.stringify({ 
          timestamp: new Date().toISOString() 
        }), { qos: 0, retain: false });
      }
    }, 30000);

    mqttPublisher.on('close', () => {
      clearInterval(heartbeatInterval);
    });
  } else {
    console.log('⚠️  Publisher MQTT désactivé');
  }
} else {
  console.log('⚠️  MQTT désactivé - les fonctionnalités MQTT ne seront pas disponibles');
}

// Store des topics écoutés
const subscribedTopics = new Set();

// File d'attente pour les messages différés quand le publisher n'est pas connecté
const pendingMessages = [];
const MAX_PENDING_MESSAGES = 100;

// Fonction pour traiter la file d'attente des messages différés
function processPendingMessages() {
  if (!mqttPublisher || !mqttPublisher.connected) {
    return;
  }
  
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
  
  if (mqttPublisher && mqttPublisher.connected) {
    console.log('ℹ️  Publisher déjà connecté');
    return;
  }
  
  console.log('🔄 Reconnexion manuelle du Publisher MQTT...');
  
  if (mqttPublisher) {
    mqttPublisher.end();
  }
  
  mqttPublisher = mqtt.connect(MQTT_BROKER_URL, {
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
  });

  mqttPublisher.on('connect', () => {
    console.log('✅ Publisher MQTT reconnecté');
    mqttPublisher.publish('ktur/server/status', JSON.stringify({ 
      status: 'online', 
      timestamp: new Date().toISOString() 
    }), { qos: 1, retain: true });
    
    processPendingMessages();
  });

  mqttPublisher.on('error', err => {
    console.error('❌ Erreur Publisher MQTT:', err.message);
  });

  mqttPublisher.on('close', (hadError) => {
    console.log(`🔌 Publisher MQTT fermé${hadError ? ' avec erreur' : ''}`);
  });

  mqttPublisher.on('offline', () => {
    console.log('📴 Publisher MQTT hors ligne');
  });

  const heartbeatInterval = setInterval(() => {
    if (mqttPublisher && mqttPublisher.connected) {
      mqttPublisher.publish('ktur/server/heartbeat', JSON.stringify({ 
        timestamp: new Date().toISOString() 
      }), { qos: 0, retain: false });
    }
  }, 30000);

  mqttPublisher.on('close', () => {
    clearInterval(heartbeatInterval);
  });
}

// Configuration des topics de diffusion
const RESERVATIONS_RECENTES_TOPIC = 'ktur/reservations/recentes';
const STATUS_TOPIC = 'ktur/chauffeurs/status';
const POSITION_TOPIC = 'ktur/chauffeurs/position';

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
    listener_connected: mqttClient ? mqttClient.connected : false,
    publisher_connected: mqttPublisher ? mqttPublisher.connected : false,
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
    const parts = topic.split('/');
    if (parts.length >= 3) {
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

// Nouveaux endpoints pour la gestion des statuts
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

app.get('/api/chauffeurs/:id/status', async (req, res) => {
  try {
    const { id } = req.params;
    const statut = await redis.hgetall(`chauffeur:${id}`);
    
    if (!statut || Object.keys(statut).length === 0) {
      return res.status(404).json({ error: 'Chauffeur non trouvé' });
    }
    
    res.json({
      id,
      ...statut,
      disponible: statut.disponible === '1',
      en_ligne: statut.en_ligne === '1',
      en_course: statut.en_course === '1'
    });
  } catch (error) {
    logger.error('Erreur récupération statut chauffeur:', error);
    res.status(500).json({ error: 'Erreur serveur' });
  }
});

// Capteur de messages MQTT
if (mqttClient) {
  mqttClient.on('message', async (topic, message) => {
    const [ , chauffeurId, channel ] = topic.split('/');
    let data;

    try {
      data = JSON.parse(message.toString());
    } catch (err) {
      return logger.error('Payload invalide', { error: err.message });
    }

    try {
      switch (data.type) {
        case 'position':
          await handlePosition(chauffeurId, data.data);
          break;
        
        case 'acceptation':
          await notifyLaravel('/reservation/acceptation', {
            chauffeur_id: chauffeurId,
            resa_id: data.data.resa_id,
          });
          await updateStatut(chauffeurId, { 
            en_ligne: true,
            en_course: true,
            disponible: false
          });
          break;

        case 'debut':
          await notifyLaravel('/reservation/debut', { 
            resa_id: data.data.resa_id 
          });
          await updateStatut(chauffeurId, { en_course: true });
          break;

        case 'fin':
          await notifyLaravel('/reservation/fin', { 
            resa_id: data.data.resa_id 
          });
          await updateStatut(chauffeurId, { 
            en_course: false,
            disponible: true
          });
          break;

        default:
          logger.warn('Type non géré', { type: data.type });
      }
    } catch (err) {
      logger.error('Erreur traitement', { 
        error: err.message,
        stack: err.stack 
      });
    }
  });
}

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
  const mapping = {};
  Object.entries(fields).forEach(([k, v]) => mapping[k] = v ? '1' : '0');
  mapping.updated_at = Date.now();
  
  await redis.hset(key, mapping);
  console.log(`🔄 Statut mis à jour pour ${chauffeurId}:`, fields);
  
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
        topic: `${STATUS_TOPIC}/${chauffeurId}`,
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

async function publishChauffeurPosition(chauffeurId, lat, lng) {
  try {
    if (!MQTT_ENABLED || !mqttPublisher) {
      logger.warn('MQTT non disponible, publication ignorée');
      return;
    }
    
    const positionData = {
      chauffeur_id: chauffeurId,
      latitude: lat,
      longitude: lng,
      timestamp: new Date().toISOString()
    };
    
    const message = {
      topic: `${POSITION_TOPIC}/${chauffeurId}`,
      payload: JSON.stringify(positionData),
      options: { qos: 1 },
      type: 'position'
    };
    
    if (!mqttPublisher.connected) {
      if (pendingMessages.length < MAX_PENDING_MESSAGES) {
        pendingMessages.push(message);
        logger.warn(`Publisher MQTT non connecté, position de ${chauffeurId} mise en file d'attente`);
      } else {
        logger.warn('File d\'attente pleine, message ignoré');
      }
      return;
    }
    
    mqttPublisher.publish(message.topic, message.payload, message.options);
    logger.info(`📍 Position publiée pour chauffeur ${chauffeurId}`);
  } catch (error) {
    logger.error('Erreur publication position MQTT:', error);
  }
}

async function handlePosition(id, { lat, lng }) {
  const key = `chauffeur:${id}`;
  const statut = await redis.hgetall(key);

  const enLigne   = true;
  const enCourse  = statut.en_course === '1';
  const disponible= enLigne && !enCourse;

  await redis.hset(key, {
    latitude:   lat,
    longitude:  lng,
    en_ligne:   '1',
    disponible: disponible ? '1' : '0',
    updated_at: Date.now()
  });
  console.log(`📍 Position de ${id} enregistrée. Disponible=${disponible}`);
  
  await publishChauffeurPosition(id, lat, lng);
  await publishChauffeurStatus(id);
}

app.listen(PORT, () => {
  console.log(`🚀 Serveur ecouteur MQTT en écoute sur le port ${PORT}`);
  console.log(`📡 Topics de diffusion:`);
  console.log(`   - ${STATUS_TOPIC}/[chauffeur_id] : Statut individuel`);
  console.log(`   - ${POSITION_TOPIC}/[chauffeur_id] : Position individuelle`);
  console.log(`   - ${RESERVATIONS_RECENTES_TOPIC} : Nouvelles réservations`);
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