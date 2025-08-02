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

// Initialisation Redis
const redis = new Redis(process.env.REDIS_URL);
redis.on('connect', () => console.log('✅ Connecté à Redis'));
redis.on('error', err => console.error('❌ Erreur Redis:', err));

// Initialisation MQTT Client (pour écouter les chauffeurs)
const mqttClient = mqtt.connect(process.env.MQTT_BROKER_URL, {
  username: process.env.MQTT_USERNAME,
  password: process.env.MQTT_PASSWORD,
});
mqttClient.on('connect', () => console.log('✅ Connecté à MQTT'));
mqttClient.on('error', err => console.error('❌ Erreur de connexion à MQTT:', err));

// Initialisation MQTT Publisher (pour propager les statuts vers les clients)
const mqttPublisher = mqtt.connect(process.env.MQTT_BROKER_URL, {
  username: process.env.MQTT_USERNAME,
  password: process.env.MQTT_PASSWORD,
  clientId: 'ktur_status_publisher'
});
mqttPublisher.on('connect', () => console.log('✅ Publisher MQTT connecté'));
mqttPublisher.on('error', err => console.error('❌ Erreur Publisher MQTT:', err));

// Store des topics écoutés
const subscribedTopics = new Set();

// Configuration des topics de diffusion
const STATUS_TOPIC = 'ktur/chauffeurs/status';
const POSITION_TOPIC = 'ktur/chauffeurs/position';

// Endpoint Écouter un topic via l'API avec QoS 1 pour une meilleure fiabilité
app.post('/api/ecouter-topic', (req, res) => {
  const topic = req.body.topic;

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

// Endpoint pour se désabonner d'un topic MQTT
app.post('/api/desabonner-topic', (req, res) => {
  const { topic } = req.body;
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
    // Extraction du chauffeurId depuis le topic
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

// Fonctions utilitaires

/** Envoi POST à Laravel avec token unique */
async function notifyLaravel(endpoint, token, payload) {
  try {
    await axios.post(`${process.env.LARAVEL_API_URL}${endpoint}`, payload, {
      headers: { Authorization: `Bearer ${token}` }
    });
    logger.info(`✅ Notification envoyée vers ${endpoint}`);
  } catch (err) {
    logger.error(`❌ Erreur appel Laravel ${endpoint}:`, err.response ? err.response.data : err.message);
  }
}

/**
 * Mise à jour du statut chauffeur dans Redis et publication MQTT
 */
async function updateStatut(chauffeurId, fields) {
  const key = `chauffeur:${chauffeurId}`;
  const mapping = {};
  Object.entries(fields).forEach(([k, v]) => mapping[k] = v ? '1' : '0');
  mapping.updated_at = Date.now();
  
  await redis.hset(key, mapping);
  console.log(`🔄 Statut mis à jour pour ${chauffeurId}:`, fields);
  
  // Publication du statut mis à jour via MQTT
  await publishChauffeurStatus(chauffeurId);
}

/**
 * Publication du statut d'un chauffeur via MQTT
 */
async function publishChauffeurStatus(chauffeurId) {
  try {
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
      
      // Publication sur le topic spécifique du chauffeur
      mqttPublisher.publish(`${STATUS_TOPIC}/${chauffeurId}`, JSON.stringify(statusData), { qos: 1 });
      

      
      logger.info(`📡 Statut publié pour chauffeur ${chauffeurId}`);
    }
  } catch (error) {
    logger.error('Erreur publication statut MQTT:', error);
  }
}

/**
 * Publication de la position d'un chauffeur via MQTT
 */
async function publishChauffeurPosition(chauffeurId, lat, lng) {
  try {
    const positionData = {
      chauffeur_id: chauffeurId,
      latitude: lat,
      longitude: lng,
      timestamp: new Date().toISOString()
    };
    
    // Publication sur le topic de position
    mqttPublisher.publish(`${POSITION_TOPIC}/${chauffeurId}`, JSON.stringify(positionData), { qos: 1 });
    

    
    logger.info(`📍 Position publiée pour chauffeur ${chauffeurId}`);
  } catch (error) {
    logger.error('Erreur publication position MQTT:', error);
  }
}

/**
 * Gestion de la position GPS
 */
async function handlePosition(id, { lat, lng }) {
  const key = `chauffeur:${id}`;
  const statut = await redis.hgetall(key);

  // Supposons qu'on considère désormais le chauffeur comme en ligne
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
  
  // Publication de la position via MQTT
  await publishChauffeurPosition(id, lat, lng);
}



app.listen(PORT, () => {
  console.log(`🚀 Serveur ecouteur MQTT en écoute sur le port ${PORT}`);
  console.log(`📡 Topics de diffusion:`);
  console.log(`   - ${STATUS_TOPIC}/[chauffeur_id] : Statut individuel`);
  console.log(`   - ${POSITION_TOPIC}/[chauffeur_id] : Position individuelle`);
});