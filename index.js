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

// Initialisation MQTT
const mqttClient = mqtt.connect(process.env.MQTT_BROKER_URL, {
  username: process.env.MQTT_USERNAME,
  password: process.env.MQTT_PASSWORD,
});
mqttClient.on('connect', () => console.log('✅ Connecté à MQTT'));
mqttClient.on('error', err => console.error('❌ Erreur de connexion à MQTT:', err));

// Store des topics écoutés
const subscribedTopics = new Set();

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
  if (!topic || !subscribed.has(topic)) {
    return res.status(200).json({ message: 'Topic non abonné' });
  }
  mqttClient.unsubscribe(topic, {}, err => {
    if (err) {
      console.error('Erreur désabonnement:', err);
      return res.status(500).json({ message: 'Erreur désabonnement MQTT' });
    }
    subscribed.delete(topic);
    console.log(`❌ Désabonné de ${topic}`);
    res.json({ message: `Désabonnement de ${topic} réussi` });
  });
});

// Capteur de messages MQTT
mqttClient.on('message', async (topic, message) => {
  let data;
  try {
    data = JSON.parse(message.toString());
  } catch (err) {
    return console.error('❌ Payload non JSON:', err);
  }

  const [ , chauffeurId, event ] = topic.split('/');

  try {
    switch (event) {
      case 'position':
        await handlePosition(chauffeurId, data);
        break;
      case 'acceptation':
        await notifyLaravel('/reservation/acceptation', {
          chauffeur_id: chauffeurId,
          resa_id: data.resa_id,
        });
        break;
      case 'debutCourse':
        await notifyLaravel('/reservation/debut', { resa_id: data.resa_id });
        await updateStatut(chauffeurId, { en_course: true });
        break;
      case 'finCourse':
        await notifyLaravel('/reservation/fin', { resa_id: data.resa_id });
        await updateStatut(chauffeurId, { en_course: false });
        break;
      default:
        console.log(`🔍 Topic non géré: ${topic}`);
    }
  } 
  catch (err) {
    console.error('❌ Erreur gestion message:', err);
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
 * Mise à jour du statut chauffeur dans Redis
 */
async function updateStatut(chauffeurId, fields) {
  const key = `chauffeur:${chauffeurId}`;
  const mapping = {};
  Object.entries(fields).forEach(([k, v]) => mapping[k] = v ? '1' : '0');
  mapping.updated_at = Date.now();
  await redis.hset(key, mapping);
  console.log(`🔄 Statut mis à jour pour ${chauffeurId}:`, fields);
}

/**
 * Gestion de la position GPS
 */
async function handlePosition(chauffeurId, { lat, lng }) {
  const key = `chauffeur:${chauffeurId}`;
  const statut = await redis.hgetall(key);
  const enLigne = statut.en_ligne === '1';
  const enCourse = statut.en_course === '1';
  const disponible = enLigne && !enCourse ? '1' : '0';
  await redis.hset(key, {
    latitude: lat,
    longitude: lng,
    disponible,
    updated_at: Date.now(),
  });
  console.log(`📍 Position de ${chauffeurId} enregistrée. Disponible=${disponible}`);
}

app.listen(PORT, () => {
  console.log(`🚀 Serveur ecouteur MQTT en écoute sur le port ${PORT}`);
});