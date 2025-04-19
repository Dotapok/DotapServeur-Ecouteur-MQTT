require('dotenv').config();
const express = require('express');
const mqtt = require('mqtt');
const Redis = require('ioredis');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

const PORT = process.env.PORT || 3000;
console.log('URL Redis:', process.env.REDIS_URL);
console.log('URL MQTT:', process.env.MQTT_BROKER_URL);

// Connexion Redis
const redis = new Redis(process.env.REDIS_URL);

redis.on('connect', () => {
  console.log('✅ Connecté à Redis');
});

redis.on('error', (err) => {
  console.error('❌ Erreur de connexion à Redis:', err);
});

// Connexion MQTT
const mqttClient = mqtt.connect(process.env.MQTT_BROKER_URL, {
  username: process.env.MQTT_USERNAME,
  password: process.env.MQTT_PASSWORD,
});

mqttClient.on('connect', () => {
  console.log('✅ Connecté à EMQX MQTT');
});

mqttClient.on('error', (err) => {
  console.error('❌ Erreur de connexion à EMQX MQTT:', err);
});

// Store des topics écoutés
const subscribedTopics = new Set();

// Écouter un topic via l'API avec QoS 1 pour une meilleure fiabilité
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

// Quand un message arrive → stocker dans Redis (on pourrait envisager QoS 2 ici si nécessaire)
mqttClient.on('message', async (topic, message) => {
  try {
    const payload = message.toString();
    console.log(`📩 Message reçu: ${payload}`);

    const data = JSON.parse(payload);
    const chauffeurId = topic.split('/')[1];

    // Récupération du statut actuel du chauffeur
    const statut = await redis.hgetall(`chauffeur:${chauffeurId}`);

    // Conversions robustes des états
    let enLigne = String(statut.en_ligne) === '1';
    let enCourse = String(statut.en_course) === '1';
    let disponible = 0;

    if (enLigne && !enCourse) {
      disponible = 1;
    }

    // Mise à jour de la position dans le zset GEO
    await redis.geoadd('chauffeurs_positions', data.lng, data.lat, chauffeurId);

    // Mise à jour des infos dans le hash
    await redis.hset(`chauffeur:${chauffeurId}`,
      'updated_at', Date.now(),
      'disponible', disponible,
      'en_ligne', enLigne ? '1' : '0',
      'en_course', enCourse ? '1' : '0'
    );

    console.log(`✅ Position de ${chauffeurId} mise à jour. Disponible = ${disponible}, en_ligne = ${enLigne}, en_course = ${enCourse}`);
  } catch (e) {
    console.error('❌ Erreur de parsing MQTT:', e);
  }
});

// Endpoint pour se désabonner d'un topic MQTT
app.post('/api/desabonner-topic', (req, res) => {
  const topic = req.body.topic;

  if (!topic || !subscribedTopics.has(topic)) {
    return res.status(200).json({ message: 'Le topic n\'est pas en écoute' });
  }

  mqttClient.unsubscribe(topic, { qos: 0 }, (err) => {
    if (!err) {
      subscribedTopics.delete(topic);
      console.log(`❌ Désabonnement du topic: ${topic}`);
      res.status(200).json({ message: `Désabonnement du topic ${topic} effectué.` });
    } else {
      res.status(500).json({ message: 'Erreur lors du désabonnement du topic' });
    }
  });
});

app.listen(PORT, () => {
  console.log(`🚀 Serveur ecouteur MQTT en écoute sur le port ${PORT}`);
});
