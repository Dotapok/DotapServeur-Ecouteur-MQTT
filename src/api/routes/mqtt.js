const express = require('express');
const router = express.Router();

// Import des services
const mqttService = require('../../services/mqtt');
const config = require('../../config');

/**
 * @route GET /mqtt/status
 * @description Statut du service MQTT
 * @returns {Object} Statut de connexion MQTT
 */
router.get('/status', (req, res) => {
  res.json({
    service: 'mqtt',
    connected: mqttService.isConnected(),
    status: mqttService.isConnected() ? 'connected' : 'disconnected',
    clients: mqttService.clients.size,
    timestamp: Date.now()
  });
});

/**
 * @route POST /mqtt/reconnect-publisher
 * @description Forcer la reconnexion du publisher MQTT
 * @returns {Object} Résultat de la reconnexion
 */
router.post('/reconnect-publisher', async (req, res) => {
  try {
    if (!mqttService.isConnected()) {
      return res.status(503).json({
        status: 'error',
        message: 'Service MQTT non connecté',
        timestamp: Date.now()
      });
    }

    const result = await mqttService.reconnectPublisher();
    
    res.json({
      status: 'success',
      message: 'Reconnexion publisher MQTT initiée',
      result,
      timestamp: Date.now()
    });
    
  } catch (error) {
    res.status(500).json({
      status: 'error',
      message: 'Erreur lors de la reconnexion',
      error: error.message,
      timestamp: Date.now()
    });
  }
});

/**
 * @route POST /mqtt/ecouter-topic
 * @description S'abonner à un topic MQTT
 * @param {Object} req.body - Données de la requête
 * @param {string} req.body.topic - Topic MQTT à écouter
 * @param {number} [req.body.qos=0] - Qualité de service (0, 1, 2)
 * @returns {Object} Confirmation de l'abonnement
 */
router.post('/ecouter-topic', async (req, res) => {
  try {
    const { topic, qos = 0 } = req.body;
    
    if (!topic) {
      return res.status(400).json({
        status: 'error',
        message: 'Topic MQTT requis',
        timestamp: Date.now()
      });
    }

    if (!mqttService.isConnected()) {
      return res.status(503).json({
        status: 'error',
        message: 'Service MQTT non connecté',
        timestamp: Date.now()
      });
    }

    const success = mqttService.subscribe(topic, parseInt(qos));
    
    if (success) {
      res.json({
        status: 'success',
        message: `Abonné au topic: ${topic}`,
        topic,
        qos: parseInt(qos),
        timestamp: Date.now()
      });
    } else {
      res.status(500).json({
        status: 'error',
        message: 'Erreur lors de l\'abonnement',
        topic,
        timestamp: Date.now()
      });
    }
    
  } catch (error) {
    res.status(500).json({
      status: 'error',
      message: 'Erreur lors de l\'abonnement au topic',
      error: error.message,
      timestamp: Date.now()
    });
  }
});

/**
 * @route POST /mqtt/desabonner-topic
 * @description Se désabonner d'un topic MQTT
 * @param {Object} req.body - Données de la requête
 * @param {string} req.body.topic - Topic MQTT à désabonner
 * @returns {Object} Confirmation du désabonnement
 */
router.post('/desabonner-topic', async (req, res) => {
  try {
    const { topic } = req.body;
    
    if (!topic) {
      return res.status(400).json({
        status: 'error',
        message: 'Topic MQTT requis',
        timestamp: Date.now()
      });
    }

    if (!mqttService.isConnected()) {
      return res.status(503).json({
        status: 'error',
        message: 'Service MQTT non connecté',
        timestamp: Date.now()
      });
    }

    const success = mqttService.unsubscribe(topic);
    
    if (success) {
      res.json({
        status: 'success',
        message: `Désabonné du topic: ${topic}`,
        topic,
        timestamp: Date.now()
      });
    } else {
      res.status(500).json({
        status: 'error',
        message: 'Erreur lors du désabonnement',
        topic,
        timestamp: Date.now()
      });
    }
    
  } catch (error) {
    res.status(500).json({
      status: 'error',
      message: 'Erreur lors du désabonnement du topic',
      error: error.message,
      timestamp: Date.now()
    });
  }
});

/**
 * @route GET /mqtt/topics
 * @description Liste des topics MQTT actuellement écoutés
 * @returns {Object} Liste des topics et leurs QoS
 */
router.get('/topics', (req, res) => {
  try {
    const topics = mqttService.getSubscribedTopics();
    
    res.json({
      service: 'mqtt',
      topics: Object.keys(topics).map(topic => ({
        topic,
        qos: topics[topic]
      })),
      count: Object.keys(topics).length,
      timestamp: Date.now()
    });
    
  } catch (error) {
    res.status(500).json({
      status: 'error',
      message: 'Erreur lors de la récupération des topics',
      error: error.message,
      timestamp: Date.now()
    });
  }
});

module.exports = router;