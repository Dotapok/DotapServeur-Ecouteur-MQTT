const express = require('express');
const router = express.Router();

// Import des services
const mqttService = require('../../services/mqtt');
const cacheService = require('../../services/cache');
const queueService = require('../../services/queue');

/**
 * @route GET /stats
 * @description Statistiques du système
 * @returns {Object} Statistiques des services
 */
router.get('/', async (req, res) => {
  try {
    const stats = {
      cache: await cacheService.getStats(),
      queue: await queueService.getQueueStats(),
      mqtt: {
        connected: mqttService.isConnected(),
        clients: mqttService.clients.size
      },
      timestamp: Date.now()
    };
    
    res.json(stats);
  } catch (error) {
    res.status(500).json({ 
      status: 'error', 
      error: error.message,
      timestamp: Date.now()
    });
  }
});

/**
 * @route GET /stats/cache
 * @description Statistiques du cache
 * @returns {Object} Statistiques détaillées du cache
 */
router.get('/cache', async (req, res) => {
  try {
    const cacheStats = await cacheService.getStats();
    res.json({
      service: 'cache',
      ...cacheStats,
      timestamp: Date.now()
    });
  } catch (error) {
    res.status(500).json({
      service: 'cache',
      status: 'error',
      error: error.message,
      timestamp: Date.now()
    });
  }
});

/**
 * @route GET /stats/queue
 * @description Statistiques de la queue
 * @returns {Object} Statistiques détaillées de la queue
 */
router.get('/queue', async (req, res) => {
  try {
    const queueStats = await queueService.getQueueStats();
    res.json({
      service: 'queue',
      ...queueStats,
      timestamp: Date.now()
    });
  } catch (error) {
    res.status(500).json({
      service: 'queue',
      status: 'error',
      error: error.message,
      timestamp: Date.now()
    });
  }
});

/**
 * @route GET /stats/mqtt
 * @description Statistiques MQTT
 * @returns {Object} Statistiques détaillées MQTT
 */
router.get('/mqtt', (req, res) => {
  res.json({
    service: 'mqtt',
    connected: mqttService.isConnected(),
    clients: mqttService.clients.size,
    timestamp: Date.now()
  });
});

module.exports = router;