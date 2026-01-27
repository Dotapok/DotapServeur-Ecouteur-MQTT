const express = require('express');
const router = express.Router();

// Import des services
const redisService = require('../../services/redis');
const mqttService = require('../../services/mqtt');
const cacheService = require('../../services/cache');
const queueService = require('../../services/queue');

/**
 * @route GET /health
 * @description Endpoint de santé du système
 * @returns {Object} Statut de tous les services
 */
router.get('/', async (req, res) => {
  try {
    const health = {
      status: 'ok',
      timestamp: Date.now(),
      services: {
        redis: await redisService.healthCheck(),
        mqtt: { status: mqttService.isConnected() ? 'healthy' : 'unhealthy' },
        cache: await cacheService.healthCheck(),
        queue: await queueService.healthCheck()
      },
      memory: process.memoryUsage(),
      uptime: process.uptime()
    };
    
    res.json(health);
  } catch (error) {
    res.status(500).json({ 
      status: 'error', 
      error: error.message,
      timestamp: Date.now()
    });
  }
});

/**
 * @route GET /health/redis
 * @description Health check spécifique à Redis
 * @returns {Object} Statut Redis détaillé
 */
router.get('/redis', async (req, res) => {
  try {
    const redisHealth = await redisService.healthCheck();
    res.json({
      service: 'redis',
      ...redisHealth,
      timestamp: Date.now()
    });
  } catch (error) {
    res.status(503).json({
      service: 'redis',
      status: 'down',
      error: error.message,
      timestamp: Date.now()
    });
  }
});

/**
 * @route GET /health/mqtt
 * @description Health check spécifique à MQTT
 * @returns {Object} Statut MQTT
 */
router.get('/mqtt', (req, res) => {
  res.json({
    service: 'mqtt',
    status: mqttService.isConnected() ? 'connected' : 'disconnected',
    connected: mqttService.isConnected(),
    timestamp: Date.now()
  });
});

module.exports = router;