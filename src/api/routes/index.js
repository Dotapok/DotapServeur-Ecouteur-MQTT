const express = require('express');
const router = express.Router();

// Import des routes
const healthRoutes = require('./health');
const statsRoutes = require('./stats');
const mqttRoutes = require('./mqtt');
const chauffeursRoutes = require('./chauffeurs');
const reservationsRoutes = require('./reservations');
const chatRoutes = require('./chat');

// Montage des routes
router.use('/health', healthRoutes);
router.use('/stats', statsRoutes);
router.use('/mqtt', mqttRoutes);
router.use('/chauffeurs', chauffeursRoutes);
router.use('/reservations', reservationsRoutes);
router.use('/chat', chatRoutes);

// Route racine de l'API
router.get('/', (req, res) => {
  res.json({
    message: 'API Server MQTT - KTUR',
    version: '1.0.0',
    timestamp: Date.now(),
    endpoints: {
      health: '/health',
      health_redis: '/health/redis',
      health_mqtt: '/health/mqtt',
      stats: '/stats',
      stats_cache: '/stats/cache',
      stats_queue: '/stats/queue',
      stats_mqtt: '/stats/mqtt',
      mqtt_status: '/mqtt/status',
      mqtt_reconnect: '/mqtt/reconnect-publisher',
      mqtt_subscribe: '/mqtt/ecouter-topic',
      mqtt_unsubscribe: '/mqtt/desabonner-topic',
      mqtt_topics: '/mqtt/topics',
      chauffeurs_status: '/chauffeurs/status',
      chauffeur_update: '/chauffeurs/:id/status',
      chauffeur_subscribe: '/chauffeurs/:id/subscribe-status',
      chauffeur_details: '/chauffeurs/:id',
      reservation_subscribe: '/reservations/subscribe',
      reservation_send_message: '/reservations/send-message',
      reservation_position: '/reservations/:id/position',
      reservation_details: '/reservations/:id',
      chat_history: '/chat/history/:reservationId',
      chat_send_message: '/chat/message',
      chat_sos_history: '/chat/sos/history/:sosId'
    }
  });
});

module.exports = router;
