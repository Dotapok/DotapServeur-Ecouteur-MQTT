const express = require('express');
const router = express.Router();

// Import des services
const redisService = require('../../services/redis');
const mqttService = require('../../services/mqtt');
const config = require('../../config');

/**
 * @route POST /reservations/subscribe
 * @description S'abonner aux mises à jour de réservation
 * @param {Object} req.body - Données de la requête
 * @param {string} req.body.reservation_id - ID de la réservation
 * @returns {Object} Confirmation de l'abonnement
 */
router.post('/subscribe', async (req, res) => {
  try {
    const { reservation_id } = req.body;
    
    if (!reservation_id) {
      return res.status(400).json({
        status: 'error',
        message: 'ID réservation requis',
        timestamp: Date.now()
      });
    }
    
    const topic = config.getReservationTopic(reservation_id);
    
    // S'abonner au topic MQTT
    const success = mqttService.subscribe(topic, 1);
    
    if (success) {
      res.json({
        status: 'success',
        message: `Abonné aux mises à jour de la réservation ${reservation_id}`,
        reservation_id,
        topic,
        timestamp: Date.now()
      });
    } else {
      res.status(500).json({
        status: 'error',
        message: 'Erreur lors de l\'abonnement au topic MQTT',
        reservation_id,
        timestamp: Date.now()
      });
    }
    
  } catch (error) {
    res.status(500).json({
      status: 'error',
      message: 'Erreur lors de l\'abonnement aux mises à jour de réservation',
      error: error.message,
      timestamp: Date.now()
    });
  }
});

/**
 * @route POST /reservations/send-message
 * @description Envoyer un message dans une réservation
 * @param {Object} req.body - Données de la requête
 * @param {string} req.body.reservation_id - ID de la réservation
 * @param {string} req.body.message - Message à envoyer
 * @param {string} [req.body.sender] - Expéditeur du message
 * @returns {Object} Confirmation de l'envoi
 */
router.post('/send-message', async (req, res) => {
  try {
    const { reservation_id, message, sender = 'system' } = req.body;
    
    if (!reservation_id || !message) {
      return res.status(400).json({
        status: 'error',
        message: 'ID réservation et message requis',
        timestamp: Date.now()
      });
    }
    
    const messageData = {
      reservation_id,
      message,
      sender,
      timestamp: Date.now(),
      type: 'chat_message'
    };
    
    // Publier le message MQTT
    const topic = config.getReservationChatTopic(reservation_id);
    const success = mqttService.publish(
      topic,
      JSON.stringify(messageData),
      { qos: 1 }
    );
    
    if (success) {
      res.json({
        status: 'success',
        message: 'Message envoyé',
        reservation_id,
        topic,
        timestamp: Date.now()
      });
    } else {
      res.status(500).json({
        status: 'error',
        message: 'Erreur lors de l\'envoi du message',
        reservation_id,
        timestamp: Date.now()
      });
    }
    
  } catch (error) {
    res.status(500).json({
      status: 'error',
      message: 'Erreur lors de l\'envoi du message',
      error: error.message,
      timestamp: Date.now()
    });
  }
});

/**
 * @route GET /reservations/:id/position
 * @description Récupérer la position d'une réservation
 * @param {string} id - ID de la réservation
 * @returns {Object} Position de la réservation
 */
router.get('/:id/position', async (req, res) => {
  try {
    const reservationId = req.params.id;
    
    if (!reservationId) {
      return res.status(400).json({
        status: 'error',
        message: 'ID réservation requis',
        timestamp: Date.now()
      });
    }
    
    const key = config.getReservationPositionKey(reservationId);
    const positionData = await redisService.hgetall(key);
    
    if (!positionData || Object.keys(positionData).length === 0) {
      return res.status(404).json({
        status: 'error',
        message: 'Position non trouvée pour cette réservation',
        reservation_id: reservationId,
        timestamp: Date.now()
      });
    }
    
    res.json({
      status: 'success',
      reservation_id: reservationId,
      position: {
        latitude: parseFloat(positionData.latitude || 0),
        longitude: parseFloat(positionData.longitude || 0),
        accuracy: parseFloat(positionData.accuracy || 0),
        speed: parseFloat(positionData.speed || 0),
        heading: parseFloat(positionData.heading || 0),
        updated_at: parseInt(positionData.updated_at || Date.now())
      },
      timestamp: Date.now()
    });
    
  } catch (error) {
    res.status(500).json({
      status: 'error',
      message: 'Erreur lors de la récupération de la position',
      error: error.message,
      timestamp: Date.now()
    });
  }
});

/**
 * @route GET /reservations/:id
 * @description Récupérer les détails d'une réservation
 * @param {string} id - ID de la réservation
 * @returns {Object} Détails de la réservation
 */
router.get('/:id', async (req, res) => {
  try {
    const reservationId = req.params.id;
    
    if (!reservationId) {
      return res.status(400).json({
        status: 'error',
        message: 'ID réservation requis',
        timestamp: Date.now()
      });
    }
    
    const key = config.getReservationKey(reservationId);
    const reservationData = await redisService.hgetall(key);
    
    if (!reservationData || Object.keys(reservationData).length === 0) {
      return res.status(404).json({
        status: 'error',
        message: 'Réservation non trouvée',
        reservation_id: reservationId,
        timestamp: Date.now()
      });
    }
    
    res.json({
      status: 'success',
      reservation: {
        id: reservationId,
        ...reservationData,
        // Convertir les valeurs numériques
        created_at: parseInt(reservationData.created_at || Date.now()),
        updated_at: parseInt(reservationData.updated_at || Date.now())
      },
      timestamp: Date.now()
    });
    
  } catch (error) {
    res.status(500).json({
      status: 'error',
      message: 'Erreur lors de la récupération des détails de réservation',
      error: error.message,
      timestamp: Date.now()
    });
  }
});

module.exports = router;