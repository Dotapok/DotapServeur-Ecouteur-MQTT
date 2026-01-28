const express = require('express');
const router = express.Router();

// Import des services
const redisService = require('../../services/redis');

/**
 * @route GET /chat/history/:reservationId
 * @description Récupérer l'historique du chat d'une réservation
 * @param {string} reservationId - ID de la réservation
 * @returns {Object} Historique des messages du chat
 */
router.get('/history/:reservationId', async (req, res) => {
  try {
    const reservationId = req.params.reservationId;
    
    if (!reservationId) {
      return res.status(400).json({
        status: 'error',
        message: 'ID réservation requis',
        timestamp: Date.now()
      });
    }
    
    const key = `chat:history:${reservationId}`;
    
    // Récupérer l'historique depuis Redis
    // Note: Cette implémentation est simplifiée, l'historique réel serait stocké différemment
    const chatHistory = await redisService.lrange(key, 0, -1);
    
    const messages = chatHistory.map(msg => {
      try {
        return JSON.parse(msg);
      } catch {
        return { message: msg, timestamp: Date.now() };
      }
    });
    
    res.json({
      status: 'success',
      reservation_id: reservationId,
      messages,
      count: messages.length,
      timestamp: Date.now()
    });
    
  } catch (error) {
    res.status(500).json({
      status: 'error',
      message: 'Erreur lors de la récupération de l\'historique du chat',
      error: error.message,
      timestamp: Date.now()
    });
  }
});

/**
 * @route POST /chat/message
 * @description Envoyer un message dans le chat
 * @param {Object} req.body - Données de la requête
 * @param {string} req.body.reservation_id - ID de la réservation
 * @param {string} req.body.message - Message à envoyer
 * @param {string} [req.body.sender] - Expéditeur du message
 * @param {string} [req.body.type] - Type de message
 * @returns {Object} Confirmation de l'envoi
 */
router.post('/message', async (req, res) => {
  try {
    const { reservation_id, message, sender = 'system', type = 'chat_message' } = req.body;
    
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
      type,
      timestamp: Date.now()
    };
    
    const key = `chat:history:${reservation_id}`;
    
    // Stocker le message dans Redis (liste)
    await redisService.lpush(key, JSON.stringify(messageData));
    
    // Limiter l'historique à 1000 messages
    await redisService.ltrim(key, 0, 999);
    
    res.json({
      status: 'success',
      message: 'Message envoyé',
      reservation_id,
      message_data: messageData,
      timestamp: Date.now()
    });
    
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
 * @route GET /chat/sos/history/:sosId
 * @description Récupérer l'historique du chat SOS (utilisateur ↔ admin)
 * @param {string} sosId - ID du SOS
 * @returns {Object} Historique des messages du chat SOS
 */
router.get('/sos/history/:sosId', async (req, res) => {
  try {
    const sosId = req.params.sosId;
    
    if (!sosId) {
      return res.status(400).json({
        status: 'error',
        message: 'ID SOS requis',
        timestamp: Date.now()
      });
    }
    
    const key = `chat:sos_history:${sosId}`;
    const chatHistory = await redisService.lrange(key, 0, -1);
    
    const messages = chatHistory.map(msg => {
      try {
        return JSON.parse(msg);
      } catch {
        return { message: msg, timestamp: Date.now() };
      }
    });
    
    res.json({
      status: 'success',
      sos_id: sosId,
      messages,
      count: messages.length,
      timestamp: Date.now()
    });
    
  } catch (error) {
    res.status(500).json({
      status: 'error',
      message: 'Erreur lors de la récupération de l\'historique du chat SOS',
      error: error.message,
      timestamp: Date.now()
    });
  }
});

module.exports = router;
