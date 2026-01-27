const express = require('express');
const router = express.Router();

// Import des services
const redisService = require('../../services/redis');
const mqttService = require('../../services/mqtt');
const config = require('../../config');

/**
 * @route GET /chauffeurs/status
 * @description Statut de tous les chauffeurs
 * @returns {Object} Liste des chauffeurs avec leur statut
 */
router.get('/status', async (req, res) => {
  try {
    // Rechercher toutes les clés de chauffeurs (utilisation de scan pour éviter le blocage)
    const chauffeurKeys = await redisService.scanKeys('chauffeur:*');
    
    const chauffeurs = [];
    
    // Récupérer les données de chaque chauffeur
    for (const key of chauffeurKeys) {
      const chauffeurData = await redisService.hgetall(key);
      if (chauffeurData && Object.keys(chauffeurData).length > 0) {
        const chauffeurId = key.replace('chauffeur:', '');
        chauffeurs.push({
          id: chauffeurId,
          ...chauffeurData,
          // Convertir les valeurs booléennes
          en_ligne: chauffeurData.en_ligne === '1',
          disponible: chauffeurData.disponible === '1',
          en_course: chauffeurData.en_course === '1'
        });
      }
    }
    
    res.json({
      count: chauffeurs.length,
      chauffeurs,
      timestamp: Date.now()
    });
    
  } catch (error) {
    res.status(500).json({
      status: 'error',
      message: 'Erreur lors de la récupération des statuts chauffeurs',
      error: error.message,
      timestamp: Date.now()
    });
  }
});

/**
 * @route POST /chauffeurs/:id/status
 * @description Mettre à jour le statut d'un chauffeur
 * @param {string} id - ID du chauffeur
 * @param {Object} req.body - Données de mise à jour
 * @param {boolean} [req.body.en_ligne] - Statut en ligne
 * @param {boolean} [req.body.disponible] - Statut disponible
 * @param {boolean} [req.body.en_course] - Statut en course
 * @returns {Object} Confirmation de la mise à jour
 */
router.post('/:id/status', async (req, res) => {
  try {
    const chauffeurId = req.params.id;
    const { en_ligne, disponible, en_course } = req.body;
    
    if (!chauffeurId) {
      return res.status(400).json({
        status: 'error',
        message: 'ID chauffeur requis',
        timestamp: Date.now()
      });
    }
    
    const key = config.getChauffeurKey(chauffeurId);
    const now = Date.now();
    
    const updates = {
      updated_at: now
    };
    
    // Mettre à jour seulement les champs fournis
    if (en_ligne !== undefined) updates.en_ligne = en_ligne ? '1' : '0';
    if (disponible !== undefined) updates.disponible = disponible ? '1' : '0';
    if (en_course !== undefined) updates.en_course = en_course ? '1' : '0';
    
    // Récupérer les données actuelles pour construire le hash complet
    const currentData = await redisService.hgetall(key);
    const completeData = {
      ...currentData,
      ...updates
    };
    
    // Mettre à jour Redis
    await redisService.hmset(key, completeData);
    
    // Publier la mise à jour MQTT
    await mqttService.publish(
      config.getChauffeurStatusTopic(chauffeurId),
      JSON.stringify({
        chauffeur_id: chauffeurId,
        ...updates,
        timestamp: now,
        source: 'api'
      }),
      { qos: 1, retain: true }
    );
    
    res.json({
      status: 'success',
      message: 'Statut chauffeur mis à jour',
      chauffeur_id: chauffeurId,
      updates,
      timestamp: now
    });
    
  } catch (error) {
    res.status(500).json({
      status: 'error',
      message: 'Erreur lors de la mise à jour du statut chauffeur',
      error: error.message,
      timestamp: Date.now()
    });
  }
});

/**
 * @route POST /chauffeurs/:id/subscribe-status
 * @description S'abonner aux mises à jour de statut d'un chauffeur
 * @param {string} id - ID du chauffeur
 * @returns {Object} Confirmation de l'abonnement
 */
router.post('/:id/subscribe-status', async (req, res) => {
  try {
    const chauffeurId = req.params.id;
    
    if (!chauffeurId) {
      return res.status(400).json({
        status: 'error',
        message: 'ID chauffeur requis',
        timestamp: Date.now()
      });
    }
    
    const topic = config.getChauffeurStatusTopic(chauffeurId);
    
    // S'abonner au topic MQTT
    const success = mqttService.subscribe(topic, 1);
    
    if (success) {
      res.json({
        status: 'success',
        message: `Abonné aux mises à jour de statut du chauffeur ${chauffeurId}`,
        chauffeur_id: chauffeurId,
        topic,
        timestamp: Date.now()
      });
    } else {
      res.status(500).json({
        status: 'error',
        message: 'Erreur lors de l\'abonnement au topic MQTT',
        chauffeur_id: chauffeurId,
        timestamp: Date.now()
      });
    }
    
  } catch (error) {
    res.status(500).json({
      status: 'error',
      message: 'Erreur lors de l\'abonnement aux mises à jour de statut',
      error: error.message,
      timestamp: Date.now()
    });
  }
});

/**
 * @route GET /chauffeurs/:id
 * @description Récupérer les données d'un chauffeur spécifique
 * @param {string} id - ID du chauffeur
 * @returns {Object} Données du chauffeur
 */
router.get('/:id', async (req, res) => {
  try {
    const chauffeurId = req.params.id;
    
    if (!chauffeurId) {
      return res.status(400).json({
        status: 'error',
        message: 'ID chauffeur requis',
        timestamp: Date.now()
      });
    }
    
    const key = config.getChauffeurKey(chauffeurId);
    const chauffeurData = await redisService.hgetall(key);
    
    if (!chauffeurData || Object.keys(chauffeurData).length === 0) {
      return res.status(404).json({
        status: 'error',
        message: 'Chauffeur non trouvé',
        chauffeur_id: chauffeurId,
        timestamp: Date.now()
      });
    }
    
    // Convertir les valeurs booléennes
    const formattedData = {
      id: chauffeurId,
      ...chauffeurData,
      en_ligne: chauffeurData.en_ligne === '1',
      disponible: chauffeurData.disponible === '1',
      en_course: chauffeurData.en_course === '1'
    };
    
    res.json({
      status: 'success',
      chauffeur: formattedData,
      timestamp: Date.now()
    });
    
  } catch (error) {
    res.status(500).json({
      status: 'error',
      message: 'Erreur lors de la récupération des données chauffeur',
      error: error.message,
      timestamp: Date.now()
    });
  }
});

module.exports = router;