const redisService = require('../redis');
const logger = require('../../utils/logger');

class QueueService {
  constructor() {
    this.STREAM_KEY = 'ktur:mqtt:messages';
    this.CONSUMER_GROUP = 'mqtt_publishers';
    this.CONSUMER_NAME = `consumer_${process.pid}`;
    this.MAX_STREAM_LENGTH = parseInt(process.env.QUEUE_MAX_LENGTH || '10000', 10);
    this.BLOCK_TIMEOUT = parseInt(process.env.QUEUE_BLOCK_TIMEOUT_MS || '5000', 10);
    this.PROCESSING_BATCH_SIZE = parseInt(process.env.QUEUE_BATCH_SIZE || '100', 10);
  }

  // Initialiser le stream et le consumer group
  async initialize() {
    try {
      // Créer le consumer group s'il n'existe pas
      await redisService.client.xgroup('CREATE', this.STREAM_KEY, this.CONSUMER_GROUP, '0', 'MKSTREAM');
      logger.info('Consumer group Redis Stream initialisé', { 
        stream: this.STREAM_KEY, 
        group: this.CONSUMER_GROUP 
      });
    } catch (err) {
      if (err.message.includes('BUSYGROUP')) {
        logger.info('Consumer group existe déjà');
      } else {
        logger.error('Erreur initialisation Redis Stream', { error: err.message });
        throw err;
      }
    }
  }

  // Ajouter un message à la queue (remplace pendingMessages.push)
  async enqueue(topic, payload, options = { qos: 1, retain: false }) {
    const message = {
      topic,
      payload: typeof payload === 'string' ? payload : JSON.stringify(payload),
      options: JSON.stringify(options),
      timestamp: Date.now(),
      attempts: 0
    };

    try {
      const messageId = await redisService.xadd(
        this.STREAM_KEY, 
        message, 
        this.MAX_STREAM_LENGTH
      );

      if (process.env.LOG_LEVEL === 'debug') {
        logger.debug('Message ajouté à Redis Stream', { 
          topic, 
          messageId,
          streamLength: await this.getStreamLength() 
        });
      }

      return messageId;
    } catch (err) {
      logger.error('Erreur ajout message à Redis Stream', { topic, error: err.message });
      throw err;
    }
  }

  // Lire les messages en attente de traitement
  async readPendingMessages(count = this.PROCESSING_BATCH_SIZE) {
    try {
      const messages = await redisService.client.xreadgroup(
        'GROUP', this.CONSUMER_GROUP, this.CONSUMER_NAME,
        'COUNT', count,
        'BLOCK', this.BLOCK_TIMEOUT,
        'STREAMS', this.STREAM_KEY, '>'
      );

      if (!messages || messages.length === 0) {
        return [];
      }

      const parsedMessages = [];
      for (const [stream, streamMessages] of messages) {
        for (const [messageId, messageData] of streamMessages) {
          try {
            const message = this.parseStreamMessage(messageId, messageData);
            parsedMessages.push(message);
          } catch (parseErr) {
            logger.error('Erreur parsing message stream', { messageId, error: parseErr.message });
            await this.acknowledge(messageId);
          }
        }
      }

      return parsedMessages;
    } catch (err) {
      if (err.message.includes('NOGROUP')) {
        await this.initialize();
        return [];
      }
      logger.error('Erreur lecture Redis Stream', { error: err.message });
      return [];
    }
  }

  // Parser les messages du stream
  parseStreamMessage(messageId, messageData) {
    const message = { id: messageId };
    
    for (let i = 0; i < messageData.length; i += 2) {
      const key = messageData[i];
      const value = messageData[i + 1];
      
      if (key === 'payload' || key === 'options') {
        try {
          message[key] = JSON.parse(value);
        } catch {
          message[key] = value;
        }
      } else if (key === 'timestamp' || key === 'attempts') {
        message[key] = parseInt(value, 10);
      } else {
        message[key] = value;
      }
    }

    return message;
  }

  // Accuser réception d'un message traité
  async acknowledge(messageId) {
    try {
      await redisService.xack(this.STREAM_KEY, this.CONSUMER_GROUP, messageId);
      
      if (process.env.LOG_LEVEL === 'debug') {
        logger.debug('Message acknowledge', { messageId });
      }
      
      return true;
    } catch (err) {
      logger.error('Erreur acknowledge message', { messageId, error: err.message });
      return false;
    }
  }

  // Marquer un message comme échoué (pour retry)
  async markAsFailed(messageId, maxAttempts = 5) {
    try {
      const message = await redisService.client.xrange(this.STREAM_KEY, messageId, messageId);
      if (!message || message.length === 0) {
        return false;
      }

      const currentAttempts = this.parseStreamMessage(messageId, message[0][1]).attempts || 0;
      
      if (currentAttempts >= maxAttempts) {
        // Trop d'échecs, on supprime le message
        await redisService.client.xdel(this.STREAM_KEY, messageId);
        logger.warn('Message supprimé après trop d\'échecs', { messageId, attempts: currentAttempts });
        return false;
      }

      // Incrémenter le compteur d'essais et réinsérer
      await redisService.client.xadd(
        this.STREAM_KEY,
        '*',
        ...Object.entries({
          ...this.parseStreamMessage(messageId, message[0][1]),
          attempts: currentAttempts + 1,
          lastFailure: Date.now()
        }).flat()
      );

      // Supprimer l'ancien message
      await redisService.client.xdel(this.STREAM_KEY, messageId);

      logger.warn('Message réinséré pour retry', { messageId, attempts: currentAttempts + 1 });
      return true;

    } catch (err) {
      logger.error('Erreur marquage message échoué', { messageId, error: err.message });
      return false;
    }
  }

  // Obtenir les statistiques de la queue
  async getQueueStats() {
    try {
      const info = await redisService.client.xinfo('STREAM', this.STREAM_KEY);
      const pending = await redisService.client.xpending(this.STREAM_KEY, this.CONSUMER_GROUP);
      
      return {
        length: info.length,
        pending: pending.count,
        consumers: info.groups?.[0]?.consumers || 0,
        firstId: info.firstEntry?.[0] || null,
        lastId: info.lastEntry?.[0] || null
      };
    } catch (err) {
      logger.error('Erreur récupération stats queue', { error: err.message });
      return null;
    }
  }

  // Obtenir la longueur du stream
  async getStreamLength() {
    try {
      const info = await redisService.client.xinfo('STREAM', this.STREAM_KEY);
      return info.length || 0;
    } catch (err) {
      return 0;
    }
  }

  // Nettoyer les vieux messages
  async cleanupOldMessages(maxAgeHours = 24) {
    try {
      const maxAgeMs = maxAgeHours * 60 * 60 * 1000;
      const cutoffTime = Date.now() - maxAgeMs;
      
      const messages = await redisService.client.xrange(this.STREAM_KEY, '-', '+');
      let deleted = 0;
      
      for (const [messageId, messageData] of messages) {
        const message = this.parseStreamMessage(messageId, messageData);
        if (message.timestamp && message.timestamp < cutoffTime) {
          await redisService.client.xdel(this.STREAM_KEY, messageId);
          deleted++;
        }
      }
      
      if (deleted > 0) {
        logger.info('Messages anciens nettoyés', { deleted, maxAgeHours });
      }
      
      return deleted;
    } catch (err) {
      logger.error('Erreur nettoyage messages anciens', { error: err.message });
      return 0;
    }
  }

  // Health check de la queue
  async healthCheck() {
    try {
      await redisService.client.xlen(this.STREAM_KEY);
      return { status: 'healthy', service: 'queue', stream: this.STREAM_KEY };
    } catch (err) {
      return { status: 'unhealthy', service: 'queue', error: err.message };
    }
  }
}

// Export singleton instance
module.exports = new QueueService();