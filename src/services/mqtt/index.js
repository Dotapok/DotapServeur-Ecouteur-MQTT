const mqtt = require('mqtt');
const logger = require('../../utils/logger');

class MQTTService {
  constructor() {
    this.clients = new Map();
    this.pendingMessages = [];
    this.MAX_PENDING_MESSAGES = parseInt(process.env.MAX_PENDING_MESSAGES || '500', 10);
    this.MQTT_BROKER_URL = process.env.MQTT_BROKER_URL || 'mqtts://test.mosquitto.org:8883';
  }

  // Créer un client MQTT avec configuration
  createClient(role, options = {}) {
    const clientId = `ktur_${role}_${Math.random().toString(16).slice(2, 8)}`;
    
    const defaultOptions = {
      username: '',
      password: '',
      reconnectPeriod: 5000,
      connectTimeout: 30000,
      keepalive: 60,
      clean: true,
      clientId,
      rejectUnauthorized: false,
      protocolVersion: 4,
      protocolId: 'MQTT',
      ...options
    };

    logger.info('Création client MQTT', { role, clientId });
    const client = mqtt.connect(this.MQTT_BROKER_URL, defaultOptions);
    
    this.clients.set(role, client);
    return client;
  }

  // Initialiser le service MQTT
  initialize() {
    if (process.env.MQTT_ENABLED === 'false') {
      logger.warn('MQTT désactivé');
      return;
    }

    // Client listener
    const listener = this.createClient('listener');
    this.setupListenerClient(listener);

    // Client publisher si activé
    if (process.env.MQTT_PUBLISHER_ENABLED !== 'false') {
      const publisher = this.createClient('publisher', {
        will: {
          topic: 'ktur/server/status',
          payload: JSON.stringify({ status: 'offline', timestamp: Date.now() }),
          qos: 1,
          retain: true
        }
      });
      this.setupPublisherClient(publisher);
    }
  }

  // Configuration du client listener
  setupListenerClient(client) {
    client.on('connect', () => {
      logger.info('MQTT Listener connecté');
      
      const topics = [
        'ktur/reservations/recentes',
        'chauffeur/+/status',
        'chauffeur/+/position',
        'passager_mobile/status',
        'passager_mobile/position'
      ];

      client.subscribe(topics, { qos: 1 }, (err) => {
        if (err) {
          logger.error('Erreur abonnement topics principaux:', err.message);
        } else {
          logger.info('Abonnements MQTT effectués', { topics });
        }
      });

      this.processPendingMessages();
    });

    client.on('error', err => logger.error('MQTT Listener erreur:', err.message));
    client.on('close', () => logger.info('MQTT Listener fermé'));
    client.on('offline', () => logger.warn('MQTT Listener hors ligne'));
  }

  // Configuration du client publisher
  setupPublisherClient(client) {
    client.on('connect', () => {
      logger.info('MQTT Publisher connecté');
      this.publish('ktur/server/status', 
        JSON.stringify({ status: 'online', timestamp: Date.now() }),
        { qos: 1, retain: true }
      );
      this.processPendingMessages();
    });

    client.on('error', err => logger.error('MQTT Publisher erreur:', err.message));
    client.on('close', () => logger.info('MQTT Publisher fermé'));
    client.on('offline', () => logger.warn('MQTT Publisher hors ligne'));
  }

  // Publier un message (avec file d'attente si nécessaire)
  publish(topic, payload, options = { qos: 1, retain: false }) {
    const publisher = this.clients.get('publisher');
    
    if (!publisher || !publisher.connected) {
      this.enqueuePendingMessage(topic, payload, options);
      return false;
    }

    try {
      publisher.publish(topic, payload, options);
      return true;
    } catch (err) {
      logger.error('Erreur publication MQTT', { topic, error: err.message });
      this.enqueuePendingMessage(topic, payload, options);
      return false;
    }
  }

  // Mettre en file d'attente
  enqueuePendingMessage(topic, payload, options) {
    if (this.pendingMessages.length >= this.MAX_PENDING_MESSAGES) {
      const removed = this.pendingMessages.shift();
      logger.warn('File d\'attente MQTT pleine, suppression du plus ancien', { removedTopic: removed.topic });
    }

    this.pendingMessages.push({ topic, payload, options });
    
    if (process.env.LOG_LEVEL === 'debug') {
      logger.debug('Message mis en file d\'attente', { topic, queueSize: this.pendingMessages.length });
    }
  }

  // Traiter les messages en attente
  processPendingMessages() {
    const publisher = this.clients.get('publisher');
    if (!publisher || !publisher.connected) return;

    logger.info(`Traitement de ${this.pendingMessages.length} messages en attente`);

    while (this.pendingMessages.length > 0) {
      const message = this.pendingMessages.shift();
      try {
        publisher.publish(message.topic, message.payload, message.options);
      } catch (err) {
        logger.error('Erreur publication message différé', { topic: message.topic, error: err.message });
        this.pendingMessages.unshift(message);
        break;
      }
    }
  }

  // Fermer tous les clients
  async shutdown() {
    logger.info('Fermeture des clients MQTT');
    
    for (const [role, client] of this.clients) {
      try {
        client.end();
        logger.info(`Client MQTT ${role} fermé`);
      } catch (err) {
        logger.error(`Erreur fermeture client ${role}`, { error: err.message });
      }
    }
    
    this.clients.clear();
  }

  // Vérifier si un client est connecté
  isConnected(role = 'publisher') {
    const client = this.clients.get(role);
    return client && client.connected;
  }

  // S'abonner à un topic
  subscribe(topic, qos = 0) {
    const listener = this.clients.get('listener');
    if (!listener || !listener.connected) {
      logger.error('Impossible de s\'abonner: listener non connecté');
      return false;
    }

    try {
      listener.subscribe(topic, { qos }, (err) => {
        if (err) {
          logger.error('Erreur abonnement topic', { topic, error: err.message });
        } else {
          logger.info('Abonné au topic', { topic, qos });
        }
      });
      return true;
    } catch (err) {
      logger.error('Erreur abonnement topic', { topic, error: err.message });
      return false;
    }
  }

  // Se désabonner d'un topic
  unsubscribe(topic) {
    const listener = this.clients.get('listener');
    if (!listener || !listener.connected) {
      logger.error('Impossible de se désabonner: listener non connecté');
      return false;
    }

    try {
      listener.unsubscribe(topic, (err) => {
        if (err) {
          logger.error('Erreur désabonnement topic', { topic, error: err.message });
        } else {
          logger.info('Désabonné du topic', { topic });
        }
      });
      return true;
    } catch (err) {
      logger.error('Erreur désabonnement topic', { topic, error: err.message });
      return false;
    }
  }

  // Obtenir la liste des topics abonnés
  getSubscribedTopics() {
    const listener = this.clients.get('listener');
    if (!listener) {
      return {};
    }
    
    // Note: Cette implémentation est simplifiée car mqtt.js ne fournit pas
    // une méthode native pour obtenir les abonnements actuels
    // On retourne les topics principaux configurés
    return {
      'ktur/reservations/recentes': 1,
      'chauffeur/+/status': 1,
      'chauffeur/+/position': 1,
      'passager_mobile/status': 1,
      'passager_mobile/position': 1
    };
  }

  // Forcer la reconnexion du publisher
  async reconnectPublisher() {
    const publisher = this.clients.get('publisher');
    if (!publisher) {
      logger.error('Publisher non trouvé pour reconnexion');
      return false;
    }

    try {
      logger.info('Forçage reconnexion publisher MQTT');
      publisher.end();
      
      // Recréer le client publisher
      const newPublisher = this.createClient('publisher', {
        will: {
          topic: 'ktur/server/status',
          payload: JSON.stringify({ status: 'offline', timestamp: Date.now() }),
          qos: 1,
          retain: true
        }
      });
      
      this.setupPublisherClient(newPublisher);
      this.clients.set('publisher', newPublisher);
      
      logger.info('Reconnexion publisher MQTT initiée');
      return true;
      
    } catch (err) {
      logger.error('Erreur reconnexion publisher', { error: err.message });
      return false;
    }
  }
}

module.exports = new MQTTService();