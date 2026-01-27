require('dotenv').config();

class Config {
  constructor() {
    this.loadEnvironmentVariables();
    this.validateRequiredConfig();
  }

  loadEnvironmentVariables() {
    // Configuration serveur
    this.PORT = parseInt(process.env.PORT || '3000', 10);
    this.NODE_ENV = process.env.NODE_ENV || 'development';
    this.IS_PRODUCTION = this.NODE_ENV === 'production';
    this.IS_DEVELOPMENT = this.NODE_ENV === 'development';

    // Configuration Redis
    this.REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
    this.REDIS_MAX_RETRIES = parseInt(process.env.REDIS_MAX_RETRIES || '5', 10);
    this.REDIS_RETRY_DELAY = parseInt(process.env.REDIS_RETRY_DELAY_MS || '2000', 10);

    // Configuration MQTT
    this.MQTT_BROKER_URL = process.env.MQTT_BROKER_URL || 'mqtts://test.mosquitto.org:8883';
    this.MQTT_ENABLED = process.env.MQTT_ENABLED !== 'false';
    this.MQTT_PUBLISHER_ENABLED = process.env.MQTT_PUBLISHER_ENABLED !== 'false';
    this.MQTT_KEEPALIVE = parseInt(process.env.MQTT_KEEPALIVE || '60', 10);
    this.MQTT_RECONNECT_PERIOD = parseInt(process.env.MQTT_RECONNECT_PERIOD_MS || '5000', 10);

    // Configuration cache
    this.CACHE_TTL_SECONDS = parseInt(process.env.CACHE_TTL_SECONDS || '3600', 10); // 1 heure
    this.POSITION_CACHE_TTL = parseInt(process.env.POSITION_CACHE_TTL_SECONDS || '300', 10); // 5 minutes
    this.STATUS_CACHE_TTL = parseInt(process.env.STATUS_CACHE_TTL_SECONDS || '600', 10); // 10 minutes

    // Configuration queue
    this.QUEUE_MAX_LENGTH = parseInt(process.env.QUEUE_MAX_LENGTH || '10000', 10);
    this.QUEUE_BATCH_SIZE = parseInt(process.env.QUEUE_BATCH_SIZE || '100', 10);
    this.QUEUE_BLOCK_TIMEOUT_MS = parseInt(process.env.QUEUE_BLOCK_TIMEOUT_MS || '5000', 10);

    // Configuration application
    this.LOG_LEVEL = process.env.LOG_LEVEL || 'info';
    this.IS_DEBUG = this.LOG_LEVEL.toLowerCase() === 'debug';
    this.MAX_PENDING_MESSAGES = parseInt(process.env.MAX_PENDING_MESSAGES || '500', 10);
    this.POSITION_THROTTLE_MS = parseInt(process.env.POSITION_THROTTLE_MS || '250', 10);
    this.INACTIVITY_THRESHOLD_MS = parseInt(process.env.INACTIVITY_THRESHOLD_MS || (5 * 60 * 1000), 10);

    // Configuration Laravel API
    this.LARAVEL_BASE_URL = process.env.LARAVEL_BASE_URL || 'http://localhost:8000';
    this.LARAVEL_API_TIMEOUT = parseInt(process.env.LARAVEL_API_TIMEOUT_MS || '5000', 10);

    // Topics MQTT
    this.MQTT_TOPICS = {
      RESERVATIONS_RECENTES: 'ktur/reservations/recentes',
      RESERVATION_PREFIX: 'ktur/reservations/',
      STATUS_WILDCARD: 'chauffeur/+/status',
      POSITION_WILDCARD: 'chauffeur/+/position',
      PASSAGER_STATUS: 'passager_mobile/status',
      PASSAGER_POSITION: 'passager_mobile/position',
      SERVER_STATUS: 'ktur/server/status'
    };

    // Redis keys patterns
    this.REDIS_KEYS = {
      CHAUFFEUR_PREFIX: 'chauffeur:',
      CACHE_POSITION_PREFIX: 'cache:position:',
      CACHE_STATUS_PREFIX: 'cache:status_publish:',
      STREAM_MQTT: 'ktur:mqtt:messages'
    };
  }

  validateRequiredConfig() {
    const required = [
      'REDIS_URL',
      'MQTT_BROKER_URL'
    ];

    const missing = [];
    for (const key of required) {
      if (!process.env[key]) {
        missing.push(key);
      }
    }

    if (missing.length > 0 && this.IS_PRODUCTION) {
      throw new Error(`Configuration requise manquante: ${missing.join(', ')}`);
    }

    if (missing.length > 0) {
      console.warn(`⚠️  Configuration manquante (utilisation des valeurs par défaut): ${missing.join(', ')}`);
    }
  }

  // Méthodes utilitaires
  getChauffeurKey(chauffeurId) {
    return `${this.REDIS_KEYS.CHAUFFEUR_PREFIX}${chauffeurId}`;
  }

  getPositionCacheKey(chauffeurId) {
    return `${this.REDIS_KEYS.CACHE_POSITION_PREFIX}${chauffeurId}`;
  }

  getStatusCacheKey(chauffeurId) {
    return `${this.REDIS_KEYS.CACHE_STATUS_PREFIX}${chauffeurId}`;
  }

  getReservationTopic(reservationId) {
    return `${this.MQTT_TOPICS.RESERVATION_PREFIX}${reservationId}`;
  }

  getReservationChatTopic(reservationId) {
    return `${this.MQTT_TOPICS.RESERVATION_PREFIX}${reservationId}/chat`;
  }

  getReservationPositionTopic(reservationId) {
    return `${this.MQTT_TOPICS.RESERVATION_PREFIX}${reservationId}/position`;
  }

  getReservationKey(reservationId) {
    return `reservation:${reservationId}`;
  }

  getReservationPositionKey(reservationId) {
    return `reservation:${reservationId}:position`;
  }

  getChauffeurStatusTopic(chauffeurId) {
    return `chauffeur/${chauffeurId}/status`;
  }

  getChauffeurPositionTopic(chauffeurId) {
    return `chauffeur/${chauffeurId}/position`;
  }

  // Vérification de configuration
  isMQTTEnabled() {
    return this.MQTT_ENABLED;
  }

  isMQTTPublisherEnabled() {
    return this.MQTT_PUBLISHER_ENABLED;
  }

  // Export de la configuration pour les tests
  toJSON() {
    return {
      PORT: this.PORT,
      NODE_ENV: this.NODE_ENV,
      REDIS_URL: this.REDIS_URL,
      MQTT_BROKER_URL: this.MQTT_BROKER_URL,
      MQTT_ENABLED: this.MQTT_ENABLED,
      LOG_LEVEL: this.LOG_LEVEL,
      IS_DEBUG: this.IS_DEBUG,
      IS_PRODUCTION: this.IS_PRODUCTION,
      IS_DEVELOPMENT: this.IS_DEVELOPMENT
    };
  }
}

// Export singleton instance
module.exports = new Config();