const Redis = require('ioredis');
const logger = require('../../utils/logger');

class RedisService {
  constructor() {
    this.client = null;
    this.isConnected = false;
    this.connectionRetries = 0;
    this.MAX_RETRIES = 5;
    this.RETRY_DELAY = 2000;
  }

  // Initialiser la connexion Redis
  async initialize() {
    try {
      this.client = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');
      
      this.client.on('connect', () => {
        this.isConnected = true;
        this.connectionRetries = 0;
        logger.info('Connecté à Redis');
      });

      this.client.on('error', (err) => {
        this.isConnected = false;
        logger.error('Erreur Redis:', err.message);
        
        // Retry stratégique avec exponential backoff
        if (this.connectionRetries < this.MAX_RETRIES) {
          this.connectionRetries++;
          const delay = this.RETRY_DELAY * Math.pow(2, this.connectionRetries - 1);
          logger.warn(`Tentative de reconnexion ${this.connectionRetries}/${this.MAX_RETRIES} dans ${delay}ms`);
          
          setTimeout(() => {
            this.initialize().catch(() => {});
          }, delay);
        }
      });

      this.client.on('close', () => {
        this.isConnected = false;
        logger.warn('Connexion Redis fermée');
      });

      this.client.on('reconnecting', () => {
        logger.info('Reconnexion à Redis...');
      });

      await this.client.ping();
      return this.client;

    } catch (err) {
      logger.error('Erreur initialisation Redis:', err.message);
      throw err;
    }
  }

  // Vérifier si Redis est connecté
  isReady() {
    return this.isConnected && this.client && this.client.status === 'ready';
  }

  // Méthodes de base avec gestion d'erreurs
  async get(key) {
    if (!this.isReady()) {
      throw new Error('Redis non connecté');
    }

    try {
      return await this.client.get(key);
    } catch (err) {
      logger.error('Erreur Redis get:', err.message);
      throw err;
    }
  }

  async set(key, value, ttlSeconds = null) {
    if (!this.isReady()) {
      throw new Error('Redis non connecté');
    }

    try {
      if (ttlSeconds) {
        return await this.client.set(key, value, 'EX', ttlSeconds);
      } else {
        return await this.client.set(key, value);
      }
    } catch (err) {
      logger.error('Erreur Redis set:', err.message);
      throw err;
    }
  }

  async hset(key, field, value) {
    if (!this.isReady()) {
      throw new Error('Redis non connecté');
    }

    try {
      return await this.client.hset(key, field, value);
    } catch (err) {
      logger.error('Erreur Redis hset:', err.message);
      throw err;
    }
  }

  async hgetall(key) {
    if (!this.isReady()) {
      throw new Error('Redis non connecté');
    }

    try {
      return await this.client.hgetall(key);
    } catch (err) {
      logger.error('Erreur Redis hgetall:', err.message);
      throw err;
    }
  }

  async hmset(key, data) {
    if (!this.isReady()) {
      throw new Error('Redis non connecté');
    }

    try {
      return await this.client.hmset(key, data);
    } catch (err) {
      logger.error('Erreur Redis hmset:', err.message);
      throw err;
    }
  }

  // Méthodes avancées avec TTL pour le cache
  async setWithTTL(key, value, ttlSeconds = 3600) {
    return this.set(key, value, ttlSeconds);
  }

  async hsetWithTTL(key, field, value, ttlSeconds = 3600) {
    await this.hset(key, field, value);
    await this.client.expire(key, ttlSeconds);
  }

  async hmsetWithTTL(key, data, ttlSeconds = 3600) {
    await this.hmset(key, data);
    await this.client.expire(key, ttlSeconds);
  }

  // Gestion des clés avec pattern matching
  async scanKeys(pattern) {
    if (!this.isReady()) {
      throw new Error('Redis non connecté');
    }

    try {
      const keys = [];
      let cursor = '0';
      
      do {
        const [nextCursor, foundKeys] = await this.client.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
        cursor = nextCursor;
        if (foundKeys?.length) keys.push(...foundKeys);
      } while (cursor !== '0');
      
      return keys;
    } catch (err) {
      logger.error('Erreur Redis scan:', err.message);
      throw err;
    }
  }

  // Récupérer toutes les clés correspondant à un pattern
  async keys(pattern) {
    if (!this.isReady()) {
      throw new Error('Redis non connecté');
    }

    try {
      return await this.client.keys(pattern);
    } catch (err) {
      logger.error('Erreur Redis keys:', err.message);
      throw err;
    }
  }

  // List operations for chat history
  async lpush(key, value) {
    if (!this.isReady()) {
      throw new Error('Redis non connecté');
    }
    try {
      return await this.client.lpush(key, value);
    } catch (err) {
      logger.error('Erreur Redis lpush:', err.message);
      throw err;
    }
  }

  async ltrim(key, start, stop) {
    if (!this.isReady()) {
      throw new Error('Redis non connecté');
    }
    try {
      return await this.client.ltrim(key, start, stop);
    } catch (err) {
      logger.error('Erreur Redis ltrim:', err.message);
      throw err;
    }
  }

  async lrange(key, start, stop) {
    if (!this.isReady()) {
      throw new Error('Redis non connecté');
    }
    try {
      return await this.client.lrange(key, start, stop);
    } catch (err) {
      logger.error('Erreur Redis lrange:', err.message);
      throw err;
    }
  }

  // Pipeline pour les opérations batch
  async pipeline(operations) {
    if (!this.isReady()) {
      throw new Error('Redis non connecté');
    }

    try {
      const pipeline = this.client.multi();
      
      operations.forEach(([operation, ...args]) => {
        pipeline[operation](...args);
      });
      
      return await pipeline.exec();
    } catch (err) {
      logger.error('Erreur Redis pipeline:', err.message);
      throw err;
    }
  }

  // Redis Streams pour la file d'attente persistante
  async xadd(streamKey, message, maxLength = 1000) {
    if (!this.isReady()) {
      throw new Error('Redis non connecté');
    }

    try {
      return await this.client.xadd(streamKey, 'MAXLEN', '~', maxLength, '*', 'message', JSON.stringify(message));
    } catch (err) {
      logger.error('Erreur Redis xadd:', err.message);
      throw err;
    }
  }

  async xread(streamKey, count = 10, block = 5000) {
    if (!this.isReady()) {
      throw new Error('Redis non connecté');
    }

    try {
      return await this.client.xread('BLOCK', block, 'COUNT', count, 'STREAMS', streamKey, '0');
    } catch (err) {
      logger.error('Erreur Redis xread:', err.message);
      throw err;
    }
  }

  async xack(streamKey, group, ...ids) {
    if (!this.isReady()) {
      throw new Error('Redis non connecté');
    }

    try {
      return await this.client.xack(streamKey, group, ...ids);
    } catch (err) {
      logger.error('Erreur Redis xack:', err.message);
      throw err;
    }
  }

  // Health check pour l'endpoint /health
  async healthCheck() {
    try {
      if (!this.isReady()) {
        return { status: 'down', message: 'Redis non connecté' };
      }
      
      // Tester la connexion avec un ping
      const startTime = Date.now();
      await this.client.ping();
      const responseTime = Date.now() - startTime;
      
      return {
        status: 'up',
        responseTime: `${responseTime}ms`,
        connection: this.client.status,
        retries: this.connectionRetries
      };
      
    } catch (err) {
      return {
        status: 'down',
        message: `Erreur health check: ${err.message}`,
        error: err.message
      };
    }
  }

  // Fermer la connexion
  async shutdown() {
    if (this.client) {
      try {
        await this.client.quit();
        logger.info('Connexion Redis fermée proprement');
      } catch (err) {
        logger.error('Erreur fermeture Redis:', err.message);
      }
      this.client = null;
      this.isConnected = false;
    }
  }

  // Circuit breaker simple
  async withCircuitBreaker(operation, fallback = null, maxRetries = 3) {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await operation();
      } catch (err) {
        if (attempt === maxRetries) {
          logger.warn(`Circuit breaker déclenché après ${maxRetries} tentatives`);
          return fallback;
        }
        
        const delay = Math.pow(2, attempt - 1) * 1000;
        logger.warn(`Tentative ${attempt}/${maxRetries} échouée, réessai dans ${delay}ms`);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
  }
}

// Export singleton instance
module.exports = new RedisService();
