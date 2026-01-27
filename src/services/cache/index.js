const redisService = require('../redis');
const logger = require('../../utils/logger');

class CacheService {
  constructor() {
    this.DEFAULT_TTL = parseInt(process.env.CACHE_TTL_SECONDS || '3600', 10); // 1 heure par défaut
    this.POSITION_CACHE_TTL = parseInt(process.env.POSITION_CACHE_TTL_SECONDS || '300', 10); // 5 minutes pour les positions
    this.STATUS_CACHE_TTL = parseInt(process.env.STATUS_CACHE_TTL_SECONDS || '600', 10); // 10 minutes pour les statuts
  }

  // Cache pour les positions des chauffeurs (remplace lastPositionCache)
  async setDriverPosition(driverId, positionData) {
    const key = `cache:position:${driverId}`;
    const value = JSON.stringify({
      lat: positionData.lat,
      lng: positionData.lng,
      ts: positionData.ts || Date.now()
    });

    try {
      await redisService.setWithTTL(key, value, this.POSITION_CACHE_TTL);
      
      if (process.env.LOG_LEVEL === 'debug') {
        logger.debug('Position mise en cache', { driverId, key, ttl: this.POSITION_CACHE_TTL });
      }
      
      return true;
    } catch (err) {
      logger.error('Erreur mise en cache position', { driverId, error: err.message });
      return false;
    }
  }

  async getDriverPosition(driverId) {
    const key = `cache:position:${driverId}`;
    
    try {
      const cached = await redisService.get(key);
      if (!cached) return null;
      
      return JSON.parse(cached);
    } catch (err) {
      logger.error('Erreur récupération cache position', { driverId, error: err.message });
      return null;
    }
  }

  async hasPositionChanged(driverId, newPosition) {
    const previous = await this.getDriverPosition(driverId);
    
    if (!previous) return true;
    
    return previous.lat !== newPosition.lat || previous.lng !== newPosition.lng;
  }

  // Cache pour les timestamps de publication de statut (remplace lastStatusPublishTs)
  async setStatusPublishTimestamp(driverId, timestamp = Date.now()) {
    const key = `cache:status_publish:${driverId}`;
    
    try {
      await redisService.setWithTTL(key, timestamp.toString(), this.STATUS_CACHE_TTL);
      return true;
    } catch (err) {
      logger.error('Erreur mise en cache timestamp statut', { driverId, error: err.message });
      return false;
    }
  }

  async getStatusPublishTimestamp(driverId) {
    const key = `cache:status_publish:${driverId}`;
    
    try {
      const timestamp = await redisService.get(key);
      return timestamp ? parseInt(timestamp, 10) : null;
    } catch (err) {
      logger.error('Erreur récupération cache timestamp statut', { driverId, error: err.message });
      return null;
    }
  }

  async shouldPublishStatus(driverId, throttleMs = 5000) {
    const lastPublish = await this.getStatusPublishTimestamp(driverId);
    
    if (!lastPublish) return true;
    
    const now = Date.now();
    return (now - lastPublish) >= throttleMs;
  }

  // Cache générique avec TTL
  async set(key, value, ttlSeconds = null) {
    const cacheKey = `cache:${key}`;
    const ttl = ttlSeconds || this.DEFAULT_TTL;
    
    try {
      const serialized = typeof value === 'object' ? JSON.stringify(value) : value.toString();
      await redisService.setWithTTL(cacheKey, serialized, ttl);
      return true;
    } catch (err) {
      logger.error('Erreur mise en cache générique', { key: cacheKey, error: err.message });
      return false;
    }
  }

  async get(key, parseJson = true) {
    const cacheKey = `cache:${key}`;
    
    try {
      const cached = await redisService.get(cacheKey);
      if (!cached) return null;
      
      return parseJson ? JSON.parse(cached) : cached;
    } catch (err) {
      logger.error('Erreur récupération cache générique', { key: cacheKey, error: err.message });
      return null;
    }
  }

  async delete(key) {
    const cacheKey = `cache:${key}`;
    
    try {
      await redisService.client.del(cacheKey);
      return true;
    } catch (err) {
      logger.error('Erreur suppression cache', { key: cacheKey, error: err.message });
      return false;
    }
  }

  // Nettoyage du cache par pattern
  async clearPattern(pattern) {
    try {
      const keys = await redisService.scanKeys(`cache:${pattern}`);
      
      if (keys.length > 0) {
        await redisService.client.del(...keys);
        logger.info('Cache nettoyé', { pattern, keysDeleted: keys.length });
      }
      
      return keys.length;
    } catch (err) {
      logger.error('Erreur nettoyage cache par pattern', { pattern, error: err.message });
      return 0;
    }
  }

  // Statistiques du cache
  async getStats() {
    try {
      const positionKeys = await redisService.scanKeys('cache:position:*');
      const statusKeys = await redisService.scanKeys('cache:status_publish:*');
      const genericKeys = await redisService.scanKeys('cache:*');
      
      return {
        total: genericKeys.length,
        positions: positionKeys.length,
        statusTimestamps: statusKeys.length,
        memoryUsage: await this.getMemoryUsage()
      };
    } catch (err) {
      logger.error('Erreur récupération statistiques cache', { error: err.message });
      return null;
    }
  }

  async getMemoryUsage() {
    try {
      const info = await redisService.client.info('memory');
      const usedMemory = info.match(/used_memory:([0-9]+)/);
      return usedMemory ? parseInt(usedMemory[1], 10) : 0;
    } catch (err) {
      return 0;
    }
  }

  // Health check
  async healthCheck() {
    try {
      await redisService.client.ping();
      return { status: 'healthy', service: 'cache' };
    } catch (err) {
      return { status: 'unhealthy', service: 'cache', error: err.message };
    }
  }
}

// Export singleton instance
module.exports = new CacheService();