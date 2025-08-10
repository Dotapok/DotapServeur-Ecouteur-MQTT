# 🚀 Serveur MQTT KTUR Driver - Serveur d'Écoute

## 📋 Description

Serveur Node.js robuste pour la gestion des communications MQTT dans l'écosystème KTUR Driver. Gère les positions des chauffeurs, les réservations, le chat en temps réel et la synchronisation des statuts.

## ✨ Fonctionnalités Principales

### 🔄 Communication MQTT Bidirectionnelle
- **Listener** : Écoute des messages des chauffeurs et clients
- **Publisher** : Diffusion des statuts et positions en temps réel
- **Reconnexion automatique** avec gestion des messages en attente

### 📍 Gestion des Positions
- **Position générale** : Suivi des chauffeurs hors réservation
- **Position de réservation** : Suivi en temps réel pendant une course
- **Distinction claire** entre les deux types de positions

### 💬 Système de Chat
- **Chat par réservation** avec topics dédiés
- **Archivage automatique** après 100 messages
- **Sauvegarde de secours** en cas d'échec d'archivage
- **Confirmation de réception** des messages

### 🗄️ Stockage Redis
- **Statuts des chauffeurs** (disponibilité, en ligne, en course)
- **Positions en temps réel** avec métadonnées
- **Historique des messages** de chat
- **Nettoyage automatique** des données expirées

## 🏗️ Architecture

### Topics MQTT

```
# Positions générales des chauffeurs
chauffeur/{chauffeurId}/position

# Positions pendant une réservation
ktur/reservations/{reservationId}/position

# Statuts des chauffeurs
chauffeur/{chauffeurId}/status

# Messages de réservation
ktur/reservations/{reservationId}

# Nouvelles réservations
ktur/reservations/recentes

# Statut du serveur
ktur/server/status
ktur/server/heartbeat
```

### Structure des Données

#### Position Générale
```json
{
  "type": "general_position",
  "chauffeur_id": "26",
  "data": {
    "lat": 48.8566,
    "lng": 2.3522,
    "timestamp": 1640995200000
  }
}
```

#### Position de Réservation
```json
{
  "type": "reservation_position",
  "reservation_id": "12345",
  "chauffeur_id": "26",
  "position": {
    "lat": 48.8566,
    "lng": 2.3522,
    "accuracy": 5.0,
    "speed": 25.0,
    "heading": 180.0
  },
  "timestamp": 1640995200000
}
```

## 🚀 Installation et Configuration

### Prérequis
- Node.js 16+
- Redis 6+
- Broker MQTT (EMQX, Mosquitto, etc.)

### Variables d'Environnement

```bash
# Configuration générale
NODE_ENV=development
PORT=3000

# Redis
REDIS_URL=redis://localhost:6379

# MQTT
MQTT_ENABLED=true
MQTT_PUBLISHER_ENABLED=true

# Développement
MQTT_BROKER_URL_DEV=mqtt://test.mosquitto.org:1883
MQTT_USERNAME_DEV=
MQTT_PASSWORD_DEV=

# Production
MQTT_BROKER_URL=mqtts://pbb16a10.ala.us-east-1.emqxsl.com:8883
MQTT_USERNAME=Ktur_brocker
MQTT_PASSWORD=Ktur_brocker#2025

# API Laravel
LARAVEL_API_URL=https://api.ktur.fr
```

### Installation

```bash
npm install
npm start
```

## 📡 API Endpoints

### 🏥 Santé et Monitoring
- `GET /api/health` - Vérification de la santé du système
- `GET /api/stats` - Statistiques système en temps réel
- `POST /api/cleanup` - Déclenchement du nettoyage manuel

### 🔌 État MQTT
- `GET /api/mqtt/status` - État des connexions MQTT
- `POST /api/reconnect-publisher` - Reconnexion manuelle du publisher
- `POST /api/ecouter-topic` - Abonnement à un topic
- `POST /api/desabonner-topic` - Désabonnement d'un topic

### 👥 Gestion des Chauffeurs
- `GET /api/chauffeurs/status` - Statuts de tous les chauffeurs
- `GET /api/chauffeurs/:id/status` - Statut d'un chauffeur spécifique

### 💬 Gestion du Chat
- `POST /api/reservation/subscribe` - Abonnement au chat d'une réservation
- `POST /api/reservation/send-message` - Envoi d'un message
- `GET /api/reservation/:id/chat/messages` - Récupération des messages
- `DELETE /api/reservation/:id/chat/messages` - Suppression du chat
- `GET /api/reservation/:id/chat/status` - Statut du chat

## 🔧 Fonctionnalités Avancées

### 📊 Monitoring Automatique
- **Vérification de santé** toutes les 5 minutes
- **Nettoyage automatique** toutes les 6 heures
- **Statistiques horaires** automatiques
- **Alertes** en cas de problème

### 🛡️ Gestion d'Erreurs
- **Reconnexion automatique** MQTT
- **File d'attente** pour les messages en cas de déconnexion
- **Retry automatique** pour l'archivage des chats
- **Sauvegarde de secours** des données critiques

### 📈 Performance
- **QoS 1** pour la fiabilité des messages
- **Gestion des timeouts** configurable
- **Nettoyage des données** expirées
- **Optimisation mémoire** avec Winston

## 📝 Logs et Monitoring

### Format des Logs
```
2024-01-15 10:30:45 [INFO] Message chat reçu pour la réservation 12345 {"from":"chauffeur_26","message_length":45}
2024-01-15 10:30:46 [INFO] 📡 Position de réservation publiée pour 12345
2024-01-15 10:30:47 [INFO] 📊 Statistiques système horaires: {"chauffeurs":{"total":15,"en_ligne":8}}
```

### Rotation des Logs
- **Fichiers quotidiens** avec rétention de 14 jours
- **Niveaux de log** configurables par environnement
- **Format structuré** pour faciliter l'analyse

## 🚨 Gestion des Erreurs

### Types d'Erreurs Gérées
- **Déconnexion MQTT** : Reconnexion automatique
- **Erreur Redis** : Tentatives de reconnexion
- **Échec d'archivage** : Sauvegarde de secours
- **Messages invalides** : Validation et rejet
- **Timeout API** : Gestion des timeouts

### Stratégies de Récupération
- **Retry automatique** avec backoff exponentiel
- **Fallback** vers des mécanismes alternatifs
- **Notification** des erreurs critiques
- **Logging détaillé** pour le debugging

## 🔄 Tâches Planifiées

### Nettoyage Automatique (6h)
- Suppression des positions anciennes (>24h)
- Nettoyage des sauvegardes expirées (>7 jours)
- Optimisation de la mémoire Redis

### Vérification de Santé (5min)
- Test de connectivité Redis
- Vérification des connexions MQTT
- Monitoring de la file d'attente
- Statistiques système

## 📊 Métriques et Alertes

### Métriques Collectées
- **Nombre de chauffeurs** connectés/en ligne/en course
- **Réservations actives** et messages de chat
- **Topics MQTT** souscrits
- **Messages en attente** dans la file
- **Utilisation mémoire** et uptime

### Seuils d'Alerte
- **Redis déconnecté** : Erreur critique
- **File d'attente >80%** : Avertissement
- **Échec d'archivage** : Tentatives multiples
- **Déconnexion MQTT** : Reconnexion automatique

## 🚀 Déploiement

### Production
```bash
NODE_ENV=production
npm start
```

### Développement
```bash
NODE_ENV=development
npm run dev
```

### Docker (à venir)
```dockerfile
FROM node:18-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --only=production
COPY . .
EXPOSE 3000
CMD ["npm", "start"]
```

## 🤝 Contribution

### Structure du Code
- **Modularité** : Fonctions séparées par responsabilité
- **Gestion d'erreurs** : Try-catch avec logging détaillé
- **Configuration** : Variables d'environnement centralisées
- **Documentation** : Commentaires explicatifs

### Bonnes Pratiques
- **Logging structuré** avec Winston
- **Gestion des promesses** avec async/await
- **Validation des données** en entrée
- **Tests unitaires** (à implémenter)

## 📚 Ressources

### Documentation
- [MQTT.js Documentation](https://github.com/mqttjs/MQTT.js)
- [ioredis Documentation](https://github.com/luin/ioredis)
- [Winston Documentation](https://github.com/winstonjs/winston)

### Support
- **Issues** : GitHub Issues
- **Documentation** : Ce README
- **Configuration** : Variables d'environnement

---

**Version** : 2.0.0  
**Dernière mise à jour** : Janvier 2024  
**Maintenu par** : Équipe KTUR
