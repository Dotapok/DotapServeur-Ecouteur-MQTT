# Configuration MQTT - Serveur Écouteur KTUR

## 🚀 Démarrage rapide

```bash
npm start
```

## ⚙️ Configuration MQTT

### Variables d'environnement

| Variable | Description | Défaut | Exemple |
|----------|-------------|--------|---------|
| `MQTT_ENABLED` | Active/désactive MQTT complètement | `true` | `false` |
| `MQTT_PUBLISHER_ENABLED` | Active/désactive seulement le publisher | `true` | `false` |
| `MQTT_BROKER_URL` | URL du broker MQTT | `mqtt://localhost:1883` | `mqtts://broker.emqx.io:8883` |
| `MQTT_USERNAME` | Nom d'utilisateur MQTT | (vide) | `Ktur_brocker` |
| `MQTT_PASSWORD` | Mot de passe MQTT | (vide) | `password123` |

### Exemples de configuration

#### 1. Désactiver complètement MQTT
```bash
set MQTT_ENABLED=false
npm start
```

#### 2. Garder l'écoute MQTT mais désactiver la publication
```bash
set MQTT_PUBLISHER_ENABLED=false
npm start
```

#### 3. Utiliser un broker public (pour tests)
```bash
set MQTT_BROKER_URL=mqtts://broker.emqx.io:8883
set MQTT_USERNAME=Ktur_brocker
npm start
```

## 🔧 Fonctionnalités

### ✅ Connexion stable
- **Pas de reconnexion automatique** : Le publisher ne se reconnecte plus automatiquement
- **Keepalive augmenté** : 120 secondes au lieu de 60
- **Will message** : Notification automatique quand le serveur se déconnecte

### 🔄 Reconnexion manuelle
Si le publisher se déconnecte, vous pouvez le reconnecter manuellement :

```bash
curl -X POST http://localhost:3000/api/reconnect-publisher
```

### 📡 Topics MQTT

#### Topics d'écoute (Listener)
- `ktur/chauffeurs/[chauffeur_id]/status` : Statut des chauffeurs
- `ktur/chauffeurs/[chauffeur_id]/position` : Position GPS

#### Topics de publication (Publisher)
- `ktur/chauffeurs/status/[chauffeur_id]` : Statut publié
- `ktur/chauffeurs/position/[chauffeur_id]` : Position publiée
- `ktur/server/status` : Statut du serveur (online/offline)

## 🎯 Cas d'usage

### Développement
```bash
# Désactiver le publisher pour éviter les reconnexions
set MQTT_PUBLISHER_ENABLED=false
npm start
```

### Production
```bash
# Configuration complète
set MQTT_BROKER_URL=mqtt://votre-broker:1883
set MQTT_USERNAME=votre-username
set MQTT_PASSWORD=votre-password
set MQTT_ENABLED=true
set MQTT_PUBLISHER_ENABLED=true
npm start
```

### Tests
```bash
# Utiliser un broker public
set MQTT_BROKER_URL=mqtts://test.mosquitto.org:8883
set MQTT_ENABLED=true
npm start
```

## 🔍 Diagnostic

### Vérifier l'état des connexions
```bash
# Statut du serveur
curl http://localhost:3000/api/chauffeurs/status

# Reconnecter le publisher si nécessaire
curl -X POST http://localhost:3000/api/reconnect-publisher
```

### Logs attendus
```
🔧 Configuration MQTT:
   Broker: mqtts://broker.emqx.io:8883
   Username: Ktur_brocker
   Activé: true
   Publisher: true
✅ Connecté à Redis
✅ Connecté à MQTT (Listener)
✅ Publisher MQTT connecté
🚀 Serveur ecouteur MQTT en écoute sur le port 3000
``` 