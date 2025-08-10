#!/usr/bin/env node

/**
 * Script de test pour le serveur MQTT KTUR
 * Teste les endpoints API et la connectivité
 */

const axios = require('axios');

const BASE_URL = 'http://localhost:3000';
const TEST_TIMEOUT = 10000;

// Configuration axios avec timeout
const api = axios.create({
  baseURL: BASE_URL,
  timeout: TEST_TIMEOUT,
  headers: {
    'Content-Type': 'application/json'
  }
});

// Couleurs pour la console
const colors = {
  reset: '\x1b[0m',
  bright: '\x1b[1m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  magenta: '\x1b[35m',
  cyan: '\x1b[36m'
};

function log(message, color = 'reset') {
  console.log(`${colors[color]}${message}${colors.reset}`);
}

function logTest(testName, status, details = '') {
  const statusIcon = status === 'PASS' ? '✅' : '❌';
  const statusColor = status === 'PASS' ? 'green' : 'red';
  log(`${statusIcon} ${testName}: ${status}`, statusColor);
  if (details) {
    log(`   ${details}`, 'cyan');
  }
}

async function testEndpoint(endpoint, method = 'GET', data = null) {
  try {
    const config = { method, url: endpoint };
    if (data) config.data = data;
    
    const response = await api(config);
    return { success: true, data: response.data, status: response.status };
  } catch (error) {
    return { 
      success: false, 
      error: error.message, 
      status: error.response?.status || 'NETWORK_ERROR' 
    };
  }
}

async function runTests() {
  log('\n🚀 Démarrage des tests du serveur MQTT KTUR', 'bright');
  log('=' * 50, 'blue');
  
  let passedTests = 0;
  let totalTests = 0;

  // Test 1: Vérification de la santé du système
  log('\n🏥 Test 1: Vérification de la santé du système', 'yellow');
  totalTests++;
  
  const healthResult = await testEndpoint('/api/health');
  if (healthResult.success) {
    logTest('GET /api/health', 'PASS', `Status: ${healthResult.status}`);
    log(`   Redis: ${healthResult.data.redis?.connected ? 'Connecté' : 'Déconnecté'}`);
    log(`   MQTT Listener: ${healthResult.data.mqtt?.listener?.connected ? 'Connecté' : 'Déconnecté'}`);
    log(`   MQTT Publisher: ${healthResult.data.mqtt?.publisher?.connected ? 'Connecté' : 'Déconnecté'}`);
    passedTests++;
  } else {
    logTest('GET /api/health', 'FAIL', `Erreur: ${healthResult.error}`);
  }

  // Test 2: Statistiques du système
  log('\n📊 Test 2: Statistiques du système', 'yellow');
  totalTests++;
  
  const statsResult = await testEndpoint('/api/stats');
  if (statsResult.success) {
    logTest('GET /api/stats', 'PASS', `Status: ${statsResult.status}`);
    log(`   Chauffeurs: ${statsResult.data.chauffeurs?.total || 0}`);
    log(`   Réservations actives: ${statsResult.data.reservations?.active || 0}`);
    log(`   Messages en attente: ${statsResult.data.mqtt?.pending_messages || 0}`);
    passedTests++;
  } else {
    logTest('GET /api/stats', 'FAIL', `Erreur: ${statsResult.error}`);
  }

  // Test 3: État MQTT
  log('\n🔌 Test 3: État MQTT', 'yellow');
  totalTests++;
  
  const mqttStatusResult = await testEndpoint('/api/mqtt/status');
  if (mqttStatusResult.success) {
    logTest('GET /api/mqtt/status', 'PASS', `Status: ${mqttStatusResult.status}`);
    log(`   MQTT activé: ${mqttStatusResult.data.mqtt_enabled}`);
    log(`   Publisher activé: ${mqttStatusResult.data.publisher_enabled}`);
    log(`   Topics souscrits: ${mqttStatusResult.data.subscribed_topics?.length || 0}`);
    passedTests++;
  } else {
    logTest('GET /api/mqtt/status', 'FAIL', `Erreur: ${mqttStatusResult.error}`);
  }

  // Test 4: Statuts des chauffeurs
  log('\n👥 Test 4: Statuts des chauffeurs', 'yellow');
  totalTests++;
  
  const chauffeursResult = await testEndpoint('/api/chauffeurs/status');
  if (chauffeursResult.success) {
    logTest('GET /api/chauffeurs/status', 'PASS', `Status: ${chauffeursResult.status}`);
    log(`   Nombre de chauffeurs: ${chauffeursResult.data.chauffeurs?.length || 0}`);
    passedTests++;
  } else {
    logTest('GET /api/chauffeurs/status', 'FAIL', `Erreur: ${chauffeursResult.error}`);
  }

  // Test 5: Test d'envoi de message (simulation)
  log('\n💬 Test 5: Test d\'envoi de message', 'yellow');
  totalTests++;
  
  const testMessage = {
    reservation_id: 'test_123',
    message: {
      from: 'test_chauffeur',
      content: 'Message de test'
    }
  };
  
  const messageResult = await testEndpoint('/api/reservation/send-message', 'POST', testMessage);
  if (messageResult.success) {
    logTest('POST /api/reservation/send-message', 'PASS', `Status: ${messageResult.status}`);
    log(`   Message ID: ${messageResult.data.message_id}`);
    log(`   Timestamp: ${messageResult.data.timestamp}`);
    passedTests++;
  } else {
    logTest('POST /api/reservation/send-message', 'FAIL', `Erreur: ${messageResult.error}`);
  }

  // Test 6: Test de nettoyage
  log('\n🧹 Test 6: Test de nettoyage', 'yellow');
  totalTests++;
  
  const cleanupResult = await testEndpoint('/api/cleanup', 'POST');
  if (cleanupResult.success) {
    logTest('POST /api/cleanup', 'PASS', `Status: ${cleanupResult.status}`);
    log(`   Message: ${cleanupResult.data.message}`);
    passedTests++;
  } else {
    logTest('POST /api/cleanup', 'FAIL', `Erreur: ${cleanupResult.error}`);
  }

  // Résumé des tests
  log('\n' + '=' * 50, 'blue');
  log(`📋 Résumé des tests: ${passedTests}/${totalTests} réussis`, 'bright');
  
  if (passedTests === totalTests) {
    log('🎉 Tous les tests sont passés avec succès!', 'green');
  } else {
    log(`⚠️  ${totalTests - passedTests} test(s) ont échoué`, 'yellow');
  }
  
  log('=' * 50, 'blue');
  
  // Suggestions d'amélioration
  if (passedTests < totalTests) {
    log('\n💡 Suggestions d\'amélioration:', 'yellow');
    log('   - Vérifiez que le serveur est démarré sur le port 3000');
    log('   - Vérifiez la connectivité Redis');
    log('   - Vérifiez la configuration MQTT');
    log('   - Consultez les logs du serveur pour plus de détails');
  }
}

// Gestion des erreurs
process.on('unhandledRejection', (reason, promise) => {
  log(`❌ Promesse rejetée non gérée: ${reason}`, 'red');
  process.exit(1);
});

process.on('uncaughtException', (error) => {
  log(`❌ Erreur non capturée: ${error.message}`, 'red');
  process.exit(1);
});

// Exécution des tests
if (require.main === module) {
  runTests().catch(error => {
    log(`❌ Erreur lors de l'exécution des tests: ${error.message}`, 'red');
    process.exit(1);
  });
}

module.exports = { runTests, testEndpoint };
