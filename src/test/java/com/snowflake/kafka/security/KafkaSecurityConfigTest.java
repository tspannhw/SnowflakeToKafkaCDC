package com.snowflake.kafka.security;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileOutputStream;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive unit tests for KafkaSecurityConfig.
 * Tests SSL/TLS and SASL authentication configurations for enterprise Kafka security.
 */
class KafkaSecurityConfigTest {
    
    @TempDir
    Path tempDir;
    
    private Path keystorePath;
    private Path truststorePath;
    private String keystorePassword = "keystore-password";
    private String truststorePassword = "truststore-password";
    
    @BeforeEach
    void setUp() throws Exception {
        // Create test keystore
        keystorePath = tempDir.resolve("test-keystore.jks");
        KeyStore keystore = KeyStore.getInstance("JKS");
        keystore.load(null, null);
        try (FileOutputStream fos = new FileOutputStream(keystorePath.toFile())) {
            keystore.store(fos, keystorePassword.toCharArray());
        }
        
        // Create test truststore
        truststorePath = tempDir.resolve("test-truststore.jks");
        KeyStore truststore = KeyStore.getInstance("JKS");
        truststore.load(null, null);
        try (FileOutputStream fos = new FileOutputStream(truststorePath.toFile())) {
            truststore.store(fos, truststorePassword.toCharArray());
        }
    }
    
    @Test
    void testPlaintextConfiguration() {
        KafkaSecurityConfig config = new KafkaSecurityConfig.Builder()
            .securityProtocol(KafkaSecurityConfig.SecurityProtocol.PLAINTEXT)
            .build();
        
        Properties baseProps = new Properties();
        baseProps.setProperty("bootstrap.servers", "localhost:9092");
        
        Properties producerProps = config.createProducerProperties(baseProps);
        
        assertEquals("PLAINTEXT", producerProps.getProperty("security.protocol"));
        assertFalse(producerProps.containsKey("ssl.keystore.location"));
        assertFalse(producerProps.containsKey("sasl.mechanism"));
        
        KafkaSecurityConfig.SecuritySummary summary = config.getSecuritySummary();
        assertEquals(KafkaSecurityConfig.SecurityProtocol.PLAINTEXT, summary.getSecurityProtocol());
        assertFalse(summary.hasKeystore());
        assertFalse(summary.hasTruststore());
    }
    
    @Test
    void testSSLConfiguration() {
        KafkaSecurityConfig config = new KafkaSecurityConfig.Builder()
            .securityProtocol(KafkaSecurityConfig.SecurityProtocol.SSL)
            .keystore(keystorePath.toString(), keystorePassword)
            .truststore(truststorePath.toString(), truststorePassword)
            .enableHostnameVerification(true)
            .build();
        
        Properties baseProps = new Properties();
        baseProps.setProperty("bootstrap.servers", "localhost:9093");
        baseProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        Properties producerProps = config.createProducerProperties(baseProps);
        
        assertEquals("SSL", producerProps.getProperty("security.protocol"));
        assertEquals(keystorePath.toString(), producerProps.getProperty("ssl.keystore.location"));
        assertEquals(keystorePassword, producerProps.getProperty("ssl.keystore.password"));
        assertEquals(truststorePath.toString(), producerProps.getProperty("ssl.truststore.location"));
        assertEquals(truststorePassword, producerProps.getProperty("ssl.truststore.password"));
        assertEquals("TLSv1.3", producerProps.getProperty("ssl.protocol"));
        assertEquals("https", producerProps.getProperty("ssl.endpoint.identification.algorithm"));
        assertEquals("true", producerProps.getProperty("enable.idempotence"));
        
        KafkaSecurityConfig.SecuritySummary summary = config.getSecuritySummary();
        assertEquals(KafkaSecurityConfig.SecurityProtocol.SSL, summary.getSecurityProtocol());
        assertTrue(summary.hasKeystore());
        assertTrue(summary.hasTruststore());
        assertTrue(summary.isHostnameVerificationEnabled());
    }
    
    @Test
    void testSASLPlainConfiguration() {
        String username = "test-user";
        String password = "test-password";
        
        KafkaSecurityConfig config = new KafkaSecurityConfig.Builder()
            .securityProtocol(KafkaSecurityConfig.SecurityProtocol.SASL_PLAINTEXT)
            .saslMechanism(KafkaSecurityConfig.SaslMechanism.PLAIN)
            .saslCredentials(username, password)
            .build();
        
        Properties baseProps = new Properties();
        baseProps.setProperty("bootstrap.servers", "localhost:9092");
        
        Properties producerProps = config.createProducerProperties(baseProps);
        
        assertEquals("SASL_PLAINTEXT", producerProps.getProperty("security.protocol"));
        assertEquals("PLAIN", producerProps.getProperty("sasl.mechanism"));
        assertTrue(producerProps.getProperty("sasl.jaas.config").contains("username=\"" + username + "\""));
        assertTrue(producerProps.getProperty("sasl.jaas.config").contains("password=\"" + password + "\""));
        assertTrue(producerProps.getProperty("sasl.jaas.config").contains("PlainLoginModule"));
        
        KafkaSecurityConfig.SecuritySummary summary = config.getSecuritySummary();
        assertEquals(KafkaSecurityConfig.SecurityProtocol.SASL_PLAINTEXT, summary.getSecurityProtocol());
        assertEquals(KafkaSecurityConfig.SaslMechanism.PLAIN, summary.getSaslMechanism());
        assertEquals(username, summary.getSaslUsername());
    }
    
    @Test
    void testSASLScramConfiguration() {
        String username = "scram-user";
        String password = "scram-password";
        
        KafkaSecurityConfig config = new KafkaSecurityConfig.Builder()
            .securityProtocol(KafkaSecurityConfig.SecurityProtocol.SASL_SSL)
            .saslMechanism(KafkaSecurityConfig.SaslMechanism.SCRAM_SHA_256)
            .saslCredentials(username, password)
            .keystore(keystorePath.toString(), keystorePassword)
            .truststore(truststorePath.toString(), truststorePassword)
            .build();
        
        Properties baseProps = new Properties();
        baseProps.setProperty("bootstrap.servers", "localhost:9093");
        
        Properties producerProps = config.createProducerProperties(baseProps);
        
        assertEquals("SASL_SSL", producerProps.getProperty("security.protocol"));
        assertEquals("SCRAM-SHA-256", producerProps.getProperty("sasl.mechanism"));
        assertTrue(producerProps.getProperty("sasl.jaas.config").contains("ScramLoginModule"));
        assertTrue(producerProps.getProperty("sasl.jaas.config").contains("username=\"" + username + "\""));
        
        // Should also have SSL properties
        assertEquals(keystorePath.toString(), producerProps.getProperty("ssl.keystore.location"));
        assertEquals(truststorePath.toString(), producerProps.getProperty("ssl.truststore.location"));
    }
    
    @Test
    void testCustomJaasConfiguration() {
        String customJaasConfig = "org.apache.kafka.common.security.plain.PlainLoginModule required " +
            "username=\"custom-user\" password=\"custom-password\";";
        
        KafkaSecurityConfig config = new KafkaSecurityConfig.Builder()
            .securityProtocol(KafkaSecurityConfig.SecurityProtocol.SASL_PLAINTEXT)
            .saslMechanism(KafkaSecurityConfig.SaslMechanism.PLAIN)
            .saslJaasConfig(customJaasConfig)
            .build();
        
        Properties baseProps = new Properties();
        Properties producerProps = config.createProducerProperties(baseProps);
        
        assertEquals(customJaasConfig, producerProps.getProperty("sasl.jaas.config"));
    }
    
    @Test
    void testConsumerProperties() {
        KafkaSecurityConfig config = new KafkaSecurityConfig.Builder()
            .securityProtocol(KafkaSecurityConfig.SecurityProtocol.SSL)
            .keystore(keystorePath.toString(), keystorePassword)
            .truststore(truststorePath.toString(), truststorePassword)
            .build();
        
        Properties baseProps = new Properties();
        baseProps.setProperty("bootstrap.servers", "localhost:9093");
        baseProps.setProperty("group.id", "test-group");
        
        Properties consumerProps = config.createConsumerProperties(baseProps);
        
        assertEquals("SSL", consumerProps.getProperty("security.protocol"));
        assertEquals(keystorePath.toString(), consumerProps.getProperty("ssl.keystore.location"));
        assertEquals("test-group", consumerProps.getProperty("group.id"));
    }
    
    @Test
    void testSSLConnectivityTest() {
        KafkaSecurityConfig config = new KafkaSecurityConfig.Builder()
            .securityProtocol(KafkaSecurityConfig.SecurityProtocol.SSL)
            .keystore(keystorePath.toString(), keystorePassword)
            .truststore(truststorePath.toString(), truststorePassword)
            .build();
        
        // This should succeed with our test keystores
        assertTrue(config.testSSLConnectivity());
        
        // Test with plaintext (should return true as SSL not required)
        KafkaSecurityConfig plaintextConfig = new KafkaSecurityConfig.Builder()
            .securityProtocol(KafkaSecurityConfig.SecurityProtocol.PLAINTEXT)
            .build();
        
        assertTrue(plaintextConfig.testSSLConnectivity());
    }
    
    @Test
    void testValidationErrors() {
        // Test missing keystore file
        assertThrows(IllegalArgumentException.class, () -> {
            new KafkaSecurityConfig.Builder()
                .securityProtocol(KafkaSecurityConfig.SecurityProtocol.SSL)
                .keystore("/non/existent/keystore.jks", "password")
                .build();
        });
        
        // Test missing truststore file
        assertThrows(IllegalArgumentException.class, () -> {
            new KafkaSecurityConfig.Builder()
                .securityProtocol(KafkaSecurityConfig.SecurityProtocol.SSL)
                .truststore("/non/existent/truststore.jks", "password")
                .build();
        });
        
        // Test missing SASL mechanism
        assertThrows(IllegalArgumentException.class, () -> {
            new KafkaSecurityConfig.Builder()
                .securityProtocol(KafkaSecurityConfig.SecurityProtocol.SASL_PLAINTEXT)
                .build();
        });
        
        // Test missing SASL credentials
        assertThrows(IllegalArgumentException.class, () -> {
            new KafkaSecurityConfig.Builder()
                .securityProtocol(KafkaSecurityConfig.SecurityProtocol.SASL_PLAINTEXT)
                .saslMechanism(KafkaSecurityConfig.SaslMechanism.PLAIN)
                .build();
        });
    }
    
    @Test
    void testHostnameVerificationDisabled() {
        KafkaSecurityConfig config = new KafkaSecurityConfig.Builder()
            .securityProtocol(KafkaSecurityConfig.SecurityProtocol.SSL)
            .keystore(keystorePath.toString(), keystorePassword)
            .truststore(truststorePath.toString(), truststorePassword)
            .enableHostnameVerification(false)
            .build();
        
        Properties producerProps = config.createProducerProperties(new Properties());
        
        assertEquals("", producerProps.getProperty("ssl.endpoint.identification.algorithm"));
        
        KafkaSecurityConfig.SecuritySummary summary = config.getSecuritySummary();
        assertFalse(summary.isHostnameVerificationEnabled());
    }
    
    @Test
    void testSecuritySummaryToString() {
        KafkaSecurityConfig config = new KafkaSecurityConfig.Builder()
            .securityProtocol(KafkaSecurityConfig.SecurityProtocol.SASL_SSL)
            .saslMechanism(KafkaSecurityConfig.SaslMechanism.SCRAM_SHA_256)
            .saslCredentials("test-user", "test-password")
            .keystore(keystorePath.toString(), keystorePassword)
            .truststore(truststorePath.toString(), truststorePassword)
            .build();
        
        KafkaSecurityConfig.SecuritySummary summary = config.getSecuritySummary();
        String summaryString = summary.toString();
        
        assertTrue(summaryString.contains("SASL_SSL"));
        assertTrue(summaryString.contains("SCRAM_SHA_256"));
        assertTrue(summaryString.contains("keystore=true"));
        assertTrue(summaryString.contains("truststore=true"));
        assertTrue(summaryString.contains("test-user"));
    }
    
    @Test
    void testPerformanceWithSecuritySettings() {
        KafkaSecurityConfig config = new KafkaSecurityConfig.Builder()
            .securityProtocol(KafkaSecurityConfig.SecurityProtocol.SSL)
            .keystore(keystorePath.toString(), keystorePassword)
            .truststore(truststorePath.toString(), truststorePassword)
            .build();
        
        Properties baseProps = new Properties();
        baseProps.setProperty("bootstrap.servers", "localhost:9093");
        
        // Test performance of property creation
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            Properties props = config.createProducerProperties(baseProps);
            assertNotNull(props);
        }
        long endTime = System.currentTimeMillis();
        
        long totalTime = endTime - startTime;
        assertTrue(totalTime < 1000, "Property creation should be fast: " + totalTime + "ms for 1000 calls");
        
        System.out.println("Created 1000 secure property sets in " + totalTime + "ms");
    }
    
    @Test
    void testAllSaslMechanisms() {
        // Test all SASL mechanisms can be configured
        for (KafkaSecurityConfig.SaslMechanism mechanism : KafkaSecurityConfig.SaslMechanism.values()) {
            if (mechanism == KafkaSecurityConfig.SaslMechanism.GSSAPI || 
                mechanism == KafkaSecurityConfig.SaslMechanism.OAUTHBEARER) {
                // Skip mechanisms that require special setup
                continue;
            }
            
            KafkaSecurityConfig.Builder builder = new KafkaSecurityConfig.Builder()
                .securityProtocol(KafkaSecurityConfig.SecurityProtocol.SASL_PLAINTEXT)
                .saslMechanism(mechanism);
            
            if (mechanism == KafkaSecurityConfig.SaslMechanism.PLAIN ||
                mechanism == KafkaSecurityConfig.SaslMechanism.SCRAM_SHA_256 ||
                mechanism == KafkaSecurityConfig.SaslMechanism.SCRAM_SHA_512) {
                builder.saslCredentials("test-user", "test-password");
            }
            
            KafkaSecurityConfig config = builder.build();
            Properties props = config.createProducerProperties(new Properties());
            
            assertEquals(mechanism.getMechanism(), props.getProperty("sasl.mechanism"));
        }
    }
}
