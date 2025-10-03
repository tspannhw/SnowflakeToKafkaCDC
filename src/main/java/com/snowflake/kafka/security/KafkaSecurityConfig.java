package com.snowflake.kafka.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Properties;

/**
 * Enterprise-grade Kafka security configuration supporting SSL/TLS and SASL authentication.
 * Provides comprehensive security settings for production Kafka deployments.
 */
public class KafkaSecurityConfig {
    private static final Logger logger = LoggerFactory.getLogger(KafkaSecurityConfig.class);
    
    public enum SecurityProtocol {
        PLAINTEXT("PLAINTEXT"),
        SSL("SSL"),
        SASL_PLAINTEXT("SASL_PLAINTEXT"),
        SASL_SSL("SASL_SSL");
        
        private final String protocol;
        
        SecurityProtocol(String protocol) {
            this.protocol = protocol;
        }
        
        public String getProtocol() {
            return protocol;
        }
    }
    
    public enum SaslMechanism {
        PLAIN("PLAIN"),
        SCRAM_SHA_256("SCRAM-SHA-256"),
        SCRAM_SHA_512("SCRAM-SHA-512"),
        GSSAPI("GSSAPI"),
        OAUTHBEARER("OAUTHBEARER");
        
        private final String mechanism;
        
        SaslMechanism(String mechanism) {
            this.mechanism = mechanism;
        }
        
        public String getMechanism() {
            return mechanism;
        }
    }
    
    private final SecurityProtocol securityProtocol;
    private final SaslMechanism saslMechanism;
    private final String keystorePath;
    private final String keystorePassword;
    private final String truststorePath;
    private final String truststorePassword;
    private final String saslUsername;
    private final String saslPassword;
    private final String saslJaasConfig;
    private final boolean enableHostnameVerification;
    private final String sslEndpointIdentificationAlgorithm;
    
    private KafkaSecurityConfig(Builder builder) {
        this.securityProtocol = builder.securityProtocol;
        this.saslMechanism = builder.saslMechanism;
        this.keystorePath = builder.keystorePath;
        this.keystorePassword = builder.keystorePassword;
        this.truststorePath = builder.truststorePath;
        this.truststorePassword = builder.truststorePassword;
        this.saslUsername = builder.saslUsername;
        this.saslPassword = builder.saslPassword;
        this.saslJaasConfig = builder.saslJaasConfig;
        this.enableHostnameVerification = builder.enableHostnameVerification;
        this.sslEndpointIdentificationAlgorithm = builder.sslEndpointIdentificationAlgorithm;
        
        validateConfiguration();
        logger.info("Kafka security configuration initialized: protocol={}, mechanism={}", 
            securityProtocol, saslMechanism);
    }
    
    /**
     * Creates Kafka producer properties with security configuration.
     */
    public Properties createProducerProperties(Properties baseProperties) {
        Properties secureProperties = new Properties();
        secureProperties.putAll(baseProperties);
        
        // Security protocol
        secureProperties.setProperty("security.protocol", securityProtocol.getProtocol());
        
        // SSL Configuration
        if (securityProtocol == SecurityProtocol.SSL || securityProtocol == SecurityProtocol.SASL_SSL) {
            configureSSL(secureProperties);
        }
        
        // SASL Configuration
        if (securityProtocol == SecurityProtocol.SASL_PLAINTEXT || securityProtocol == SecurityProtocol.SASL_SSL) {
            configureSASL(secureProperties);
        }
        
        // Additional security settings
        configureAdditionalSecurity(secureProperties);
        
        logger.debug("Created secure Kafka producer properties with {} entries", secureProperties.size());
        return secureProperties;
    }
    
    /**
     * Creates Kafka consumer properties with security configuration.
     */
    public Properties createConsumerProperties(Properties baseProperties) {
        Properties secureProperties = new Properties();
        secureProperties.putAll(baseProperties);
        
        // Security protocol
        secureProperties.setProperty("security.protocol", securityProtocol.getProtocol());
        
        // SSL Configuration
        if (securityProtocol == SecurityProtocol.SSL || securityProtocol == SecurityProtocol.SASL_SSL) {
            configureSSL(secureProperties);
        }
        
        // SASL Configuration
        if (securityProtocol == SecurityProtocol.SASL_PLAINTEXT || securityProtocol == SecurityProtocol.SASL_SSL) {
            configureSASL(secureProperties);
        }
        
        // Additional security settings
        configureAdditionalSecurity(secureProperties);
        
        logger.debug("Created secure Kafka consumer properties with {} entries", secureProperties.size());
        return secureProperties;
    }
    
    private void configureSSL(Properties properties) {
        // Keystore configuration (client certificate)
        if (keystorePath != null && !keystorePath.isEmpty()) {
            properties.setProperty("ssl.keystore.location", keystorePath);
            properties.setProperty("ssl.keystore.password", keystorePassword != null ? keystorePassword : "");
            properties.setProperty("ssl.keystore.type", "JKS");
        }
        
        // Truststore configuration (CA certificates)
        if (truststorePath != null && !truststorePath.isEmpty()) {
            properties.setProperty("ssl.truststore.location", truststorePath);
            properties.setProperty("ssl.truststore.password", truststorePassword != null ? truststorePassword : "");
            properties.setProperty("ssl.truststore.type", "JKS");
        }
        
        // SSL protocol and cipher suites
        properties.setProperty("ssl.protocol", "TLSv1.3");
        properties.setProperty("ssl.enabled.protocols", "TLSv1.3,TLSv1.2");
        properties.setProperty("ssl.cipher.suites", 
            "TLS_AES_256_GCM_SHA384,TLS_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
        
        // Hostname verification
        if (enableHostnameVerification) {
            properties.setProperty("ssl.endpoint.identification.algorithm", 
                sslEndpointIdentificationAlgorithm != null ? sslEndpointIdentificationAlgorithm : "https");
        } else {
            properties.setProperty("ssl.endpoint.identification.algorithm", "");
        }
        
        // Additional SSL settings for security
        properties.setProperty("ssl.client.auth", "required");
        properties.setProperty("ssl.key.password", keystorePassword != null ? keystorePassword : "");
        
        logger.debug("Configured SSL settings for Kafka");
    }
    
    private void configureSASL(Properties properties) {
        properties.setProperty("sasl.mechanism", saslMechanism.getMechanism());
        
        if (saslJaasConfig != null && !saslJaasConfig.isEmpty()) {
            // Use custom JAAS configuration
            properties.setProperty("sasl.jaas.config", saslJaasConfig);
        } else {
            // Generate JAAS configuration based on mechanism
            String jaasConfig = generateJaasConfig();
            properties.setProperty("sasl.jaas.config", jaasConfig);
        }
        
        // Additional SASL settings
        if (saslMechanism == SaslMechanism.GSSAPI) {
            properties.setProperty("sasl.kerberos.service.name", "kafka");
            properties.setProperty("sasl.kerberos.kinit.cmd", "/usr/bin/kinit");
            properties.setProperty("sasl.kerberos.ticket.renew.window.factor", "0.80");
            properties.setProperty("sasl.kerberos.ticket.renew.jitter", "0.05");
            properties.setProperty("sasl.kerberos.min.time.before.relogin", "60000");
        }
        
        logger.debug("Configured SASL settings for Kafka with mechanism: {}", saslMechanism);
    }
    
    private String generateJaasConfig() {
        switch (saslMechanism) {
            case PLAIN:
                return String.format("org.apache.kafka.common.security.plain.PlainLoginModule required " +
                    "username=\"%s\" password=\"%s\";", saslUsername, saslPassword);
                
            case SCRAM_SHA_256:
                return String.format("org.apache.kafka.common.security.scram.ScramLoginModule required " +
                    "username=\"%s\" password=\"%s\";", saslUsername, saslPassword);
                
            case SCRAM_SHA_512:
                return String.format("org.apache.kafka.common.security.scram.ScramLoginModule required " +
                    "username=\"%s\" password=\"%s\";", saslUsername, saslPassword);
                
            case GSSAPI:
                return "com.sun.security.auth.module.Krb5LoginModule required " +
                    "useKeyTab=true storeKey=true keyTab=\"/etc/security/keytabs/kafka.keytab\" " +
                    "principal=\"kafka/kafka-broker@REALM.COM\";";
                
            case OAUTHBEARER:
                return "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;";
                
            default:
                throw new IllegalArgumentException("Unsupported SASL mechanism: " + saslMechanism);
        }
    }
    
    private void configureAdditionalSecurity(Properties properties) {
        // Connection security
        properties.setProperty("connections.max.idle.ms", "540000"); // 9 minutes
        properties.setProperty("request.timeout.ms", "30000");
        properties.setProperty("retry.backoff.ms", "100");
        
        // Metadata security
        properties.setProperty("metadata.max.age.ms", "300000"); // 5 minutes
        
        // Producer-specific security settings
        if (properties.containsKey("key.serializer")) {
            properties.setProperty("enable.idempotence", "true");
            properties.setProperty("max.in.flight.requests.per.connection", "5");
            properties.setProperty("retries", "2147483647");
        }
        
        logger.debug("Configured additional security settings for Kafka");
    }
    
    private void validateConfiguration() {
        // Validate SSL configuration
        if (securityProtocol == SecurityProtocol.SSL || securityProtocol == SecurityProtocol.SASL_SSL) {
            if (truststorePath != null && !truststorePath.isEmpty()) {
                Path truststore = Paths.get(truststorePath);
                if (!Files.exists(truststore)) {
                    throw new IllegalArgumentException("Truststore file not found: " + truststorePath);
                }
                if (!Files.isReadable(truststore)) {
                    throw new IllegalArgumentException("Truststore file is not readable: " + truststorePath);
                }
            }
            
            if (keystorePath != null && !keystorePath.isEmpty()) {
                Path keystore = Paths.get(keystorePath);
                if (!Files.exists(keystore)) {
                    throw new IllegalArgumentException("Keystore file not found: " + keystorePath);
                }
                if (!Files.isReadable(keystore)) {
                    throw new IllegalArgumentException("Keystore file is not readable: " + keystorePath);
                }
            }
        }
        
        // Validate SASL configuration
        if (securityProtocol == SecurityProtocol.SASL_PLAINTEXT || securityProtocol == SecurityProtocol.SASL_SSL) {
            if (saslMechanism == null) {
                throw new IllegalArgumentException("SASL mechanism must be specified for SASL security protocol");
            }
            
            if (saslJaasConfig == null || saslJaasConfig.isEmpty()) {
                if ((saslUsername == null || saslUsername.isEmpty()) && 
                    (saslMechanism == SaslMechanism.PLAIN || saslMechanism == SaslMechanism.SCRAM_SHA_256 || saslMechanism == SaslMechanism.SCRAM_SHA_512)) {
                    throw new IllegalArgumentException("SASL username must be specified for " + saslMechanism);
                }
                
                if ((saslPassword == null || saslPassword.isEmpty()) && 
                    (saslMechanism == SaslMechanism.PLAIN || saslMechanism == SaslMechanism.SCRAM_SHA_256 || saslMechanism == SaslMechanism.SCRAM_SHA_512)) {
                    throw new IllegalArgumentException("SASL password must be specified for " + saslMechanism);
                }
            }
        }
        
        logger.info("Kafka security configuration validation successful");
    }
    
    /**
     * Tests SSL connectivity by attempting to create SSL context.
     */
    public boolean testSSLConnectivity() {
        if (securityProtocol != SecurityProtocol.SSL && securityProtocol != SecurityProtocol.SASL_SSL) {
            return true; // SSL not required
        }
        
        try {
            SSLContext sslContext = SSLContext.getInstance("TLSv1.3");
            
            KeyManagerFactory kmf = null;
            if (keystorePath != null && !keystorePath.isEmpty()) {
                KeyStore keystore = KeyStore.getInstance("JKS");
                try (FileInputStream fis = new FileInputStream(keystorePath)) {
                    keystore.load(fis, keystorePassword != null ? keystorePassword.toCharArray() : null);
                }
                kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(keystore, keystorePassword != null ? keystorePassword.toCharArray() : null);
            }
            
            TrustManagerFactory tmf = null;
            if (truststorePath != null && !truststorePath.isEmpty()) {
                KeyStore truststore = KeyStore.getInstance("JKS");
                try (FileInputStream fis = new FileInputStream(truststorePath)) {
                    truststore.load(fis, truststorePassword != null ? truststorePassword.toCharArray() : null);
                }
                tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(truststore);
            }
            
            sslContext.init(
                kmf != null ? kmf.getKeyManagers() : null,
                tmf != null ? tmf.getTrustManagers() : null,
                new SecureRandom()
            );
            
            logger.info("SSL connectivity test successful");
            return true;
            
        } catch (Exception e) {
            logger.error("SSL connectivity test failed", e);
            return false;
        }
    }
    
    /**
     * Gets security configuration summary for logging.
     */
    public SecuritySummary getSecuritySummary() {
        return new SecuritySummary(
            securityProtocol,
            saslMechanism,
            keystorePath != null && !keystorePath.isEmpty(),
            truststorePath != null && !truststorePath.isEmpty(),
            enableHostnameVerification,
            saslUsername
        );
    }
    
    public static class Builder {
        private SecurityProtocol securityProtocol = SecurityProtocol.PLAINTEXT;
        private SaslMechanism saslMechanism;
        private String keystorePath;
        private String keystorePassword;
        private String truststorePath;
        private String truststorePassword;
        private String saslUsername;
        private String saslPassword;
        private String saslJaasConfig;
        private boolean enableHostnameVerification = true;
        private String sslEndpointIdentificationAlgorithm = "https";
        
        public Builder securityProtocol(SecurityProtocol protocol) {
            this.securityProtocol = protocol;
            return this;
        }
        
        public Builder saslMechanism(SaslMechanism mechanism) {
            this.saslMechanism = mechanism;
            return this;
        }
        
        public Builder keystore(String path, String password) {
            this.keystorePath = path;
            this.keystorePassword = password;
            return this;
        }
        
        public Builder truststore(String path, String password) {
            this.truststorePath = path;
            this.truststorePassword = password;
            return this;
        }
        
        public Builder saslCredentials(String username, String password) {
            this.saslUsername = username;
            this.saslPassword = password;
            return this;
        }
        
        public Builder saslJaasConfig(String jaasConfig) {
            this.saslJaasConfig = jaasConfig;
            return this;
        }
        
        public Builder enableHostnameVerification(boolean enable) {
            this.enableHostnameVerification = enable;
            return this;
        }
        
        public Builder sslEndpointIdentificationAlgorithm(String algorithm) {
            this.sslEndpointIdentificationAlgorithm = algorithm;
            return this;
        }
        
        public KafkaSecurityConfig build() {
            return new KafkaSecurityConfig(this);
        }
    }
    
    public static class SecuritySummary {
        private final SecurityProtocol securityProtocol;
        private final SaslMechanism saslMechanism;
        private final boolean hasKeystore;
        private final boolean hasTruststore;
        private final boolean hostnameVerificationEnabled;
        private final String saslUsername;
        
        public SecuritySummary(SecurityProtocol securityProtocol, SaslMechanism saslMechanism, 
                             boolean hasKeystore, boolean hasTruststore, 
                             boolean hostnameVerificationEnabled, String saslUsername) {
            this.securityProtocol = securityProtocol;
            this.saslMechanism = saslMechanism;
            this.hasKeystore = hasKeystore;
            this.hasTruststore = hasTruststore;
            this.hostnameVerificationEnabled = hostnameVerificationEnabled;
            this.saslUsername = saslUsername;
        }
        
        // Getters
        public SecurityProtocol getSecurityProtocol() { return securityProtocol; }
        public SaslMechanism getSaslMechanism() { return saslMechanism; }
        public boolean hasKeystore() { return hasKeystore; }
        public boolean hasTruststore() { return hasTruststore; }
        public boolean isHostnameVerificationEnabled() { return hostnameVerificationEnabled; }
        public String getSaslUsername() { return saslUsername; }
        
        @Override
        public String toString() {
            return String.format("SecuritySummary{protocol=%s, saslMechanism=%s, keystore=%s, truststore=%s, hostnameVerification=%s, saslUser=%s}",
                securityProtocol, saslMechanism, hasKeystore, hasTruststore, hostnameVerificationEnabled, saslUsername);
        }
    }
}
