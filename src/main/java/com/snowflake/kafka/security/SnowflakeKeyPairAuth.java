package com.snowflake.kafka.security;

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.bouncycastle.pkcs.PKCSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Security;
import java.security.interfaces.RSAPublicKey;
import java.util.Base64;

/**
 * Enterprise-grade Snowflake key-pair authentication utility.
 * Handles RSA key-pair generation, loading, and JWT token creation for secure Snowflake connections.
 */
public class SnowflakeKeyPairAuth {
    private static final Logger logger = LoggerFactory.getLogger(SnowflakeKeyPairAuth.class);
    
    static {
        // Add BouncyCastle provider for enhanced cryptographic support
        if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
            Security.addProvider(new BouncyCastleProvider());
        }
    }
    
    private final PrivateKey privateKey;
    private final PublicKey publicKey;
    private final String publicKeyFingerprint;
    
    /**
     * Creates SnowflakeKeyPairAuth from private key file path.
     * 
     * @param privateKeyPath Path to the private key file (PEM format)
     * @param passphrase Passphrase for encrypted private key (null if not encrypted)
     */
    public SnowflakeKeyPairAuth(String privateKeyPath, String passphrase) throws Exception {
        this.privateKey = loadPrivateKey(privateKeyPath, passphrase);
        this.publicKey = generatePublicKeyFromPrivate(privateKey);
        this.publicKeyFingerprint = generatePublicKeyFingerprint();
        
        logger.info("Snowflake key-pair authentication initialized successfully");
        logger.debug("Public key fingerprint: {}", publicKeyFingerprint);
    }
    
    /**
     * Creates SnowflakeKeyPairAuth from private key content string.
     * 
     * @param privateKeyContent Private key content in PEM format
     * @param passphrase Passphrase for encrypted private key (null if not encrypted)
     */
    public static SnowflakeKeyPairAuth fromPrivateKeyContent(String privateKeyContent, String passphrase) throws Exception {
        PrivateKey privateKey = parsePrivateKeyFromContent(privateKeyContent, passphrase);
        return new SnowflakeKeyPairAuth(privateKey);
    }
    
    private SnowflakeKeyPairAuth(PrivateKey privateKey) throws Exception {
        this.privateKey = privateKey;
        this.publicKey = generatePublicKeyFromPrivate(privateKey);
        this.publicKeyFingerprint = generatePublicKeyFingerprint();
    }
    
    /**
     * Loads private key from file path with optional passphrase.
     */
    private static PrivateKey loadPrivateKey(String privateKeyPath, String passphrase) throws Exception {
        Path keyPath = Paths.get(privateKeyPath);
        
        if (!Files.exists(keyPath)) {
            throw new IllegalArgumentException("Private key file not found: " + privateKeyPath);
        }
        
        if (!Files.isReadable(keyPath)) {
            throw new IllegalArgumentException("Private key file is not readable: " + privateKeyPath);
        }
        
        String privateKeyContent = Files.readString(keyPath);
        return parsePrivateKeyFromContent(privateKeyContent, passphrase);
    }
    
    /**
     * Generates public key from RSA private key.
     */
    private static PublicKey generatePublicKeyFromPrivate(PrivateKey privateKey) throws Exception {
        if (!(privateKey instanceof java.security.interfaces.RSAPrivateCrtKey)) {
            throw new IllegalArgumentException("Private key must be RSA CRT key");
        }
        
        java.security.interfaces.RSAPrivateCrtKey rsaPrivateKey = 
            (java.security.interfaces.RSAPrivateCrtKey) privateKey;
        
        java.security.spec.RSAPublicKeySpec publicKeySpec = new java.security.spec.RSAPublicKeySpec(
            rsaPrivateKey.getModulus(),
            rsaPrivateKey.getPublicExponent()
        );
        
        java.security.KeyFactory keyFactory = java.security.KeyFactory.getInstance("RSA");
        return keyFactory.generatePublic(publicKeySpec);
    }
    
    /**
     * Parses private key from PEM content string.
     */
    private static PrivateKey parsePrivateKeyFromContent(String privateKeyContent, String passphrase) throws Exception {
        // Clean up the private key content
        String cleanedContent = privateKeyContent
            .replaceAll("\\r\\n", "\\n")
            .replaceAll("\\r", "\\n");
        
        try (PEMParser pemParser = new PEMParser(new StringReader(cleanedContent))) {
            Object pemObject = pemParser.readObject();
            
            if (pemObject == null) {
                throw new IllegalArgumentException("No PEM object found in private key content");
            }
            
            JcaPEMKeyConverter converter = new JcaPEMKeyConverter()
                .setProvider(BouncyCastleProvider.PROVIDER_NAME);
            
            if (pemObject instanceof PKCS8EncryptedPrivateKeyInfo) {
                // Encrypted private key
                if (passphrase == null || passphrase.isEmpty()) {
                    throw new IllegalArgumentException("Passphrase required for encrypted private key");
                }
                
                PKCS8EncryptedPrivateKeyInfo encryptedPrivateKeyInfo = (PKCS8EncryptedPrivateKeyInfo) pemObject;
                InputDecryptorProvider decryptorProvider = new JceOpenSSLPKCS8DecryptorProviderBuilder()
                    .setProvider(BouncyCastleProvider.PROVIDER_NAME)
                    .build(passphrase.toCharArray());
                
                PrivateKeyInfo privateKeyInfo = encryptedPrivateKeyInfo.decryptPrivateKeyInfo(decryptorProvider);
                return converter.getPrivateKey(privateKeyInfo);
                
            } else if (pemObject instanceof PrivateKeyInfo) {
                // Unencrypted private key
                PrivateKeyInfo privateKeyInfo = (PrivateKeyInfo) pemObject;
                return converter.getPrivateKey(privateKeyInfo);
                
            } else {
                throw new IllegalArgumentException("Unsupported PEM object type: " + pemObject.getClass().getName());
            }
            
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse private key: " + e.getMessage(), e);
        } catch (OperatorCreationException | PKCSException e) {
            throw new IllegalArgumentException("Failed to decrypt private key: " + e.getMessage(), e);
        }
    }
    
    /**
     * Generates SHA-256 fingerprint of the public key for Snowflake registration.
     */
    private String generatePublicKeyFingerprint() throws Exception {
        if (publicKey == null) {
            throw new IllegalStateException("Public key is not available");
        }
        
        if (!(publicKey instanceof RSAPublicKey)) {
            throw new IllegalArgumentException("Only RSA public keys are supported");
        }
        
        RSAPublicKey rsaPublicKey = (RSAPublicKey) publicKey;
        
        // Generate the public key in the format expected by Snowflake
        String publicKeyString = String.format("-----BEGIN PUBLIC KEY-----\n%s\n-----END PUBLIC KEY-----",
            Base64.getEncoder().encodeToString(publicKey.getEncoded()));
        
        // Calculate SHA-256 fingerprint
        java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(publicKey.getEncoded());
        
        // Convert to hex string
        StringBuilder hexString = new StringBuilder();
        for (byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        
        return hexString.toString().toUpperCase();
    }
    
    /**
     * Creates JWT token for Snowflake authentication.
     */
    public String createJwtToken(String accountName, String userName) throws Exception {
        return createJwtToken(accountName, userName, 3600); // 1 hour default
    }
    
    /**
     * Creates JWT token for Snowflake authentication with custom expiration.
     */
    public String createJwtToken(String accountName, String userName, long expirationSeconds) throws Exception {
        long currentTime = System.currentTimeMillis() / 1000;
        long expirationTime = currentTime + expirationSeconds;
        
        // Create JWT header
        String header = Base64.getUrlEncoder().withoutPadding().encodeToString(
            String.format("{\"alg\":\"RS256\",\"typ\":\"JWT\"}").getBytes("UTF-8")
        );
        
        // Create JWT payload
        String payload = Base64.getUrlEncoder().withoutPadding().encodeToString(
            String.format(
                "{\"iss\":\"%s.%s\",\"sub\":\"%s.%s\",\"iat\":%d,\"exp\":%d}",
                accountName.toUpperCase(), userName.toUpperCase(),
                accountName.toUpperCase(), userName.toUpperCase(),
                currentTime, expirationTime
            ).getBytes("UTF-8")
        );
        
        // Create signature
        String headerAndPayload = header + "." + payload;
        java.security.Signature signature = java.security.Signature.getInstance("SHA256withRSA");
        signature.initSign(privateKey);
        signature.update(headerAndPayload.getBytes("UTF-8"));
        byte[] signatureBytes = signature.sign();
        
        String signatureString = Base64.getUrlEncoder().withoutPadding().encodeToString(signatureBytes);
        
        return headerAndPayload + "." + signatureString;
    }
    
    /**
     * Gets the public key in PEM format for Snowflake user registration.
     */
    public String getPublicKeyPem() {
        if (publicKey == null) {
            throw new IllegalStateException("Public key is not available");
        }
        
        String encodedKey = Base64.getEncoder().encodeToString(publicKey.getEncoded());
        return String.format("-----BEGIN PUBLIC KEY-----\n%s\n-----END PUBLIC KEY-----", encodedKey);
    }
    
    /**
     * Gets the public key fingerprint for verification.
     */
    public String getPublicKeyFingerprint() {
        return publicKeyFingerprint;
    }
    
    /**
     * Validates that the key pair is suitable for Snowflake authentication.
     */
    public boolean validateKeyPair() {
        try {
            if (privateKey == null || publicKey == null) {
                logger.error("Private key or public key is null");
                return false;
            }
            
            if (!(privateKey instanceof java.security.interfaces.RSAPrivateKey)) {
                logger.error("Private key is not RSA");
                return false;
            }
            
            if (!(publicKey instanceof RSAPublicKey)) {
                logger.error("Public key is not RSA");
                return false;
            }
            
            RSAPublicKey rsaPublicKey = (RSAPublicKey) publicKey;
            int keySize = rsaPublicKey.getModulus().bitLength();
            
            if (keySize < 2048) {
                logger.error("RSA key size {} is too small, minimum 2048 bits required", keySize);
                return false;
            }
            
            // Test signing and verification
            String testData = "test-data-for-validation";
            java.security.Signature signature = java.security.Signature.getInstance("SHA256withRSA");
            signature.initSign(privateKey);
            signature.update(testData.getBytes("UTF-8"));
            byte[] signatureBytes = signature.sign();
            
            signature.initVerify(publicKey);
            signature.update(testData.getBytes("UTF-8"));
            boolean verified = signature.verify(signatureBytes);
            
            if (!verified) {
                logger.error("Key pair validation failed: signature verification failed");
                return false;
            }
            
            logger.info("Key pair validation successful: RSA-{} key", keySize);
            return true;
            
        } catch (Exception e) {
            logger.error("Key pair validation failed", e);
            return false;
        }
    }
    
    /**
     * Utility method to generate a new RSA key pair for Snowflake.
     * This is typically done offline and the keys stored securely.
     */
    public static java.security.KeyPair generateKeyPair(int keySize) throws Exception {
        if (keySize < 2048) {
            throw new IllegalArgumentException("Key size must be at least 2048 bits");
        }
        
        java.security.KeyPairGenerator keyPairGenerator = java.security.KeyPairGenerator.getInstance("RSA");
        keyPairGenerator.initialize(keySize);
        return keyPairGenerator.generateKeyPair();
    }
    
    /**
     * Utility method to save a private key to PEM format.
     */
    public static String privateKeyToPem(PrivateKey privateKey) throws Exception {
        String encodedKey = Base64.getEncoder().encodeToString(privateKey.getEncoded());
        return String.format("-----BEGIN PRIVATE KEY-----\n%s\n-----END PRIVATE KEY-----", encodedKey);
    }
    
    /**
     * Gets key information for logging and debugging.
     */
    public KeyInfo getKeyInfo() {
        try {
            RSAPublicKey rsaPublicKey = (RSAPublicKey) publicKey;
            return new KeyInfo(
                privateKey.getAlgorithm(),
                rsaPublicKey.getModulus().bitLength(),
                publicKeyFingerprint,
                rsaPublicKey.getPublicExponent().toString()
            );
        } catch (Exception e) {
            logger.error("Failed to get key info", e);
            return new KeyInfo("UNKNOWN", 0, publicKeyFingerprint, "UNKNOWN");
        }
    }
    
    public static class KeyInfo {
        private final String algorithm;
        private final int keySize;
        private final String fingerprint;
        private final String publicExponent;
        
        public KeyInfo(String algorithm, int keySize, String fingerprint, String publicExponent) {
            this.algorithm = algorithm;
            this.keySize = keySize;
            this.fingerprint = fingerprint;
            this.publicExponent = publicExponent;
        }
        
        public String getAlgorithm() { return algorithm; }
        public int getKeySize() { return keySize; }
        public String getFingerprint() { return fingerprint; }
        public String getPublicExponent() { return publicExponent; }
        
        @Override
        public String toString() {
            return String.format("KeyInfo{algorithm='%s', keySize=%d, fingerprint='%s', publicExponent='%s'}",
                algorithm, keySize, fingerprint, publicExponent);
        }
    }
}
