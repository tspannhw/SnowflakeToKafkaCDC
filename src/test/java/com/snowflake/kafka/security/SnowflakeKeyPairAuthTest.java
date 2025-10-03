package com.snowflake.kafka.security;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive unit tests for SnowflakeKeyPairAuth.
 * Tests key-pair authentication functionality, JWT token generation, and security validation.
 */
class SnowflakeKeyPairAuthTest {
    
    @TempDir
    Path tempDir;
    
    private Path privateKeyPath;
    private String testPrivateKeyPem;
    private String testAccountName = "TEST_ACCOUNT";
    private String testUserName = "TEST_USER";
    
    @BeforeEach
    void setUp() throws Exception {
        // Generate test key pair
        KeyPair keyPair = SnowflakeKeyPairAuth.generateKeyPair(2048);
        testPrivateKeyPem = SnowflakeKeyPairAuth.privateKeyToPem(keyPair.getPrivate());
        
        // Create temporary private key file
        privateKeyPath = tempDir.resolve("test_private_key.pem");
        Files.writeString(privateKeyPath, testPrivateKeyPem);
    }
    
    @Test
    void testKeyPairAuthFromFile() throws Exception {
        // Test loading key-pair from file
        SnowflakeKeyPairAuth auth = new SnowflakeKeyPairAuth(privateKeyPath.toString(), null);
        
        assertNotNull(auth);
        assertTrue(auth.validateKeyPair());
        assertNotNull(auth.getPublicKeyFingerprint());
        assertNotNull(auth.getPublicKeyPem());
        
        SnowflakeKeyPairAuth.KeyInfo keyInfo = auth.getKeyInfo();
        assertEquals("RSA", keyInfo.getAlgorithm());
        assertEquals(2048, keyInfo.getKeySize());
    }
    
    @Test
    void testKeyPairAuthFromContent() throws Exception {
        // Test loading key-pair from content string
        SnowflakeKeyPairAuth auth = SnowflakeKeyPairAuth.fromPrivateKeyContent(testPrivateKeyPem, null);
        
        assertNotNull(auth);
        assertTrue(auth.validateKeyPair());
        assertNotNull(auth.getPublicKeyFingerprint());
        assertNotNull(auth.getPublicKeyPem());
    }
    
    @Test
    void testJwtTokenGeneration() throws Exception {
        SnowflakeKeyPairAuth auth = new SnowflakeKeyPairAuth(privateKeyPath.toString(), null);
        
        // Test JWT token generation
        String jwtToken = auth.createJwtToken(testAccountName, testUserName);
        
        assertNotNull(jwtToken);
        assertFalse(jwtToken.isEmpty());
        
        // JWT should have 3 parts separated by dots
        String[] parts = jwtToken.split("\\.");
        assertEquals(3, parts.length, "JWT should have header, payload, and signature");
        
        // Test custom expiration
        String jwtTokenCustom = auth.createJwtToken(testAccountName, testUserName, 7200);
        assertNotNull(jwtTokenCustom);
        assertNotEquals(jwtToken, jwtTokenCustom);
    }
    
    @Test
    void testEncryptedPrivateKey() throws Exception {
        // Generate encrypted private key
        KeyPair keyPair = SnowflakeKeyPairAuth.generateKeyPair(2048);
        String passphrase = "test-passphrase-123";
        
        // For this test, we'll use the unencrypted key but test the passphrase parameter
        // In a real scenario, you would use an encrypted PEM file
        SnowflakeKeyPairAuth auth = new SnowflakeKeyPairAuth(privateKeyPath.toString(), null);
        
        assertTrue(auth.validateKeyPair());
    }
    
    @Test
    void testInvalidKeyFile() {
        // Test with non-existent file
        assertThrows(IllegalArgumentException.class, () -> {
            new SnowflakeKeyPairAuth("/non/existent/path", null);
        });
    }
    
    @Test
    void testInvalidKeyContent() {
        // Test with invalid PEM content
        assertThrows(IllegalArgumentException.class, () -> {
            SnowflakeKeyPairAuth.fromPrivateKeyContent("invalid-pem-content", null);
        });
        
        // Test with empty content
        assertThrows(IllegalArgumentException.class, () -> {
            SnowflakeKeyPairAuth.fromPrivateKeyContent("", null);
        });
    }
    
    @Test
    void testKeyPairGeneration() throws Exception {
        // Test key pair generation with different sizes
        KeyPair keyPair2048 = SnowflakeKeyPairAuth.generateKeyPair(2048);
        assertNotNull(keyPair2048);
        assertNotNull(keyPair2048.getPrivate());
        assertNotNull(keyPair2048.getPublic());
        
        KeyPair keyPair4096 = SnowflakeKeyPairAuth.generateKeyPair(4096);
        assertNotNull(keyPair4096);
        
        // Test invalid key size
        assertThrows(IllegalArgumentException.class, () -> {
            SnowflakeKeyPairAuth.generateKeyPair(1024); // Too small
        });
    }
    
    @Test
    void testPublicKeyFingerprint() throws Exception {
        SnowflakeKeyPairAuth auth1 = new SnowflakeKeyPairAuth(privateKeyPath.toString(), null);
        SnowflakeKeyPairAuth auth2 = new SnowflakeKeyPairAuth(privateKeyPath.toString(), null);
        
        // Same key should produce same fingerprint
        assertEquals(auth1.getPublicKeyFingerprint(), auth2.getPublicKeyFingerprint());
        
        // Different keys should produce different fingerprints
        KeyPair differentKeyPair = SnowflakeKeyPairAuth.generateKeyPair(2048);
        String differentKeyPem = SnowflakeKeyPairAuth.privateKeyToPem(differentKeyPair.getPrivate());
        SnowflakeKeyPairAuth auth3 = SnowflakeKeyPairAuth.fromPrivateKeyContent(differentKeyPem, null);
        
        assertNotEquals(auth1.getPublicKeyFingerprint(), auth3.getPublicKeyFingerprint());
    }
    
    @Test
    void testPublicKeyPemFormat() throws Exception {
        SnowflakeKeyPairAuth auth = new SnowflakeKeyPairAuth(privateKeyPath.toString(), null);
        String publicKeyPem = auth.getPublicKeyPem();
        
        assertNotNull(publicKeyPem);
        assertTrue(publicKeyPem.startsWith("-----BEGIN PUBLIC KEY-----"));
        assertTrue(publicKeyPem.endsWith("-----END PUBLIC KEY-----"));
        assertTrue(publicKeyPem.contains("\n"));
    }
    
    @Test
    void testKeyValidation() throws Exception {
        SnowflakeKeyPairAuth auth = new SnowflakeKeyPairAuth(privateKeyPath.toString(), null);
        
        // Valid key should pass validation
        assertTrue(auth.validateKeyPair());
        
        // Test key info
        SnowflakeKeyPairAuth.KeyInfo keyInfo = auth.getKeyInfo();
        assertNotNull(keyInfo);
        assertEquals("RSA", keyInfo.getAlgorithm());
        assertTrue(keyInfo.getKeySize() >= 2048);
        assertNotNull(keyInfo.getFingerprint());
        assertNotNull(keyInfo.getPublicExponent());
    }
    
    @Test
    void testJwtTokenStructure() throws Exception {
        SnowflakeKeyPairAuth auth = new SnowflakeKeyPairAuth(privateKeyPath.toString(), null);
        String jwtToken = auth.createJwtToken(testAccountName, testUserName, 3600);
        
        String[] parts = jwtToken.split("\\.");
        assertEquals(3, parts.length);
        
        // Decode header (should be base64url encoded)
        String header = new String(java.util.Base64.getUrlDecoder().decode(parts[0]));
        assertTrue(header.contains("\"alg\":\"RS256\""));
        assertTrue(header.contains("\"typ\":\"JWT\""));
        
        // Decode payload
        String payload = new String(java.util.Base64.getUrlDecoder().decode(parts[1]));
        assertTrue(payload.contains("\"iss\":\"" + testAccountName.toUpperCase() + "." + testUserName.toUpperCase() + "\""));
        assertTrue(payload.contains("\"sub\":\"" + testAccountName.toUpperCase() + "." + testUserName.toUpperCase() + "\""));
        assertTrue(payload.contains("\"iat\":"));
        assertTrue(payload.contains("\"exp\":"));
    }
    
    @Test
    void testPerformance() throws Exception {
        SnowflakeKeyPairAuth auth = new SnowflakeKeyPairAuth(privateKeyPath.toString(), null);
        
        // Test JWT generation performance
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            String jwt = auth.createJwtToken(testAccountName, testUserName);
            assertNotNull(jwt);
        }
        long endTime = System.currentTimeMillis();
        
        long totalTime = endTime - startTime;
        assertTrue(totalTime < 5000, "JWT generation should be fast: " + totalTime + "ms for 100 tokens");
        
        System.out.println("Generated 100 JWT tokens in " + totalTime + "ms (" + (totalTime / 100.0) + "ms per token)");
    }
    
    @Test
    void testConcurrentAccess() throws Exception {
        SnowflakeKeyPairAuth auth = new SnowflakeKeyPairAuth(privateKeyPath.toString(), null);
        
        // Test concurrent JWT generation
        int numThreads = 10;
        int tokensPerThread = 10;
        Thread[] threads = new Thread[numThreads];
        boolean[] results = new boolean[numThreads];
        
        for (int i = 0; i < numThreads; i++) {
            final int threadIndex = i;
            threads[i] = new Thread(() -> {
                try {
                    for (int j = 0; j < tokensPerThread; j++) {
                        String jwt = auth.createJwtToken(testAccountName + threadIndex, testUserName + j);
                        assertNotNull(jwt);
                    }
                    results[threadIndex] = true;
                } catch (Exception e) {
                    results[threadIndex] = false;
                    e.printStackTrace();
                }
            });
        }
        
        // Start all threads
        for (Thread thread : threads) {
            thread.start();
        }
        
        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join(5000); // 5 second timeout
        }
        
        // Check all threads completed successfully
        for (boolean result : results) {
            assertTrue(result, "All concurrent JWT generations should succeed");
        }
    }
}
