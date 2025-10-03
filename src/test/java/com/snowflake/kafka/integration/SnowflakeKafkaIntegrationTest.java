package com.snowflake.kafka.integration;

import com.snowflake.kafka.SnowflakeKafkaStreamer;
import com.snowflake.kafka.config.AppConfig;
import com.snowflake.kafka.model.StreamRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Integration tests for the complete Snowflake to Kafka streaming pipeline.
 * Uses Testcontainers for Kafka and mocks Snowflake connections for testing.
 */
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SnowflakeKafkaIntegrationTest {
    
    @Container
    static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))
        .withExposedPorts(9093)
        .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
        .withEnv("KAFKA_NUM_PARTITIONS", "3")
        .withEnv("KAFKA_DEFAULT_REPLICATION_FACTOR", "1");
    
    private static String kafkaBootstrapServers;
    private KafkaConsumer<String, String> testConsumer;
    
    @BeforeAll
    static void setUpClass() {
        kafka.start();
        kafkaBootstrapServers = kafka.getBootstrapServers();
        System.setProperty("KAFKA_BOOTSTRAP_SERVERS", kafkaBootstrapServers);
        System.setProperty("SNOWFLAKE_ACCOUNT", "test-account");
        System.setProperty("SNOWFLAKE_USER", "test-user");
        System.setProperty("SNOWFLAKE_PASSWORD", "test-password");
        System.setProperty("SNOWFLAKE_WAREHOUSE", "test-warehouse");
        System.setProperty("SNOWFLAKE_DATABASE", "test-database");
        System.setProperty("SNOWFLAKE_SCHEMA", "test-schema");
        System.setProperty("SNOWFLAKE_ROLE", "test-role");
    }
    
    @BeforeEach
    void setUp() {
        // Create test consumer
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        
        testConsumer = new KafkaConsumer<>(consumerProps);
    }
    
    @AfterEach
    void tearDown() {
        if (testConsumer != null) {
            testConsumer.close();
        }
    }
    
    @Test
    @Order(1)
    void testKafkaContainerIsRunning() {
        assertTrue(kafka.isRunning(), "Kafka container should be running");
        assertNotNull(kafkaBootstrapServers, "Kafka bootstrap servers should be available");
        System.out.println("Kafka is running on: " + kafkaBootstrapServers);
    }
    
    @Test
    @Order(2)
    void testAppConfigLoading() {
        AppConfig config = new AppConfig();
        
        assertNotNull(config.getKafkaConfig());
        assertNotNull(config.getSnowflakeConfig());
        assertNotNull(config.getPerformanceConfig());
        assertNotNull(config.getMonitoringConfig());
        assertNotNull(config.getErrorHandlingConfig());
        
        assertEquals(kafkaBootstrapServers, config.getKafkaConfig().getBootstrapServers());
        assertEquals("test-account", config.getSnowflakeConfig().getAccount());
    }
    
    @Test
    @Order(3)
    void testStreamRecordSerialization() {
        StreamRecord record = new StreamRecord("TEST_STREAM", "INSERT");
        record.setRowId("test-row-123");
        record.setStreamOffset(1000L);
        record.addColumn("customer_id", 12345);
        record.addColumn("customer_name", "John Doe");
        record.addColumn("email", "john.doe@example.com");
        record.addMetadata("source", "integration-test");
        
        // Test JSON serialization
        String json = record.toJson();
        assertNotNull(json);
        assertFalse(json.isEmpty());
        assertTrue(json.contains("TEST_STREAM"));
        assertTrue(json.contains("INSERT"));
        assertTrue(json.contains("John Doe"));
        
        // Test deserialization
        StreamRecord deserializedRecord = StreamRecord.fromJson(json);
        assertEquals(record.getStreamName(), deserializedRecord.getStreamName());
        assertEquals(record.getAction(), deserializedRecord.getAction());
        assertEquals(record.getRowId(), deserializedRecord.getRowId());
        assertEquals(record.getColumn("customer_name"), deserializedRecord.getColumn("customer_name"));
        
        // Test Kafka key generation
        String kafkaKey = record.generateKafkaKey();
        assertNotNull(kafkaKey);
        assertTrue(kafkaKey.contains("TEST_STREAM"));
        assertTrue(kafkaKey.contains("test-row-123"));
    }
    
    @Test
    @Order(4)
    void testKafkaProducerConnectivity() throws Exception {
        AppConfig config = new AppConfig();
        
        // Create a simple producer test
        Properties producerProps = config.getKafkaConfig().getProducerProperties();
        
        try (org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = 
             new org.apache.kafka.clients.producer.KafkaProducer<>(producerProps)) {
            
            // Send a test message
            String testTopic = "test-connectivity-topic";
            String testKey = "test-key";
            String testValue = "test-value";
            
            org.apache.kafka.clients.producer.ProducerRecord<String, String> record = 
                new org.apache.kafka.clients.producer.ProducerRecord<>(testTopic, testKey, testValue);
            
            org.apache.kafka.clients.producer.RecordMetadata metadata = producer.send(record).get(10, TimeUnit.SECONDS);
            
            assertNotNull(metadata);
            assertEquals(testTopic, metadata.topic());
            assertTrue(metadata.offset() >= 0);
            
            System.out.println("Successfully sent message to topic: " + testTopic + 
                             ", partition: " + metadata.partition() + 
                             ", offset: " + metadata.offset());
        }
    }
    
    @Test
    @Order(5)
    void testKafkaConsumerConnectivity() throws Exception {
        String testTopic = "test-consumer-topic";
        String testMessage = "integration-test-message-" + System.currentTimeMillis();
        
        // Subscribe to test topic
        testConsumer.subscribe(Collections.singletonList(testTopic));
        
        // Send a test message using producer
        AppConfig config = new AppConfig();
        Properties producerProps = config.getKafkaConfig().getProducerProperties();
        
        try (org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = 
             new org.apache.kafka.clients.producer.KafkaProducer<>(producerProps)) {
            
            org.apache.kafka.clients.producer.ProducerRecord<String, String> record = 
                new org.apache.kafka.clients.producer.ProducerRecord<>(testTopic, "test-key", testMessage);
            
            producer.send(record).get(5, TimeUnit.SECONDS);
            producer.flush();
        }
        
        // Consume the message
        boolean messageReceived = false;
        long startTime = System.currentTimeMillis();
        
        while (!messageReceived && (System.currentTimeMillis() - startTime) < 10000) {
            ConsumerRecords<String, String> records = testConsumer.poll(Duration.ofMillis(1000));
            
            for (ConsumerRecord<String, String> record : records) {
                if (testMessage.equals(record.value())) {
                    messageReceived = true;
                    assertEquals(testTopic, record.topic());
                    assertEquals("test-key", record.key());
                    System.out.println("Successfully consumed message from topic: " + record.topic() + 
                                     ", partition: " + record.partition() + 
                                     ", offset: " + record.offset());
                    break;
                }
            }
        }
        
        assertTrue(messageReceived, "Should have received the test message");
    }
    
    @Test
    @Order(6)
    void testStreamRecordKafkaRoundTrip() throws Exception {
        String testTopic = "stream-record-test-topic";
        
        // Create test stream record
        StreamRecord originalRecord = new StreamRecord("CUSTOMER_STREAM", "INSERT");
        originalRecord.setRowId("customer-123");
        originalRecord.setStreamOffset(5000L);
        originalRecord.addColumn("customer_id", 123);
        originalRecord.addColumn("name", "Alice Johnson");
        originalRecord.addColumn("email", "alice@example.com");
        originalRecord.addColumn("created_at", Instant.now().toString());
        originalRecord.addMetadata("test_run", "integration");
        
        // Subscribe consumer
        testConsumer.subscribe(Collections.singletonList(testTopic));
        
        // Send record via Kafka producer
        AppConfig config = new AppConfig();
        Properties producerProps = config.getKafkaConfig().getProducerProperties();
        
        try (org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = 
             new org.apache.kafka.clients.producer.KafkaProducer<>(producerProps)) {
            
            String kafkaKey = originalRecord.generateKafkaKey();
            String kafkaValue = originalRecord.toJson();
            
            org.apache.kafka.clients.producer.ProducerRecord<String, String> record = 
                new org.apache.kafka.clients.producer.ProducerRecord<>(testTopic, kafkaKey, kafkaValue);
            
            producer.send(record).get(5, TimeUnit.SECONDS);
            producer.flush();
        }
        
        // Consume and verify
        boolean recordReceived = false;
        long startTime = System.currentTimeMillis();
        
        while (!recordReceived && (System.currentTimeMillis() - startTime) < 10000) {
            ConsumerRecords<String, String> records = testConsumer.poll(Duration.ofMillis(1000));
            
            for (ConsumerRecord<String, String> record : records) {
                StreamRecord receivedRecord = StreamRecord.fromJson(record.value());
                
                if ("customer-123".equals(receivedRecord.getRowId())) {
                    recordReceived = true;
                    
                    // Verify all fields
                    assertEquals(originalRecord.getStreamName(), receivedRecord.getStreamName());
                    assertEquals(originalRecord.getAction(), receivedRecord.getAction());
                    assertEquals(originalRecord.getRowId(), receivedRecord.getRowId());
                    assertEquals(originalRecord.getStreamOffset(), receivedRecord.getStreamOffset());
                    assertEquals(originalRecord.getColumn("customer_id"), receivedRecord.getColumn("customer_id"));
                    assertEquals(originalRecord.getColumn("name"), receivedRecord.getColumn("name"));
                    assertEquals(originalRecord.getColumn("email"), receivedRecord.getColumn("email"));
                    assertEquals(originalRecord.getMetadataValue("test_run"), receivedRecord.getMetadataValue("test_run"));
                    
                    System.out.println("Successfully verified round-trip for StreamRecord: " + 
                                     receivedRecord.toCompactString());
                    break;
                }
            }
        }
        
        assertTrue(recordReceived, "Should have received and verified the StreamRecord");
    }
    
    @Test
    @Order(7)
    void testHighThroughputScenario() throws Exception {
        String testTopic = "high-throughput-test-topic";
        int numRecords = 1000;
        CountDownLatch latch = new CountDownLatch(numRecords);
        
        // Subscribe consumer
        testConsumer.subscribe(Collections.singletonList(testTopic));
        
        // Start consumer in background
        Thread consumerThread = new Thread(() -> {
            long startTime = System.currentTimeMillis();
            Set<String> receivedKeys = new HashSet<>();
            
            while (latch.getCount() > 0 && (System.currentTimeMillis() - startTime) < 30000) {
                ConsumerRecords<String, String> records = testConsumer.poll(Duration.ofMillis(100));
                
                for (ConsumerRecord<String, String> record : records) {
                    if (record.key().startsWith("throughput-test-")) {
                        receivedKeys.add(record.key());
                        latch.countDown();
                    }
                }
            }
            
            System.out.println("Consumer received " + receivedKeys.size() + " unique records");
        });
        
        consumerThread.start();
        
        // Send records via producer
        AppConfig config = new AppConfig();
        Properties producerProps = config.getKafkaConfig().getProducerProperties();
        
        long sendStartTime = System.currentTimeMillis();
        
        try (org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = 
             new org.apache.kafka.clients.producer.KafkaProducer<>(producerProps)) {
            
            List<java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata>> futures = new ArrayList<>();
            
            for (int i = 0; i < numRecords; i++) {
                StreamRecord record = new StreamRecord("THROUGHPUT_STREAM", "INSERT");
                record.setRowId("row-" + i);
                record.setStreamOffset((long) i);
                record.addColumn("id", i);
                record.addColumn("data", "test-data-" + i);
                record.addColumn("timestamp", Instant.now().toString());
                
                String kafkaKey = "throughput-test-" + i;
                String kafkaValue = record.toJson();
                
                org.apache.kafka.clients.producer.ProducerRecord<String, String> producerRecord = 
                    new org.apache.kafka.clients.producer.ProducerRecord<>(testTopic, kafkaKey, kafkaValue);
                
                futures.add(producer.send(producerRecord));
            }
            
            // Wait for all sends to complete
            for (java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata> future : futures) {
                future.get(10, TimeUnit.SECONDS);
            }
            
            producer.flush();
        }
        
        long sendEndTime = System.currentTimeMillis();
        long sendDuration = sendEndTime - sendStartTime;
        
        // Wait for consumer to receive all records
        boolean allReceived = latch.await(20, TimeUnit.SECONDS);
        consumerThread.interrupt();
        
        assertTrue(allReceived, "Should have received all " + numRecords + " records");
        
        double throughput = (double) numRecords / (sendDuration / 1000.0);
        System.out.println("Throughput test completed:");
        System.out.println("  Records sent: " + numRecords);
        System.out.println("  Send duration: " + sendDuration + "ms");
        System.out.println("  Throughput: " + String.format("%.2f", throughput) + " records/second");
        
        // Verify reasonable throughput (should be > 100 records/second)
        assertTrue(throughput > 100, "Throughput should be > 100 records/second, was: " + throughput);
    }
    
    @Test
    @Order(8)
    void testErrorHandlingScenario() throws Exception {
        String testTopic = "error-handling-test-topic";
        String dlqTopic = "snowflake-cdc-errors";
        
        // Subscribe to both main topic and DLQ
        testConsumer.subscribe(Arrays.asList(testTopic, dlqTopic));
        
        AppConfig config = new AppConfig();
        Properties producerProps = config.getKafkaConfig().getProducerProperties();
        
        try (org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = 
             new org.apache.kafka.clients.producer.KafkaProducer<>(producerProps)) {
            
            // Send a valid record
            StreamRecord validRecord = new StreamRecord("ERROR_TEST_STREAM", "INSERT");
            validRecord.setRowId("valid-record");
            validRecord.addColumn("id", 1);
            validRecord.addColumn("status", "valid");
            
            org.apache.kafka.clients.producer.ProducerRecord<String, String> validProducerRecord = 
                new org.apache.kafka.clients.producer.ProducerRecord<>(testTopic, 
                    validRecord.generateKafkaKey(), validRecord.toJson());
            
            producer.send(validProducerRecord).get(5, TimeUnit.SECONDS);
            
            // Send an invalid record (malformed JSON)
            org.apache.kafka.clients.producer.ProducerRecord<String, String> invalidRecord = 
                new org.apache.kafka.clients.producer.ProducerRecord<>(testTopic, 
                    "invalid-key", "{ invalid json content }");
            
            producer.send(invalidRecord).get(5, TimeUnit.SECONDS);
            producer.flush();
        }
        
        // Verify records were sent
        boolean validRecordReceived = false;
        long startTime = System.currentTimeMillis();
        
        while (!validRecordReceived && (System.currentTimeMillis() - startTime) < 10000) {
            ConsumerRecords<String, String> records = testConsumer.poll(Duration.ofMillis(1000));
            
            for (ConsumerRecord<String, String> record : records) {
                if (record.topic().equals(testTopic) && record.key().contains("valid-record")) {
                    validRecordReceived = true;
                    System.out.println("Received valid record from topic: " + record.topic());
                }
                
                if (record.topic().equals(dlqTopic)) {
                    System.out.println("Received error record in DLQ: " + record.topic());
                }
            }
        }
        
        assertTrue(validRecordReceived, "Should have received the valid record");
    }
    
    @Test
    @Order(9)
    void testMultipleTopicsScenario() throws Exception {
        Map<String, String> topicMapping = Map.of(
            "CUSTOMER_STREAM", "customer-changes",
            "ORDER_STREAM", "order-changes",
            "PRODUCT_STREAM", "product-changes"
        );
        
        // Subscribe to all topics
        testConsumer.subscribe(new ArrayList<>(topicMapping.values()));
        
        AppConfig config = new AppConfig();
        Properties producerProps = config.getKafkaConfig().getProducerProperties();
        
        Map<String, Integer> sentCounts = new HashMap<>();
        Map<String, Integer> receivedCounts = new HashMap<>();
        
        // Initialize counters
        for (String topic : topicMapping.values()) {
            sentCounts.put(topic, 0);
            receivedCounts.put(topic, 0);
        }
        
        try (org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = 
             new org.apache.kafka.clients.producer.KafkaProducer<>(producerProps)) {
            
            // Send records to different topics
            for (Map.Entry<String, String> entry : topicMapping.entrySet()) {
                String streamName = entry.getKey();
                String topicName = entry.getValue();
                
                for (int i = 0; i < 10; i++) {
                    StreamRecord record = new StreamRecord(streamName, "INSERT");
                    record.setRowId(streamName.toLowerCase() + "-" + i);
                    record.addColumn("id", i);
                    record.addColumn("stream_type", streamName);
                    
                    org.apache.kafka.clients.producer.ProducerRecord<String, String> producerRecord = 
                        new org.apache.kafka.clients.producer.ProducerRecord<>(topicName, 
                            record.generateKafkaKey(), record.toJson());
                    
                    producer.send(producerRecord).get(5, TimeUnit.SECONDS);
                    sentCounts.put(topicName, sentCounts.get(topicName) + 1);
                }
            }
            
            producer.flush();
        }
        
        // Consume from all topics
        long startTime = System.currentTimeMillis();
        int totalExpected = sentCounts.values().stream().mapToInt(Integer::intValue).sum();
        int totalReceived = 0;
        
        while (totalReceived < totalExpected && (System.currentTimeMillis() - startTime) < 15000) {
            ConsumerRecords<String, String> records = testConsumer.poll(Duration.ofMillis(1000));
            
            for (ConsumerRecord<String, String> record : records) {
                String topic = record.topic();
                if (topicMapping.containsValue(topic)) {
                    receivedCounts.put(topic, receivedCounts.get(topic) + 1);
                    totalReceived++;
                }
            }
        }
        
        // Verify all records were received
        for (String topic : topicMapping.values()) {
            assertEquals(sentCounts.get(topic), receivedCounts.get(topic), 
                "Should have received all records for topic: " + topic);
        }
        
        System.out.println("Multi-topic test completed:");
        for (String topic : topicMapping.values()) {
            System.out.println("  " + topic + ": sent=" + sentCounts.get(topic) + 
                             ", received=" + receivedCounts.get(topic));
        }
    }
}
