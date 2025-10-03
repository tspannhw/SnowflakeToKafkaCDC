package com.snowflake.kafka.producer;

import com.snowflake.kafka.config.AppConfig;
import com.snowflake.kafka.metrics.MetricsCollector;
import com.snowflake.kafka.model.StreamRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * High-performance Kafka producer optimized for maximum throughput.
 * Features async batching, connection pooling, and comprehensive error handling.
 */
public class OptimizedKafkaProducer implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(OptimizedKafkaProducer.class);
    
    private final AppConfig.KafkaConfig kafkaConfig;
    private final AppConfig.PerformanceConfig performanceConfig;
    private final AppConfig.ErrorHandlingConfig errorConfig;
    private final MetricsCollector metricsCollector;
    
    private final KafkaProducer<String, String> producer;
    private final ExecutorService executorService;
    private final ScheduledExecutorService scheduledExecutor;
    
    // Batching support
    private final BlockingQueue<StreamRecord> recordQueue;
    private final List<StreamRecord> currentBatch;
    private final ReentrantLock batchLock = new ReentrantLock();
    private volatile Instant lastBatchTime = Instant.now();
    
    // Metrics
    private final AtomicLong recordsSent = new AtomicLong(0);
    private final AtomicLong bytesSent = new AtomicLong(0);
    private final AtomicLong sendErrors = new AtomicLong(0);
    private final AtomicLong batchesSent = new AtomicLong(0);
    
    // Circuit breaker state
    private volatile boolean circuitBreakerOpen = false;
    private volatile Instant circuitBreakerOpenTime;
    private final AtomicLong consecutiveFailures = new AtomicLong(0);
    
    private volatile boolean running = false;
    
    public OptimizedKafkaProducer(AppConfig.KafkaConfig kafkaConfig,
                                AppConfig.PerformanceConfig performanceConfig,
                                AppConfig.ErrorHandlingConfig errorConfig,
                                MetricsCollector metricsCollector) {
        this.kafkaConfig = kafkaConfig;
        this.performanceConfig = performanceConfig;
        this.errorConfig = errorConfig;
        this.metricsCollector = metricsCollector;
        
        // Initialize Kafka producer with optimized settings
        this.producer = new KafkaProducer<>(kafkaConfig.getProducerProperties());
        
        // Initialize thread pools
        this.executorService = new ThreadPoolExecutor(
            performanceConfig.getCoreThreads(),
            performanceConfig.getMaxThreads(),
            performanceConfig.getKeepAliveSeconds(),
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(performanceConfig.getQueueCapacity()),
            new ThreadFactory() {
                private int counter = 0;
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "kafka-producer-" + (++counter));
                    t.setDaemon(true);
                    return t;
                }
            }
        );
        
        this.scheduledExecutor = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "kafka-producer-scheduler");
            t.setDaemon(true);
            return t;
        });
        
        // Initialize batching components
        this.recordQueue = new LinkedBlockingQueue<>(performanceConfig.getAsyncQueueSize());
        this.currentBatch = new ArrayList<>();
        
        // Start background processing
        startBackgroundProcessing();
        
        logger.info("OptimizedKafkaProducer initialized with batching={}, async={}", 
            performanceConfig.isEnableBatching(), performanceConfig.isAsyncProcessing());
    }
    
    private void startBackgroundProcessing() {
        running = true;
        
        // Start batch processor
        if (performanceConfig.isEnableBatching()) {
            executorService.submit(this::processBatches);
            
            // Start batch timeout handler
            scheduledExecutor.scheduleAtFixedRate(
                this::flushTimedOutBatch,
                performanceConfig.getBatchTimeoutMs(),
                performanceConfig.getBatchTimeoutMs(),
                TimeUnit.MILLISECONDS
            );
        }
        
        // Start async record processor
        if (performanceConfig.isAsyncProcessing()) {
            for (int i = 0; i < performanceConfig.getCoreThreads(); i++) {
                executorService.submit(this::processAsyncRecords);
            }
        }
        
        // Start circuit breaker recovery checker
        if (errorConfig.isEnableCircuitBreaker()) {
            scheduledExecutor.scheduleAtFixedRate(
                this::checkCircuitBreakerRecovery,
                5000, 5000, TimeUnit.MILLISECONDS
            );
        }
        
        // Start metrics reporter
        scheduledExecutor.scheduleAtFixedRate(
            this::reportMetrics,
            30000, 30000, TimeUnit.MILLISECONDS
        );
    }
    
    /**
     * Sends a single record asynchronously with optimal performance.
     */
    public CompletableFuture<RecordMetadata> sendAsync(StreamRecord record) {
        if (!running) {
            return CompletableFuture.failedFuture(new IllegalStateException("Producer is not running"));
        }
        
        if (circuitBreakerOpen) {
            return CompletableFuture.failedFuture(new RuntimeException("Circuit breaker is open"));
        }
        
        if (performanceConfig.isAsyncProcessing()) {
            // Queue for async processing
            if (!recordQueue.offer(record)) {
                metricsCollector.incrementQueueFullErrors();
                return CompletableFuture.failedFuture(new RuntimeException("Async queue is full"));
            }
            return CompletableFuture.completedFuture(null); // Async, no immediate result
        } else {
            // Direct send
            return sendRecordDirect(record);
        }
    }
    
    /**
     * Sends a batch of records with optimized batching.
     */
    public CompletableFuture<List<RecordMetadata>> sendBatch(List<StreamRecord> records) {
        if (!running || circuitBreakerOpen) {
            return CompletableFuture.failedFuture(new RuntimeException("Producer not available"));
        }
        
        if (performanceConfig.isEnableBatching()) {
            return addToBatch(records);
        } else {
            // Send all records individually
            List<CompletableFuture<RecordMetadata>> futures = records.stream()
                .map(this::sendRecordDirect)
                .toList();
            
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> futures.stream()
                    .map(CompletableFuture::join)
                    .toList());
        }
    }
    
    private CompletableFuture<RecordMetadata> sendRecordDirect(StreamRecord record) {
        CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
        
        try {
            String topic = kafkaConfig.getTopicsConfig().getTopicForStream(record.getStreamName());
            String key = record.generateKafkaKey();
            String value = record.toJson();
            
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
            
            producer.send(producerRecord, (metadata, exception) -> {
                if (exception != null) {
                    handleSendError(record, exception);
                    future.completeExceptionally(exception);
                } else {
                    handleSendSuccess(record, metadata);
                    future.complete(metadata);
                }
            });
            
        } catch (Exception e) {
            handleSendError(record, e);
            future.completeExceptionally(e);
        }
        
        return future;
    }
    
    private CompletableFuture<List<RecordMetadata>> addToBatch(List<StreamRecord> records) {
        CompletableFuture<List<RecordMetadata>> future = new CompletableFuture<>();
        
        batchLock.lock();
        try {
            currentBatch.addAll(records);
            
            // Check if batch is ready to send
            if (currentBatch.size() >= kafkaConfig.getProducerConfig().getBatchSize()) {
                List<StreamRecord> batchToSend = new ArrayList<>(currentBatch);
                currentBatch.clear();
                lastBatchTime = Instant.now();
                
                // Send batch asynchronously
                executorService.submit(() -> sendBatchDirect(batchToSend, future));
            } else {
                // Batch not ready, will be sent by timeout handler
                future.complete(new ArrayList<>()); // Empty result for now
            }
        } finally {
            batchLock.unlock();
        }
        
        return future;
    }
    
    private void sendBatchDirect(List<StreamRecord> records, CompletableFuture<List<RecordMetadata>> future) {
        List<CompletableFuture<RecordMetadata>> futures = new ArrayList<>();
        
        for (StreamRecord record : records) {
            futures.add(sendRecordDirect(record));
        }
        
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .whenComplete((v, throwable) -> {
                if (throwable != null) {
                    future.completeExceptionally(throwable);
                } else {
                    List<RecordMetadata> results = futures.stream()
                        .map(CompletableFuture::join)
                        .toList();
                    future.complete(results);
                }
                
                batchesSent.incrementAndGet();
                metricsCollector.recordBatchSent(records.size());
            });
    }
    
    private void processBatches() {
        while (running) {
            try {
                batchLock.lock();
                try {
                    if (!currentBatch.isEmpty() && 
                        Duration.between(lastBatchTime, Instant.now()).toMillis() >= performanceConfig.getBatchTimeoutMs()) {
                        
                        List<StreamRecord> batchToSend = new ArrayList<>(currentBatch);
                        currentBatch.clear();
                        lastBatchTime = Instant.now();
                        
                        sendBatchDirect(batchToSend, CompletableFuture.completedFuture(new ArrayList<>()));
                    }
                } finally {
                    batchLock.unlock();
                }
                
                Thread.sleep(10); // Small delay to prevent busy waiting
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error in batch processor", e);
            }
        }
    }
    
    private void flushTimedOutBatch() {
        if (!running) return;
        
        batchLock.lock();
        try {
            if (!currentBatch.isEmpty() && 
                Duration.between(lastBatchTime, Instant.now()).toMillis() >= performanceConfig.getBatchTimeoutMs()) {
                
                List<StreamRecord> batchToSend = new ArrayList<>(currentBatch);
                currentBatch.clear();
                lastBatchTime = Instant.now();
                
                executorService.submit(() -> 
                    sendBatchDirect(batchToSend, CompletableFuture.completedFuture(new ArrayList<>())));
            }
        } finally {
            batchLock.unlock();
        }
    }
    
    private void processAsyncRecords() {
        while (running) {
            try {
                StreamRecord record = recordQueue.poll(1, TimeUnit.SECONDS);
                if (record != null) {
                    sendRecordDirect(record);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error processing async record", e);
            }
        }
    }
    
    private void handleSendSuccess(StreamRecord record, RecordMetadata metadata) {
        recordsSent.incrementAndGet();
        bytesSent.addAndGet(record.getEstimatedSizeBytes());
        consecutiveFailures.set(0);
        
        metricsCollector.recordMessageSent(
            metadata.topic(), 
            metadata.partition(), 
            record.getEstimatedSizeBytes()
        );
        
        logger.debug("Successfully sent record to topic {} partition {} offset {}", 
            metadata.topic(), metadata.partition(), metadata.offset());
    }
    
    private void handleSendError(StreamRecord record, Exception exception) {
        sendErrors.incrementAndGet();
        long failures = consecutiveFailures.incrementAndGet();
        
        metricsCollector.incrementSendErrors();
        
        logger.error("Failed to send record {}: {}", record.toCompactString(), exception.getMessage());
        
        // Check circuit breaker
        if (errorConfig.isEnableCircuitBreaker() && 
            failures >= errorConfig.getFailureThreshold()) {
            openCircuitBreaker();
        }
        
        // Handle retries for retriable exceptions
        if (exception instanceof RetriableException && 
            failures <= errorConfig.getMaxRetries()) {
            scheduleRetry(record, (int) failures);
        } else if (errorConfig.isEnableDlq()) {
            sendToDeadLetterQueue(record, exception);
        }
    }
    
    private void scheduleRetry(StreamRecord record, int attemptNumber) {
        long delay = calculateRetryDelay(attemptNumber);
        
        scheduledExecutor.schedule(() -> {
            logger.info("Retrying send for record {} (attempt {})", record.toCompactString(), attemptNumber);
            sendRecordDirect(record);
        }, delay, TimeUnit.MILLISECONDS);
    }
    
    private long calculateRetryDelay(int attemptNumber) {
        long baseDelay = errorConfig.getRetryDelayMs();
        
        if (errorConfig.isExponentialBackoff()) {
            long exponentialDelay = baseDelay * (1L << (attemptNumber - 1));
            return Math.min(exponentialDelay, errorConfig.getMaxRetryDelayMs());
        } else {
            return baseDelay;
        }
    }
    
    private void sendToDeadLetterQueue(StreamRecord record, Exception exception) {
        try {
            // Add error information to metadata
            record.addMetadata("error_message", exception.getMessage());
            record.addMetadata("error_timestamp", Instant.now().toString());
            record.addMetadata("original_topic", 
                kafkaConfig.getTopicsConfig().getTopicForStream(record.getStreamName()));
            
            ProducerRecord<String, String> dlqRecord = new ProducerRecord<>(
                errorConfig.getDlqTopic(),
                record.generateKafkaKey(),
                record.toJson()
            );
            
            producer.send(dlqRecord, (metadata, dlqException) -> {
                if (dlqException != null) {
                    logger.error("Failed to send record to DLQ: {}", dlqException.getMessage());
                    metricsCollector.incrementDlqErrors();
                } else {
                    logger.info("Sent failed record to DLQ: {}", record.toCompactString());
                    metricsCollector.incrementDlqMessages();
                }
            });
            
        } catch (Exception e) {
            logger.error("Failed to send record to DLQ", e);
            metricsCollector.incrementDlqErrors();
        }
    }
    
    private void openCircuitBreaker() {
        circuitBreakerOpen = true;
        circuitBreakerOpenTime = Instant.now();
        logger.warn("Circuit breaker opened due to {} consecutive failures", consecutiveFailures.get());
        metricsCollector.incrementCircuitBreakerOpens();
    }
    
    private void checkCircuitBreakerRecovery() {
        if (circuitBreakerOpen && circuitBreakerOpenTime != null) {
            long timeSinceOpen = Duration.between(circuitBreakerOpenTime, Instant.now()).toMillis();
            
            if (timeSinceOpen >= errorConfig.getRecoveryTimeoutMs()) {
                circuitBreakerOpen = false;
                circuitBreakerOpenTime = null;
                consecutiveFailures.set(0);
                logger.info("Circuit breaker closed after recovery timeout");
                metricsCollector.incrementCircuitBreakerCloses();
            }
        }
    }
    
    private void reportMetrics() {
        ProducerStats stats = getStats();
        logger.info("Producer stats: records={}, bytes={}, errors={}, batches={}, queue={}",
            stats.getRecordsSent(), stats.getBytesSent(), stats.getSendErrors(), 
            stats.getBatchesSent(), stats.getQueueSize());
    }
    
    /**
     * Forces flush of any pending batches.
     */
    public void flush() {
        batchLock.lock();
        try {
            if (!currentBatch.isEmpty()) {
                List<StreamRecord> batchToSend = new ArrayList<>(currentBatch);
                currentBatch.clear();
                lastBatchTime = Instant.now();
                
                sendBatchDirect(batchToSend, CompletableFuture.completedFuture(new ArrayList<>()));
            }
        } finally {
            batchLock.unlock();
        }
        
        producer.flush();
    }
    
    /**
     * Gets current producer statistics.
     */
    public ProducerStats getStats() {
        return new ProducerStats(
            recordsSent.get(),
            bytesSent.get(),
            sendErrors.get(),
            batchesSent.get(),
            recordQueue.size(),
            circuitBreakerOpen,
            consecutiveFailures.get()
        );
    }
    
    @Override
    public void close() {
        logger.info("Closing OptimizedKafkaProducer");
        
        running = false;
        
        // Flush any remaining records
        flush();
        
        // Shutdown executors
        scheduledExecutor.shutdown();
        executorService.shutdown();
        
        try {
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
            if (!scheduledExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduledExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            scheduledExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        // Close Kafka producer
        producer.close(Duration.ofSeconds(30));
        
        logger.info("OptimizedKafkaProducer closed. Total records sent: {}, errors: {}", 
            recordsSent.get(), sendErrors.get());
    }
    
    public static class ProducerStats {
        private final long recordsSent;
        private final long bytesSent;
        private final long sendErrors;
        private final long batchesSent;
        private final int queueSize;
        private final boolean circuitBreakerOpen;
        private final long consecutiveFailures;
        
        public ProducerStats(long recordsSent, long bytesSent, long sendErrors, 
                           long batchesSent, int queueSize, boolean circuitBreakerOpen, 
                           long consecutiveFailures) {
            this.recordsSent = recordsSent;
            this.bytesSent = bytesSent;
            this.sendErrors = sendErrors;
            this.batchesSent = batchesSent;
            this.queueSize = queueSize;
            this.circuitBreakerOpen = circuitBreakerOpen;
            this.consecutiveFailures = consecutiveFailures;
        }
        
        // Getters
        public long getRecordsSent() { return recordsSent; }
        public long getBytesSent() { return bytesSent; }
        public long getSendErrors() { return sendErrors; }
        public long getBatchesSent() { return batchesSent; }
        public int getQueueSize() { return queueSize; }
        public boolean isCircuitBreakerOpen() { return circuitBreakerOpen; }
        public long getConsecutiveFailures() { return consecutiveFailures; }
    }
}
