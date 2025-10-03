package com.snowflake.kafka;

import com.snowflake.kafka.config.AppConfig;
import com.snowflake.kafka.metrics.MetricsCollector;
import com.snowflake.kafka.model.StreamRecord;
import com.snowflake.kafka.producer.OptimizedKafkaProducer;
import com.snowflake.kafka.stream.SnowflakeStreamReader;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Main application class for high-performance Snowflake to Kafka streaming.
 * Orchestrates stream reading, Kafka publishing, and monitoring.
 */
public class SnowflakeKafkaStreamer {
    private static final Logger logger = LoggerFactory.getLogger(SnowflakeKafkaStreamer.class);
    
    private final AppConfig config;
    private final MetricsCollector metricsCollector;
    private final SnowflakeStreamReader streamReader;
    private final OptimizedKafkaProducer kafkaProducer;
    private final ScheduledExecutorService scheduledExecutor;
    
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong totalRecordsProcessed = new AtomicLong(0);
    private final AtomicLong totalBytesProcessed = new AtomicLong(0);
    private final Instant startTime = Instant.now();
    
    private HttpServer metricsServer;
    private HttpServer healthServer;
    
    public SnowflakeKafkaStreamer() {
        this.config = new AppConfig();
        this.metricsCollector = new MetricsCollector();
        
        // Initialize components
        this.streamReader = new SnowflakeStreamReader(
            config.getSnowflakeConfig(), 
            metricsCollector
        );
        
        this.kafkaProducer = new OptimizedKafkaProducer(
            config.getKafkaConfig(),
            config.getPerformanceConfig(),
            config.getErrorHandlingConfig(),
            metricsCollector
        );
        
        this.scheduledExecutor = Executors.newScheduledThreadPool(4, r -> {
            Thread t = new Thread(r, "streamer-scheduler");
            t.setDaemon(true);
            return t;
        });
        
        // Setup monitoring endpoints
        setupMonitoringEndpoints();
        
        // Setup shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
        
        logger.info("SnowflakeKafkaStreamer initialized successfully");
    }
    
    /**
     * Starts the streaming process.
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            logger.info("Starting Snowflake to Kafka streaming...");
            
            try {
                // Start monitoring servers
                startMonitoringServers();
                
                // Start performance monitoring
                startPerformanceMonitoring();
                
                // Start the main streaming loop
                startStreamingLoop();
                
                logger.info("Snowflake to Kafka streaming started successfully");
                
            } catch (Exception e) {
                logger.error("Failed to start streaming", e);
                running.set(false);
                throw new RuntimeException("Failed to start streaming", e);
            }
        } else {
            logger.warn("Streaming is already running");
        }
    }
    
    /**
     * Stops the streaming process gracefully.
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            logger.info("Stopping Snowflake to Kafka streaming...");
            shutdown();
        } else {
            logger.warn("Streaming is not running");
        }
    }
    
    private void startStreamingLoop() {
        // Start continuous polling and processing
        streamReader.startPolling(this::processRecords);
        
        logger.info("Stream polling started for {} streams", 
            config.getSnowflakeConfig().getStreamsConfig().getStreamNames().size());
    }
    
    private void processRecords(List<StreamRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        
        long startTime = System.nanoTime();
        
        try {
            // Send records to Kafka asynchronously
            CompletableFuture<Void> sendFuture = kafkaProducer.sendBatch(records)
                .thenAccept(metadataList -> {
                    // Update metrics
                    totalRecordsProcessed.addAndGet(records.size());
                    long totalBytes = records.stream()
                        .mapToLong(StreamRecord::getEstimatedSizeBytes)
                        .sum();
                    totalBytesProcessed.addAndGet(totalBytes);
                    
                    long duration = System.nanoTime() - startTime;
                    metricsCollector.recordBatchProcessTime(duration);
                    
                    logger.debug("Successfully processed batch of {} records in {}ms", 
                        records.size(), TimeUnit.NANOSECONDS.toMillis(duration));
                })
                .exceptionally(throwable -> {
                    logger.error("Failed to process batch of {} records", records.size(), throwable);
                    return null;
                });
            
            // Handle completion asynchronously to avoid blocking
            sendFuture.whenComplete((result, throwable) -> {
                if (throwable != null) {
                    logger.error("Batch processing completed with error", throwable);
                }
            });
            
        } catch (Exception e) {
            logger.error("Error processing records batch", e);
        }
    }
    
    private void setupMonitoringEndpoints() {
        AppConfig.MonitoringConfig monitoringConfig = config.getMonitoringConfig();
        
        if (monitoringConfig.isEnableMetrics()) {
            try {
                // Metrics endpoint
                metricsServer = HttpServer.create(
                    new InetSocketAddress(monitoringConfig.getMetricsPort()), 0);
                
                metricsServer.createContext(monitoringConfig.getMetricsPath(), exchange -> {
                    String response = metricsCollector.getJmxMetrics();
                    exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
                    exchange.sendResponseHeaders(200, response.length());
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(response.getBytes());
                    }
                });
                
                metricsServer.setExecutor(Executors.newFixedThreadPool(2));
                
                // Health check endpoint
                healthServer = HttpServer.create(
                    new InetSocketAddress(monitoringConfig.getHealthCheckPort()), 0);
                
                healthServer.createContext(monitoringConfig.getHealthCheckPath(), exchange -> {
                    HealthStatus health = getHealthStatus();
                    String response = health.toJson();
                    int statusCode = health.isHealthy() ? 200 : 503;
                    
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(statusCode, response.length());
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(response.getBytes());
                    }
                });
                
                healthServer.setExecutor(Executors.newFixedThreadPool(2));
                
            } catch (IOException e) {
                logger.error("Failed to setup monitoring endpoints", e);
            }
        }
    }
    
    private void startMonitoringServers() {
        if (metricsServer != null) {
            metricsServer.start();
            logger.info("Metrics server started on port {}", 
                config.getMonitoringConfig().getMetricsPort());
        }
        
        if (healthServer != null) {
            healthServer.start();
            logger.info("Health check server started on port {}", 
                config.getMonitoringConfig().getHealthCheckPort());
        }
    }
    
    private void startPerformanceMonitoring() {
        AppConfig.MonitoringConfig monitoringConfig = config.getMonitoringConfig();
        
        if (monitoringConfig.isLogPerformanceStats()) {
            scheduledExecutor.scheduleAtFixedRate(
                this::logPerformanceStats,
                monitoringConfig.getStatsIntervalSeconds(),
                monitoringConfig.getStatsIntervalSeconds(),
                TimeUnit.SECONDS
            );
        }
        
        // Memory monitoring
        scheduledExecutor.scheduleAtFixedRate(
            this::monitorMemoryUsage,
            30, 30, TimeUnit.SECONDS
        );
        
        // Connection pool monitoring
        scheduledExecutor.scheduleAtFixedRate(
            this::monitorConnectionPool,
            10, 10, TimeUnit.SECONDS
        );
    }
    
    private void logPerformanceStats() {
        try {
            Duration uptime = Duration.between(startTime, Instant.now());
            long totalRecords = totalRecordsProcessed.get();
            long totalBytes = totalBytesProcessed.get();
            
            double recordsPerSecond = totalRecords / (double) uptime.getSeconds();
            double mbPerSecond = (totalBytes / 1024.0 / 1024.0) / uptime.getSeconds();
            
            SnowflakeStreamReader.StreamReaderStats readerStats = streamReader.getStats();
            OptimizedKafkaProducer.ProducerStats producerStats = kafkaProducer.getStats();
            
            logger.info("Performance Stats - Uptime: {}s, Records: {} ({:.2f}/s), " +
                       "Bytes: {} MB ({:.2f} MB/s), Errors: {}, Queue: {}, Connections: {}/{}",
                uptime.getSeconds(), totalRecords, recordsPerSecond,
                totalBytes / 1024 / 1024, mbPerSecond,
                producerStats.getSendErrors(), producerStats.getQueueSize(),
                readerStats.getActiveConnections(), readerStats.getTotalConnections());
            
            // Update throughput metrics
            metricsCollector.recordThroughput("overall", recordsPerSecond);
            metricsCollector.logMetricsSummary();
            
        } catch (Exception e) {
            logger.error("Error logging performance stats", e);
        }
    }
    
    private void monitorMemoryUsage() {
        try {
            Runtime runtime = Runtime.getRuntime();
            long maxMemory = runtime.maxMemory();
            long totalMemory = runtime.totalMemory();
            long freeMemory = runtime.freeMemory();
            long usedMemory = totalMemory - freeMemory;
            
            double usagePercent = (double) usedMemory / maxMemory * 100;
            
            metricsCollector.recordMemoryUsage(usedMemory, maxMemory);
            
            // Check memory threshold
            AppConfig.PerformanceConfig perfConfig = config.getPerformanceConfig();
            long usedMB = usedMemory / 1024 / 1024;
            
            if (usedMB > perfConfig.getGcThresholdMb()) {
                logger.warn("Memory usage high: {} MB ({}% of max), suggesting GC", 
                    usedMB, String.format("%.1f", usagePercent));
                System.gc(); // Suggest GC
            }
            
        } catch (Exception e) {
            logger.error("Error monitoring memory usage", e);
        }
    }
    
    private void monitorConnectionPool() {
        try {
            SnowflakeStreamReader.StreamReaderStats stats = streamReader.getStats();
            metricsCollector.updateConnectionMetrics(
                stats.getActiveConnections(),
                stats.getIdleConnections(),
                stats.getTotalConnections()
            );
            
            OptimizedKafkaProducer.ProducerStats producerStats = kafkaProducer.getStats();
            metricsCollector.updateQueueSize(producerStats.getQueueSize());
            
        } catch (Exception e) {
            logger.error("Error monitoring connection pool", e);
        }
    }
    
    private HealthStatus getHealthStatus() {
        boolean healthy = true;
        StringBuilder issues = new StringBuilder();
        
        try {
            // Check if streaming is running
            if (!running.get()) {
                healthy = false;
                issues.append("Streaming not running; ");
            }
            
            // Check producer circuit breaker
            OptimizedKafkaProducer.ProducerStats producerStats = kafkaProducer.getStats();
            if (producerStats.isCircuitBreakerOpen()) {
                healthy = false;
                issues.append("Kafka circuit breaker open; ");
            }
            
            // Check error rates
            MetricsCollector.MetricsSummary metrics = metricsCollector.getSummary();
            if (metrics.getErrorRate() > 10.0) { // More than 10% error rate
                healthy = false;
                issues.append(String.format("High error rate: %.1f%%; ", metrics.getErrorRate()));
            }
            
            // Check memory usage
            Runtime runtime = Runtime.getRuntime();
            long usedMemory = runtime.totalMemory() - runtime.freeMemory();
            double usagePercent = (double) usedMemory / runtime.maxMemory() * 100;
            
            if (usagePercent > 90.0) {
                healthy = false;
                issues.append(String.format("High memory usage: %.1f%%; ", usagePercent));
            }
            
        } catch (Exception e) {
            healthy = false;
            issues.append("Health check error: ").append(e.getMessage());
        }
        
        return new HealthStatus(healthy, issues.toString());
    }
    
    private void shutdown() {
        logger.info("Shutting down SnowflakeKafkaStreamer...");
        
        running.set(false);
        
        // Stop stream reader
        if (streamReader != null) {
            streamReader.close();
        }
        
        // Flush and close Kafka producer
        if (kafkaProducer != null) {
            kafkaProducer.flush();
            kafkaProducer.close();
        }
        
        // Stop scheduled executor
        scheduledExecutor.shutdown();
        try {
            if (!scheduledExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduledExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduledExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        // Stop monitoring servers
        if (metricsServer != null) {
            metricsServer.stop(5);
        }
        if (healthServer != null) {
            healthServer.stop(5);
        }
        
        // Final stats
        Duration totalUptime = Duration.between(startTime, Instant.now());
        logger.info("SnowflakeKafkaStreamer shutdown complete. " +
                   "Total uptime: {}s, Records processed: {}, Bytes processed: {} MB",
            totalUptime.getSeconds(), totalRecordsProcessed.get(), 
            totalBytesProcessed.get() / 1024 / 1024);
    }
    
    /**
     * Main entry point.
     */
    public static void main(String[] args) {
        logger.info("Starting Snowflake to Kafka CDC Streamer v1.0.0");
        
        try {
            SnowflakeKafkaStreamer streamer = new SnowflakeKafkaStreamer();
            streamer.start();
            
            // Keep the application running
            Thread.currentThread().join();
            
        } catch (Exception e) {
            logger.error("Failed to start application", e);
            System.exit(1);
        }
    }
    
    // Getters for testing
    public boolean isRunning() {
        return running.get();
    }
    
    public long getTotalRecordsProcessed() {
        return totalRecordsProcessed.get();
    }
    
    public long getTotalBytesProcessed() {
        return totalBytesProcessed.get();
    }
    
    public MetricsCollector getMetricsCollector() {
        return metricsCollector;
    }
    
    /**
     * Health status representation.
     */
    public static class HealthStatus {
        private final boolean healthy;
        private final String issues;
        private final Instant timestamp;
        
        public HealthStatus(boolean healthy, String issues) {
            this.healthy = healthy;
            this.issues = issues;
            this.timestamp = Instant.now();
        }
        
        public boolean isHealthy() {
            return healthy;
        }
        
        public String getIssues() {
            return issues;
        }
        
        public Instant getTimestamp() {
            return timestamp;
        }
        
        public String toJson() {
            return String.format(
                "{\"healthy\":%s,\"timestamp\":\"%s\",\"issues\":\"%s\"}",
                healthy, timestamp.toString(), issues
            );
        }
    }
}
