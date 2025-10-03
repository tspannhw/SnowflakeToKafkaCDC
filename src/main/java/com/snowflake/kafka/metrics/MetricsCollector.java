package com.snowflake.kafka.metrics;

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Comprehensive metrics collection for monitoring Snowflake to Kafka streaming performance.
 * Uses Micrometer with JMX registry for enterprise-ready monitoring.
 */
public class MetricsCollector {
    private static final Logger logger = LoggerFactory.getLogger(MetricsCollector.class);
    
    private final MeterRegistry meterRegistry;
    
    // Counters
    private final Counter streamReadsTotal;
    private final Counter streamErrorsTotal;
    private final Counter pollingErrorsTotal;
    private final Counter messagesSentTotal;
    private final Counter sendErrorsTotal;
    private final Counter batchesSentTotal;
    private final Counter dlqMessagesTotal;
    private final Counter dlqErrorsTotal;
    private final Counter queueFullErrorsTotal;
    private final Counter circuitBreakerOpensTotal;
    private final Counter circuitBreakerClosesTotal;
    
    // Gauges
    private final AtomicLong activeConnections = new AtomicLong(0);
    private final AtomicLong idleConnections = new AtomicLong(0);
    private final AtomicLong queueSize = new AtomicLong(0);
    private final AtomicLong batchSize = new AtomicLong(0);
    
    // Timers
    private final Timer streamReadTimer;
    private final Timer messagePublishTimer;
    private final Timer batchProcessTimer;
    
    // Histograms
    private final DistributionSummary recordSizeDistribution;
    private final DistributionSummary batchSizeDistribution;
    
    public MetricsCollector() {
        this.meterRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);
        
        // Register JVM metrics
        new JvmMemoryMetrics().bindTo(meterRegistry);
        new JvmGcMetrics().bindTo(meterRegistry);
        new JvmThreadMetrics().bindTo(meterRegistry);
        new ProcessorMetrics().bindTo(meterRegistry);
        
        // Initialize counters
        this.streamReadsTotal = Counter.builder("snowflake_stream_reads_total")
            .description("Total number of stream reads from Snowflake")
            .tag("application", "snowflake-kafka-cdc")
            .register(meterRegistry);
            
        this.streamErrorsTotal = Counter.builder("snowflake_stream_errors_total")
            .description("Total number of stream read errors")
            .tag("application", "snowflake-kafka-cdc")
            .register(meterRegistry);
            
        this.pollingErrorsTotal = Counter.builder("snowflake_polling_errors_total")
            .description("Total number of polling errors")
            .tag("application", "snowflake-kafka-cdc")
            .register(meterRegistry);
            
        this.messagesSentTotal = Counter.builder("kafka_messages_sent_total")
            .description("Total number of messages sent to Kafka")
            .tag("application", "snowflake-kafka-cdc")
            .register(meterRegistry);
            
        this.sendErrorsTotal = Counter.builder("kafka_send_errors_total")
            .description("Total number of Kafka send errors")
            .tag("application", "snowflake-kafka-cdc")
            .register(meterRegistry);
            
        this.batchesSentTotal = Counter.builder("kafka_batches_sent_total")
            .description("Total number of batches sent to Kafka")
            .tag("application", "snowflake-kafka-cdc")
            .register(meterRegistry);
            
        this.dlqMessagesTotal = Counter.builder("kafka_dlq_messages_total")
            .description("Total number of messages sent to dead letter queue")
            .tag("application", "snowflake-kafka-cdc")
            .register(meterRegistry);
            
        this.dlqErrorsTotal = Counter.builder("kafka_dlq_errors_total")
            .description("Total number of DLQ send errors")
            .tag("application", "snowflake-kafka-cdc")
            .register(meterRegistry);
            
        this.queueFullErrorsTotal = Counter.builder("queue_full_errors_total")
            .description("Total number of queue full errors")
            .tag("application", "snowflake-kafka-cdc")
            .register(meterRegistry);
            
        this.circuitBreakerOpensTotal = Counter.builder("circuit_breaker_opens_total")
            .description("Total number of circuit breaker opens")
            .tag("application", "snowflake-kafka-cdc")
            .register(meterRegistry);
            
        this.circuitBreakerClosesTotal = Counter.builder("circuit_breaker_closes_total")
            .description("Total number of circuit breaker closes")
            .tag("application", "snowflake-kafka-cdc")
            .register(meterRegistry);
        
        // Initialize gauges
        Gauge.builder("snowflake_active_connections", this, MetricsCollector::getActiveConnections)
            .description("Number of active Snowflake connections")
            .tag("application", "snowflake-kafka-cdc")
            .register(meterRegistry);
            
        Gauge.builder("snowflake_idle_connections", this, MetricsCollector::getIdleConnections)
            .description("Number of idle Snowflake connections")
            .tag("application", "snowflake-kafka-cdc")
            .register(meterRegistry);
            
        Gauge.builder("async_queue_size", this, MetricsCollector::getQueueSize)
            .description("Current size of async processing queue")
            .tag("application", "snowflake-kafka-cdc")
            .register(meterRegistry);
            
        Gauge.builder("current_batch_size", this, MetricsCollector::getBatchSize)
            .description("Current batch size being processed")
            .tag("application", "snowflake-kafka-cdc")
            .register(meterRegistry);
        
        // Initialize timers
        this.streamReadTimer = Timer.builder("snowflake_stream_read_duration")
            .description("Time taken to read from Snowflake streams")
            .tag("application", "snowflake-kafka-cdc")
            .register(meterRegistry);
            
        this.messagePublishTimer = Timer.builder("kafka_message_publish_duration")
            .description("Time taken to publish messages to Kafka")
            .tag("application", "snowflake-kafka-cdc")
            .register(meterRegistry);
            
        this.batchProcessTimer = Timer.builder("batch_process_duration")
            .description("Time taken to process batches")
            .tag("application", "snowflake-kafka-cdc")
            .register(meterRegistry);
        
        // Initialize distribution summaries
        this.recordSizeDistribution = DistributionSummary.builder("record_size_bytes")
            .description("Distribution of record sizes in bytes")
            .tag("application", "snowflake-kafka-cdc")
            .register(meterRegistry);
            
        this.batchSizeDistribution = DistributionSummary.builder("batch_size_records")
            .description("Distribution of batch sizes in number of records")
            .tag("application", "snowflake-kafka-cdc")
            .register(meterRegistry);
        
        logger.info("MetricsCollector initialized with JMX registry");
    }
    
    // Stream metrics
    public void recordStreamRead(String streamName, int recordCount, long durationNanos) {
        Counter.builder("snowflake_stream_reads_total")
            .description("Total number of stream reads")
            .tags("stream", streamName)
            .register(meterRegistry)
            .increment();
        streamReadTimer.record(durationNanos, TimeUnit.NANOSECONDS);
        
        if (recordCount > 0) {
            batchSizeDistribution.record(recordCount);
        }
    }
    
    public void incrementStreamErrors(String streamName) {
        Counter.builder("snowflake_stream_errors_total")
            .description("Total number of stream errors")
            .tags("stream", streamName)
            .register(meterRegistry)
            .increment();
    }
    
    public void incrementPollingErrors() {
        pollingErrorsTotal.increment();
    }
    
    // Kafka metrics
    public void recordMessageSent(String topic, int partition, long sizeBytes) {
        Counter.builder("kafka_messages_sent_total")
            .description("Total number of messages sent to Kafka")
            .tags("topic", topic, "partition", String.valueOf(partition))
            .register(meterRegistry)
            .increment();
        recordSizeDistribution.record(sizeBytes);
    }
    
    public void recordMessagePublishTime(long durationNanos) {
        messagePublishTimer.record(durationNanos, TimeUnit.NANOSECONDS);
    }
    
    public void incrementSendErrors() {
        sendErrorsTotal.increment();
    }
    
    public void recordBatchSent(int batchSize) {
        batchesSentTotal.increment();
        batchSizeDistribution.record(batchSize);
    }
    
    public void recordBatchProcessTime(long durationNanos) {
        batchProcessTimer.record(durationNanos, TimeUnit.NANOSECONDS);
    }
    
    // Error handling metrics
    public void incrementDlqMessages() {
        dlqMessagesTotal.increment();
    }
    
    public void incrementDlqErrors() {
        dlqErrorsTotal.increment();
    }
    
    public void incrementQueueFullErrors() {
        queueFullErrorsTotal.increment();
    }
    
    public void incrementCircuitBreakerOpens() {
        circuitBreakerOpensTotal.increment();
    }
    
    public void incrementCircuitBreakerCloses() {
        circuitBreakerClosesTotal.increment();
    }
    
    // Connection pool metrics
    public void updateConnectionMetrics(int active, int idle, int total) {
        activeConnections.set(active);
        idleConnections.set(idle);
    }
    
    // Queue metrics
    public void updateQueueSize(int size) {
        queueSize.set(size);
    }
    
    public void updateBatchSize(int size) {
        batchSize.set(size);
    }
    
    // Custom metrics for business logic
    public void recordThroughput(String component, double recordsPerSecond) {
        Gauge.builder("throughput_records_per_second", recordsPerSecond, value -> value)
            .description("Records processed per second")
            .tag("component", component)
            .register(meterRegistry);
    }
    
    public void recordLatency(String operation, Duration latency) {
        Timer.builder("operation_latency")
            .description("Operation latency")
            .tag("operation", operation)
            .register(meterRegistry)
            .record(latency);
    }
    
    // Health metrics
    public void recordHealthCheck(String component, boolean healthy) {
        Gauge.builder("health_status", healthy ? 1.0 : 0.0, value -> value)
            .description("Health status (1 = healthy, 0 = unhealthy)")
            .tag("component", component)
            .register(meterRegistry);
    }
    
    // Memory and performance metrics
    public void recordMemoryUsage(long usedBytes, long maxBytes) {
        Gauge.builder("memory_usage_bytes", (double) usedBytes, value -> value)
            .description("Current memory usage")
            .tag("type", "used")
            .register(meterRegistry);
            
        Gauge.builder("memory_usage_bytes", (double) maxBytes, value -> value)
            .description("Maximum memory available")
            .tag("type", "max")
            .register(meterRegistry);
    }
    
    // Performance summary
    public MetricsSummary getSummary() {
        return new MetricsSummary(
            (long) streamReadsTotal.count(),
            (long) streamErrorsTotal.count(),
            (long) messagesSentTotal.count(),
            (long) sendErrorsTotal.count(),
            (long) batchesSentTotal.count(),
            streamReadTimer.mean(TimeUnit.MILLISECONDS),
            messagePublishTimer.mean(TimeUnit.MILLISECONDS),
            recordSizeDistribution.mean(),
            batchSizeDistribution.mean(),
            activeConnections.get(),
            queueSize.get()
        );
    }
    
    // Getter methods for gauges
    private double getActiveConnections() {
        return activeConnections.get();
    }
    
    private double getIdleConnections() {
        return idleConnections.get();
    }
    
    private double getQueueSize() {
        return queueSize.get();
    }
    
    private double getBatchSize() {
        return batchSize.get();
    }
    
    /**
     * Gets the JMX metrics registry for enterprise monitoring tools.
     */
    public MeterRegistry getMeterRegistry() {
        return meterRegistry;
    }
    
    /**
     * Gets JMX formatted metrics string.
     */
    public String getJmxMetrics() {
        if (meterRegistry instanceof JmxMeterRegistry) {
            return "JMX metrics available via JMX port";
        }
        return "";
    }
    
    /**
     * Logs current metrics summary.
     */
    public void logMetricsSummary() {
        MetricsSummary summary = getSummary();
        logger.info("Metrics Summary: streams={}, errors={}, messages={}, send_errors={}, " +
                   "batches={}, avg_read_time={}ms, avg_publish_time={}ms, avg_record_size={} bytes, " +
                   "avg_batch_size={}, active_connections={}, queue_size={}",
            summary.getStreamReads(), summary.getStreamErrors(), summary.getMessagesSent(),
            summary.getSendErrors(), summary.getBatchesSent(), summary.getAvgReadTimeMs(),
            summary.getAvgPublishTimeMs(), summary.getAvgRecordSizeBytes(), summary.getAvgBatchSize(),
            summary.getActiveConnections(), summary.getQueueSize());
    }
    
    public static class MetricsSummary {
        private final long streamReads;
        private final long streamErrors;
        private final long messagesSent;
        private final long sendErrors;
        private final long batchesSent;
        private final double avgReadTimeMs;
        private final double avgPublishTimeMs;
        private final double avgRecordSizeBytes;
        private final double avgBatchSize;
        private final long activeConnections;
        private final long queueSize;
        
        public MetricsSummary(long streamReads, long streamErrors, long messagesSent, 
                            long sendErrors, long batchesSent, double avgReadTimeMs,
                            double avgPublishTimeMs, double avgRecordSizeBytes, 
                            double avgBatchSize, long activeConnections, long queueSize) {
            this.streamReads = streamReads;
            this.streamErrors = streamErrors;
            this.messagesSent = messagesSent;
            this.sendErrors = sendErrors;
            this.batchesSent = batchesSent;
            this.avgReadTimeMs = avgReadTimeMs;
            this.avgPublishTimeMs = avgPublishTimeMs;
            this.avgRecordSizeBytes = avgRecordSizeBytes;
            this.avgBatchSize = avgBatchSize;
            this.activeConnections = activeConnections;
            this.queueSize = queueSize;
        }
        
        // Getters
        public long getStreamReads() { return streamReads; }
        public long getStreamErrors() { return streamErrors; }
        public long getMessagesSent() { return messagesSent; }
        public long getSendErrors() { return sendErrors; }
        public long getBatchesSent() { return batchesSent; }
        public double getAvgReadTimeMs() { return avgReadTimeMs; }
        public double getAvgPublishTimeMs() { return avgPublishTimeMs; }
        public double getAvgRecordSizeBytes() { return avgRecordSizeBytes; }
        public double getAvgBatchSize() { return avgBatchSize; }
        public long getActiveConnections() { return activeConnections; }
        public long getQueueSize() { return queueSize; }
        
        public double getSuccessRate() {
            long total = messagesSent + sendErrors;
            return total > 0 ? (double) messagesSent / total * 100.0 : 0.0;
        }
        
        public double getErrorRate() {
            return 100.0 - getSuccessRate();
        }
    }
}
