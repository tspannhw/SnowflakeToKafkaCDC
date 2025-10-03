package com.snowflake.kafka.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Application configuration management using Typesafe Config.
 * Provides type-safe access to all configuration parameters.
 */
public class AppConfig {
    private final Config config;
    
    public AppConfig() {
        this.config = ConfigFactory.load();
    }
    
    public AppConfig(Config config) {
        this.config = config;
    }
    
    // Snowflake Configuration
    public SnowflakeConfig getSnowflakeConfig() {
        return new SnowflakeConfig(config.getConfig("snowflake"));
    }
    
    // Kafka Configuration
    public KafkaConfig getKafkaConfig() {
        return new KafkaConfig(config.getConfig("kafka"));
    }
    
    // Performance Configuration
    public PerformanceConfig getPerformanceConfig() {
        return new PerformanceConfig(config.getConfig("performance"));
    }
    
    // Monitoring Configuration
    public MonitoringConfig getMonitoringConfig() {
        return new MonitoringConfig(config.getConfig("monitoring"));
    }
    
    // Error Handling Configuration
    public ErrorHandlingConfig getErrorHandlingConfig() {
        return new ErrorHandlingConfig(config.getConfig("error-handling"));
    }
    
    public static class SnowflakeConfig {
        private final Config config;
        
        public SnowflakeConfig(Config config) {
            this.config = config;
        }
        
        public String getAccount() { return config.getString("account"); }
        public String getUser() { return config.getString("user"); }
        public String getPassword() { return config.getString("password"); }
        public String getWarehouse() { return config.getString("warehouse"); }
        public String getDatabase() { return config.getString("database"); }
        public String getSchema() { return config.getString("schema"); }
        public String getRole() { return config.getString("role"); }
        
        public ConnectionPoolConfig getConnectionPoolConfig() {
            return new ConnectionPoolConfig(config.getConfig("connection-pool"));
        }
        
        public StreamsConfig getStreamsConfig() {
            return new StreamsConfig(config.getConfig("streams"));
        }
        
        public String getJdbcUrl() {
            return String.format("jdbc:snowflake://%s.snowflakecomputing.com/", getAccount());
        }
        
        public Properties getJdbcProperties() {
            Properties props = new Properties();
            props.setProperty("user", getUser());
            props.setProperty("password", getPassword());
            props.setProperty("warehouse", getWarehouse());
            props.setProperty("db", getDatabase());
            props.setProperty("schema", getSchema());
            props.setProperty("role", getRole());
            
            // Performance optimizations
            props.setProperty("CLIENT_SESSION_KEEP_ALIVE", "true");
            props.setProperty("CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY", "3600");
            props.setProperty("JDBC_QUERY_RESULT_FORMAT", "JSON");
            props.setProperty("CLIENT_RESULT_PREFETCH_SLOTS", "2");
            props.setProperty("CLIENT_RESULT_PREFETCH_THREADS", "1");
            
            return props;
        }
    }
    
    public static class ConnectionPoolConfig {
        private final Config config;
        
        public ConnectionPoolConfig(Config config) {
            this.config = config;
        }
        
        public int getMaximumPoolSize() { return config.getInt("maximum-pool-size"); }
        public int getMinimumIdle() { return config.getInt("minimum-idle"); }
        public long getConnectionTimeout() { return config.getLong("connection-timeout"); }
        public long getIdleTimeout() { return config.getLong("idle-timeout"); }
        public long getMaxLifetime() { return config.getLong("max-lifetime"); }
        public long getLeakDetectionThreshold() { return config.getLong("leak-detection-threshold"); }
    }
    
    public static class StreamsConfig {
        private final Config config;
        
        public StreamsConfig(Config config) {
            this.config = config;
        }
        
        public List<String> getStreamNames() { return config.getStringList("stream-names"); }
        public long getPollIntervalMs() { return config.getLong("poll-interval-ms"); }
        public int getBatchSize() { return config.getInt("batch-size"); }
        public long getMaxWaitTimeMs() { return config.getLong("max-wait-time-ms"); }
        public int getQueryTimeoutSeconds() { return config.getInt("query-timeout-seconds"); }
        public int getMaxRetries() { return config.getInt("max-retries"); }
        public long getRetryDelayMs() { return config.getLong("retry-delay-ms"); }
        public boolean isExponentialBackoff() { return config.getBoolean("exponential-backoff"); }
    }
    
    public static class KafkaConfig {
        private final Config config;
        
        public KafkaConfig(Config config) {
            this.config = config;
        }
        
        public String getBootstrapServers() { return config.getString("bootstrap-servers"); }
        
        public ProducerConfig getProducerConfig() {
            return new ProducerConfig(config.getConfig("producer"));
        }
        
        public TopicsConfig getTopicsConfig() {
            return new TopicsConfig(config.getConfig("topics"));
        }
        
        public Properties getProducerProperties() {
            Properties props = new Properties();
            ProducerConfig producerConfig = getProducerConfig();
            
            props.setProperty("bootstrap.servers", getBootstrapServers());
            props.setProperty("acks", producerConfig.getAcks());
            props.setProperty("retries", String.valueOf(producerConfig.getRetries()));
            props.setProperty("max.in.flight.requests.per.connection", 
                String.valueOf(producerConfig.getMaxInFlightRequestsPerConnection()));
            props.setProperty("enable.idempotence", String.valueOf(producerConfig.isEnableIdempotence()));
            props.setProperty("batch.size", String.valueOf(producerConfig.getBatchSize()));
            props.setProperty("linger.ms", String.valueOf(producerConfig.getLingerMs()));
            props.setProperty("buffer.memory", String.valueOf(producerConfig.getBufferMemory()));
            props.setProperty("compression.type", producerConfig.getCompressionType());
            props.setProperty("key.serializer", producerConfig.getKeySerializer());
            props.setProperty("value.serializer", producerConfig.getValueSerializer());
            props.setProperty("request.timeout.ms", String.valueOf(producerConfig.getRequestTimeoutMs()));
            props.setProperty("delivery.timeout.ms", String.valueOf(producerConfig.getDeliveryTimeoutMs()));
            props.setProperty("metrics.sample.window.ms", String.valueOf(producerConfig.getMetricsSampleWindowMs()));
            props.setProperty("metrics.num.samples", String.valueOf(producerConfig.getMetricsNumSamples()));
            
            return props;
        }
    }
    
    public static class ProducerConfig {
        private final Config config;
        
        public ProducerConfig(Config config) {
            this.config = config;
        }
        
        public String getAcks() { return config.getString("acks"); }
        public int getRetries() { return config.getInt("retries"); }
        public int getMaxInFlightRequestsPerConnection() { return config.getInt("max-in-flight-requests-per-connection"); }
        public boolean isEnableIdempotence() { return config.getBoolean("enable-idempotence"); }
        public int getBatchSize() { return config.getInt("batch-size"); }
        public int getLingerMs() { return config.getInt("linger-ms"); }
        public long getBufferMemory() { return config.getLong("buffer-memory"); }
        public String getCompressionType() { return config.getString("compression-type"); }
        public String getKeySerializer() { return config.getString("key-serializer"); }
        public String getValueSerializer() { return config.getString("value-serializer"); }
        public int getRequestTimeoutMs() { return config.getInt("request-timeout-ms"); }
        public int getDeliveryTimeoutMs() { return config.getInt("delivery-timeout-ms"); }
        public int getMetricsSampleWindowMs() { return config.getInt("metrics-sample-window-ms"); }
        public int getMetricsNumSamples() { return config.getInt("metrics-num-samples"); }
    }
    
    public static class TopicsConfig {
        private final Config config;
        
        public TopicsConfig(Config config) {
            this.config = config;
        }
        
        public String getPrefix() { return config.getString("prefix"); }
        public Map<String, String> getMapping() { 
            return config.getConfig("mapping").entrySet().stream()
                .collect(java.util.stream.Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> entry.getValue().unwrapped().toString()
                ));
        }
        public String getDefaultTopic() { return config.getString("default-topic"); }
        
        public String getTopicForStream(String streamName) {
            return getMapping().getOrDefault(streamName, getDefaultTopic());
        }
    }
    
    public static class PerformanceConfig {
        private final Config config;
        
        public PerformanceConfig(Config config) {
            this.config = config;
        }
        
        public int getCoreThreads() { return config.getInt("core-threads"); }
        public int getMaxThreads() { return config.getInt("max-threads"); }
        public int getQueueCapacity() { return config.getInt("queue-capacity"); }
        public int getKeepAliveSeconds() { return config.getInt("keep-alive-seconds"); }
        public boolean isAsyncProcessing() { return config.getBoolean("async-processing"); }
        public int getAsyncQueueSize() { return config.getInt("async-queue-size"); }
        public boolean isEnableBatching() { return config.getBoolean("enable-batching"); }
        public long getBatchTimeoutMs() { return config.getLong("batch-timeout-ms"); }
        public int getMaxMemoryMb() { return config.getInt("max-memory-mb"); }
        public int getGcThresholdMb() { return config.getInt("gc-threshold-mb"); }
    }
    
    public static class MonitoringConfig {
        private final Config config;
        
        public MonitoringConfig(Config config) {
            this.config = config;
        }
        
        public boolean isEnableMetrics() { return config.getBoolean("enable-metrics"); }
        public int getMetricsPort() { return config.getInt("metrics-port"); }
        public String getMetricsPath() { return config.getString("metrics-path"); }
        public int getHealthCheckPort() { return config.getInt("health-check-port"); }
        public String getHealthCheckPath() { return config.getString("health-check-path"); }
        public String getLogLevel() { return config.getString("log-level"); }
        public boolean isLogPerformanceStats() { return config.getBoolean("log-performance-stats"); }
        public int getStatsIntervalSeconds() { return config.getInt("stats-interval-seconds"); }
    }
    
    public static class ErrorHandlingConfig {
        private final Config config;
        
        public ErrorHandlingConfig(Config config) {
            this.config = config;
        }
        
        public boolean isEnableDlq() { return config.getBoolean("enable-dlq"); }
        public String getDlqTopic() { return config.getString("dlq-topic"); }
        public int getMaxRetries() { return config.getInt("max-retries"); }
        public long getRetryDelayMs() { return config.getLong("retry-delay-ms"); }
        public boolean isExponentialBackoff() { return config.getBoolean("exponential-backoff"); }
        public long getMaxRetryDelayMs() { return config.getLong("max-retry-delay-ms"); }
        public boolean isEnableCircuitBreaker() { return config.getBoolean("enable-circuit-breaker"); }
        public int getFailureThreshold() { return config.getInt("failure-threshold"); }
        public long getRecoveryTimeoutMs() { return config.getLong("recovery-timeout-ms"); }
    }
}
