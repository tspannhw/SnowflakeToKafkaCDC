package com.snowflake.kafka.stream;

import com.snowflake.kafka.config.AppConfig;
import com.snowflake.kafka.metrics.MetricsCollector;
import com.snowflake.kafka.model.StreamRecord;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * High-performance Snowflake stream reader with connection pooling and optimized queries.
 * Reads from Snowflake streams and converts to StreamRecord objects for Kafka publishing.
 */
public class SnowflakeStreamReader implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(SnowflakeStreamReader.class);
    
    private final AppConfig.SnowflakeConfig config;
    private final MetricsCollector metricsCollector;
    private final HikariDataSource dataSource;
    private final ExecutorService executorService;
    private final AtomicLong recordsRead = new AtomicLong(0);
    private final AtomicLong bytesRead = new AtomicLong(0);
    
    private volatile boolean running = false;
    
    public SnowflakeStreamReader(AppConfig.SnowflakeConfig config, MetricsCollector metricsCollector) {
        this.config = config;
        this.metricsCollector = metricsCollector;
        this.dataSource = createDataSource();
        this.executorService = Executors.newFixedThreadPool(
            Math.max(2, Runtime.getRuntime().availableProcessors())
        );
        
        logger.info("SnowflakeStreamReader initialized with {} streams", 
            config.getStreamsConfig().getStreamNames().size());
    }
    
    private HikariDataSource createDataSource() {
        HikariConfig hikariConfig = new HikariConfig();
        AppConfig.ConnectionPoolConfig poolConfig = config.getConnectionPoolConfig();
        
        // Connection settings
        hikariConfig.setJdbcUrl(config.getJdbcUrl());
        hikariConfig.setDataSourceProperties(config.getJdbcProperties());
        
        // Pool settings for high performance
        hikariConfig.setMaximumPoolSize(poolConfig.getMaximumPoolSize());
        hikariConfig.setMinimumIdle(poolConfig.getMinimumIdle());
        hikariConfig.setConnectionTimeout(poolConfig.getConnectionTimeout());
        hikariConfig.setIdleTimeout(poolConfig.getIdleTimeout());
        hikariConfig.setMaxLifetime(poolConfig.getMaxLifetime());
        hikariConfig.setLeakDetectionThreshold(poolConfig.getLeakDetectionThreshold());
        
        // Performance optimizations
        hikariConfig.setPoolName("SnowflakePool");
        hikariConfig.setConnectionTestQuery("SELECT 1");
        hikariConfig.setValidationTimeout(5000);
        hikariConfig.setInitializationFailTimeout(10000);
        
        // Additional optimizations
        hikariConfig.addDataSourceProperty("cachePrepStmts", "true");
        hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250");
        hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        hikariConfig.addDataSourceProperty("useServerPrepStmts", "true");
        
        return new HikariDataSource(hikariConfig);
    }
    
    /**
     * Reads records from a specific Snowflake stream with optimized batching.
     */
    public CompletableFuture<List<StreamRecord>> readStreamAsync(String streamName) {
        return CompletableFuture.supplyAsync(() -> readStream(streamName), executorService);
    }
    
    /**
     * Synchronous stream reading with performance optimizations.
     */
    public List<StreamRecord> readStream(String streamName) {
        long startTime = System.nanoTime();
        List<StreamRecord> records = new ArrayList<>();
        
        try (Connection connection = dataSource.getConnection()) {
            // Optimize connection for this session
            optimizeConnection(connection);
            
            String query = buildOptimizedStreamQuery(streamName);
            
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                // Set query timeout
                statement.setQueryTimeout(config.getStreamsConfig().getQueryTimeoutSeconds());
                
                // Set fetch size for better performance
                statement.setFetchSize(config.getStreamsConfig().getBatchSize());
                
                try (ResultSet resultSet = statement.executeQuery()) {
                    records = processResultSet(resultSet, streamName);
                }
            }
            
        } catch (SQLException e) {
            logger.error("Error reading from stream {}: {}", streamName, e.getMessage(), e);
            metricsCollector.incrementStreamErrors(streamName);
            throw new RuntimeException("Failed to read from stream: " + streamName, e);
        }
        
        // Update metrics
        long duration = System.nanoTime() - startTime;
        recordsRead.addAndGet(records.size());
        metricsCollector.recordStreamRead(streamName, records.size(), duration);
        
        logger.debug("Read {} records from stream {} in {}ms", 
            records.size(), streamName, TimeUnit.NANOSECONDS.toMillis(duration));
        
        return records;
    }
    
    private void optimizeConnection(Connection connection) throws SQLException {
        // Set session parameters for optimal performance
        try (Statement stmt = connection.createStatement()) {
            // Optimize for analytical workloads
            stmt.execute("ALTER SESSION SET QUERY_RESULT_FORMAT = 'JSON'");
            stmt.execute("ALTER SESSION SET CLIENT_RESULT_PREFETCH_SLOTS = 2");
            stmt.execute("ALTER SESSION SET CLIENT_RESULT_PREFETCH_THREADS = 1");
            
            // Optimize memory usage
            stmt.execute("ALTER SESSION SET CLIENT_MEMORY_LIMIT = 2048");
            stmt.execute("ALTER SESSION SET CLIENT_RESULT_CHUNK_SIZE = 128");
            
            // Set timezone to UTC for consistency
            stmt.execute("ALTER SESSION SET TIMEZONE = 'UTC'");
        }
    }
    
    private String buildOptimizedStreamQuery(String streamName) {
        AppConfig.StreamsConfig streamConfig = config.getStreamsConfig();
        
        // Build optimized query with LIMIT for batching
        StringBuilder query = new StringBuilder();
        query.append("SELECT ");
        query.append("METADATA$ACTION, ");
        query.append("METADATA$ISUPDATE, ");
        query.append("METADATA$ROW_ID, ");
        query.append("METADATA$STREAM_OFFSET, ");
        query.append("* ");
        query.append("FROM ");
        query.append(streamName);
        query.append(" LIMIT ");
        query.append(streamConfig.getBatchSize());
        
        return query.toString();
    }
    
    private List<StreamRecord> processResultSet(ResultSet resultSet, String streamName) throws SQLException {
        List<StreamRecord> records = new ArrayList<>();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        
        while (resultSet.next()) {
            StreamRecord record = new StreamRecord();
            
            // Set stream metadata
            record.setStreamName(streamName);
            record.setTimestamp(Instant.now());
            record.setAction(resultSet.getString("METADATA$ACTION"));
            record.setIsUpdate(resultSet.getBoolean("METADATA$ISUPDATE"));
            record.setRowId(resultSet.getString("METADATA$ROW_ID"));
            record.setStreamOffset(resultSet.getLong("METADATA$STREAM_OFFSET"));
            
            // Process all columns efficiently
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnLabel(i);
                
                // Skip metadata columns as they're already processed
                if (columnName.startsWith("METADATA$")) {
                    continue;
                }
                
                Object value = getOptimizedColumnValue(resultSet, i, metaData.getColumnType(i));
                record.addColumn(columnName, value);
            }
            
            records.add(record);
            
            // Update bytes read metric (approximate)
            bytesRead.addAndGet(estimateRecordSize(record));
        }
        
        return records;
    }
    
    private Object getOptimizedColumnValue(ResultSet resultSet, int columnIndex, int sqlType) throws SQLException {
        // Optimized value extraction based on SQL type
        switch (sqlType) {
            case Types.VARCHAR:
            case Types.CHAR:
            case Types.LONGVARCHAR:
                return resultSet.getString(columnIndex);
            case Types.INTEGER:
                return resultSet.getInt(columnIndex);
            case Types.BIGINT:
                return resultSet.getLong(columnIndex);
            case Types.DECIMAL:
            case Types.NUMERIC:
                return resultSet.getBigDecimal(columnIndex);
            case Types.DOUBLE:
            case Types.FLOAT:
                return resultSet.getDouble(columnIndex);
            case Types.BOOLEAN:
                return resultSet.getBoolean(columnIndex);
            case Types.TIMESTAMP:
                return resultSet.getTimestamp(columnIndex);
            case Types.DATE:
                return resultSet.getDate(columnIndex);
            case Types.TIME:
                return resultSet.getTime(columnIndex);
            default:
                return resultSet.getObject(columnIndex);
        }
    }
    
    private long estimateRecordSize(StreamRecord record) {
        // Rough estimation of record size in bytes
        long size = 100; // Base overhead
        
        for (Object value : record.getColumns().values()) {
            if (value instanceof String) {
                size += ((String) value).length() * 2; // UTF-16 estimation
            } else if (value instanceof Number) {
                size += 8; // Average number size
            } else if (value != null) {
                size += 50; // Other object overhead
            }
        }
        
        return size;
    }
    
    /**
     * Reads from all configured streams concurrently.
     */
    public CompletableFuture<List<StreamRecord>> readAllStreamsAsync() {
        List<String> streamNames = config.getStreamsConfig().getStreamNames();
        
        List<CompletableFuture<List<StreamRecord>>> futures = streamNames.stream()
            .map(this::readStreamAsync)
            .toList();
        
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenApply(v -> futures.stream()
                .flatMap(future -> future.join().stream())
                .toList());
    }
    
    /**
     * Starts continuous polling of all streams.
     */
    public void startPolling(StreamRecordConsumer consumer) {
        if (running) {
            logger.warn("Stream polling is already running");
            return;
        }
        
        running = true;
        logger.info("Starting stream polling for {} streams", 
            config.getStreamsConfig().getStreamNames().size());
        
        executorService.submit(() -> {
            while (running) {
                try {
                    readAllStreamsAsync()
                        .thenAccept(records -> {
                            if (!records.isEmpty()) {
                                consumer.accept(records);
                            }
                        })
                        .exceptionally(throwable -> {
                            logger.error("Error during stream polling", throwable);
                            metricsCollector.incrementPollingErrors();
                            return null;
                        });
                    
                    // Wait before next poll
                    Thread.sleep(config.getStreamsConfig().getPollIntervalMs());
                    
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.info("Stream polling interrupted");
                    break;
                } catch (Exception e) {
                    logger.error("Unexpected error during stream polling", e);
                    metricsCollector.incrementPollingErrors();
                }
            }
        });
    }
    
    /**
     * Stops stream polling.
     */
    public void stopPolling() {
        running = false;
        logger.info("Stopping stream polling");
    }
    
    /**
     * Gets performance statistics.
     */
    public StreamReaderStats getStats() {
        return new StreamReaderStats(
            recordsRead.get(),
            bytesRead.get(),
            dataSource.getHikariPoolMXBean().getActiveConnections(),
            dataSource.getHikariPoolMXBean().getIdleConnections(),
            dataSource.getHikariPoolMXBean().getTotalConnections()
        );
    }
    
    @Override
    public void close() {
        logger.info("Closing SnowflakeStreamReader");
        
        stopPolling();
        
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        if (dataSource != null) {
            dataSource.close();
        }
        
        logger.info("SnowflakeStreamReader closed. Total records read: {}, bytes read: {}", 
            recordsRead.get(), bytesRead.get());
    }
    
    @FunctionalInterface
    public interface StreamRecordConsumer {
        void accept(List<StreamRecord> records);
    }
    
    public static class StreamReaderStats {
        private final long recordsRead;
        private final long bytesRead;
        private final int activeConnections;
        private final int idleConnections;
        private final int totalConnections;
        
        public StreamReaderStats(long recordsRead, long bytesRead, int activeConnections, 
                               int idleConnections, int totalConnections) {
            this.recordsRead = recordsRead;
            this.bytesRead = bytesRead;
            this.activeConnections = activeConnections;
            this.idleConnections = idleConnections;
            this.totalConnections = totalConnections;
        }
        
        // Getters
        public long getRecordsRead() { return recordsRead; }
        public long getBytesRead() { return bytesRead; }
        public int getActiveConnections() { return activeConnections; }
        public int getIdleConnections() { return idleConnections; }
        public int getTotalConnections() { return totalConnections; }
    }
}
