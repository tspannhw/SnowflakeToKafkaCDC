package com.snowflake.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a record from a Snowflake stream with metadata and data columns.
 * Optimized for JSON serialization and Kafka publishing.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class StreamRecord {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    
    @JsonProperty("stream_name")
    private String streamName;
    
    @JsonProperty("timestamp")
    private Instant timestamp;
    
    @JsonProperty("action")
    private String action; // INSERT, UPDATE, DELETE
    
    @JsonProperty("is_update")
    @JsonAlias({"update"})
    private Boolean isUpdate;
    
    @JsonProperty("row_id")
    private String rowId;
    
    @JsonProperty("stream_offset")
    private Long streamOffset;
    
    @JsonProperty("columns")
    private Map<String, Object> columns = new HashMap<>();
    
    @JsonProperty("metadata")
    private Map<String, Object> metadata = new HashMap<>();
    
    // Constructors
    public StreamRecord() {}
    
    public StreamRecord(String streamName, String action) {
        this.streamName = streamName;
        this.action = action;
        this.timestamp = Instant.now();
    }
    
    // Getters and Setters
    public String getStreamName() {
        return streamName;
    }
    
    public void setStreamName(String streamName) {
        this.streamName = streamName;
    }
    
    public Instant getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }
    
    public String getAction() {
        return action;
    }
    
    public void setAction(String action) {
        this.action = action;
    }
    
    public Boolean getIsUpdate() {
        return isUpdate;
    }
    
    public void setIsUpdate(Boolean isUpdate) {
        this.isUpdate = isUpdate;
    }
    
    public String getRowId() {
        return rowId;
    }
    
    public void setRowId(String rowId) {
        this.rowId = rowId;
    }
    
    public Long getStreamOffset() {
        return streamOffset;
    }
    
    public void setStreamOffset(Long streamOffset) {
        this.streamOffset = streamOffset;
    }
    
    public Map<String, Object> getColumns() {
        return columns;
    }
    
    public void setColumns(Map<String, Object> columns) {
        this.columns = columns != null ? columns : new HashMap<>();
    }
    
    public Map<String, Object> getMetadata() {
        return metadata;
    }
    
    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata != null ? metadata : new HashMap<>();
    }
    
    // Utility methods
    public void addColumn(String name, Object value) {
        this.columns.put(name, value);
    }
    
    public void addMetadata(String key, Object value) {
        this.metadata.put(key, value);
    }
    
    public Object getColumn(String name) {
        return this.columns.get(name);
    }
    
    public Object getMetadataValue(String key) {
        return this.metadata.get(key);
    }
    
    /**
     * Generates a Kafka key for this record.
     * Uses row_id if available, otherwise generates based on primary key columns.
     */
    public String generateKafkaKey() {
        if (rowId != null && !rowId.isEmpty()) {
            return streamName + ":" + rowId;
        }
        
        // Try to find primary key columns (commonly named ID, *_ID)
        for (Map.Entry<String, Object> entry : columns.entrySet()) {
            String columnName = entry.getKey().toLowerCase();
            if (columnName.equals("id") || columnName.endsWith("_id")) {
                return streamName + ":" + entry.getValue();
            }
        }
        
        // Fallback to stream offset
        return streamName + ":" + (streamOffset != null ? streamOffset : timestamp.toEpochMilli());
    }
    
    /**
     * Converts the record to JSON string for Kafka value.
     */
    public String toJson() {
        try {
            return OBJECT_MAPPER.writeValueAsString(this);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize StreamRecord to JSON", e);
        }
    }
    
    /**
     * Creates a StreamRecord from JSON string.
     */
    public static StreamRecord fromJson(String json) {
        try {
            return OBJECT_MAPPER.readValue(json, StreamRecord.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize StreamRecord from JSON", e);
        }
    }
    
    /**
     * Creates a compact representation for logging.
     */
    public String toCompactString() {
        return String.format("StreamRecord{stream=%s, action=%s, rowId=%s, columns=%d}", 
            streamName, action, rowId, columns.size());
    }
    
    /**
     * Checks if this is an INSERT operation.
     */
    public boolean isInsert() {
        return "INSERT".equalsIgnoreCase(action);
    }
    
    /**
     * Checks if this is an UPDATE operation.
     */
    public boolean isUpdate() {
        return "UPDATE".equalsIgnoreCase(action) || Boolean.TRUE.equals(isUpdate);
    }
    
    /**
     * Checks if this is a DELETE operation.
     */
    public boolean isDelete() {
        return "DELETE".equalsIgnoreCase(action);
    }
    
    /**
     * Gets the estimated size of this record in bytes.
     */
    public long getEstimatedSizeBytes() {
        long size = 200; // Base overhead
        
        // Stream name
        if (streamName != null) {
            size += streamName.length() * 2;
        }
        
        // Action
        if (action != null) {
            size += action.length() * 2;
        }
        
        // Row ID
        if (rowId != null) {
            size += rowId.length() * 2;
        }
        
        // Columns
        for (Map.Entry<String, Object> entry : columns.entrySet()) {
            size += entry.getKey().length() * 2; // Key
            Object value = entry.getValue();
            if (value instanceof String) {
                size += ((String) value).length() * 2;
            } else if (value instanceof Number) {
                size += 8;
            } else if (value != null) {
                size += 50; // Estimated overhead for other types
            }
        }
        
        // Metadata
        for (Map.Entry<String, Object> entry : metadata.entrySet()) {
            size += entry.getKey().length() * 2;
            Object value = entry.getValue();
            if (value instanceof String) {
                size += ((String) value).length() * 2;
            } else if (value != null) {
                size += 20;
            }
        }
        
        return size;
    }
    
    /**
     * Validates that the record has required fields.
     */
    public boolean isValid() {
        return streamName != null && !streamName.isEmpty() &&
               action != null && !action.isEmpty() &&
               timestamp != null;
    }
    
    /**
     * Creates a copy of this record with updated timestamp.
     */
    public StreamRecord withUpdatedTimestamp() {
        StreamRecord copy = new StreamRecord();
        copy.streamName = this.streamName;
        copy.timestamp = Instant.now();
        copy.action = this.action;
        copy.isUpdate = this.isUpdate;
        copy.rowId = this.rowId;
        copy.streamOffset = this.streamOffset;
        copy.columns = new HashMap<>(this.columns);
        copy.metadata = new HashMap<>(this.metadata);
        return copy;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamRecord that = (StreamRecord) o;
        return Objects.equals(streamName, that.streamName) &&
               Objects.equals(action, that.action) &&
               Objects.equals(rowId, that.rowId) &&
               Objects.equals(streamOffset, that.streamOffset) &&
               Objects.equals(columns, that.columns);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(streamName, action, rowId, streamOffset, columns);
    }
    
    @Override
    public String toString() {
        return "StreamRecord{" +
               "streamName='" + streamName + '\'' +
               ", timestamp=" + timestamp +
               ", action='" + action + '\'' +
               ", isUpdate=" + isUpdate +
               ", rowId='" + rowId + '\'' +
               ", streamOffset=" + streamOffset +
               ", columns=" + columns.size() +
               ", metadata=" + metadata.size() +
               '}';
    }
}
