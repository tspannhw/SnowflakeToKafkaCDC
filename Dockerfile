# Multi-stage build for optimized production image
FROM maven:3.9-eclipse-temurin-21-alpine AS builder

WORKDIR /app

# Copy POM file first for better Docker layer caching
COPY pom.xml .

# Download dependencies
RUN mvn dependency:go-offline -B

# Copy source code
COPY src ./src

# Build the application
RUN mvn clean package -DskipTests

# Production image
FROM eclipse-temurin:21-jre-alpine

# Install required packages and create app user
RUN apk add --no-cache \
        curl \
        jq \
        dumb-init && \
    addgroup -S appuser && \
    adduser -S appuser -G appuser

# Set working directory
WORKDIR /app

# Copy JAR from builder stage
COPY --from=builder /app/target/snowflake-kafka-cdc-*.jar app.jar

# Copy configuration files
COPY src/main/resources/application.conf ./config/

# Create logs directory
RUN mkdir -p logs && \
    chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Environment variables for JVM tuning (JDK 21 optimized)
ENV JAVA_OPTS="-Xms1g -Xmx2g \
    -XX:+UseZGC \
    -XX:+UnlockExperimentalVMOptions \
    -XX:+UseTransparentHugePages \
    -XX:+UseStringDeduplication \
    -XX:+ExitOnOutOfMemoryError \
    -XX:+HeapDumpOnOutOfMemoryError \
    -XX:HeapDumpPath=/app/logs/ \
    --enable-preview \
    -Djava.security.egd=file:/dev/./urandom"

# Application configuration
ENV APP_CONFIG_PATH="/app/config/application.conf"
ENV LOG_LEVEL="INFO"

# Expose ports
EXPOSE 8080 8081

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8081/health || exit 1

# Use dumb-init to handle signals properly
ENTRYPOINT ["dumb-init", "--"]

# Start the application
CMD ["sh", "-c", "java $JAVA_OPTS -Dconfig.file=$APP_CONFIG_PATH -jar app.jar"]

# Labels for metadata
LABEL maintainer="Snowflake Kafka CDC Team"
LABEL version="1.0.0"
LABEL description="High-performance Snowflake to Kafka CDC streaming application"
LABEL org.opencontainers.image.source="https://github.com/your-org/snowflake-kafka-cdc"
