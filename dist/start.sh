#!/bin/bash
# Startup script for Snowflake Kafka CDC

# Set JVM options for production
export JAVA_OPTS="${JAVA_OPTS} -Xms2g -Xmx4g"
export JAVA_OPTS="${JAVA_OPTS} -XX:+UseG1GC"
export JAVA_OPTS="${JAVA_OPTS} -XX:MaxGCPauseMillis=100"
export JAVA_OPTS="${JAVA_OPTS} -XX:+UseStringDeduplication"
export JAVA_OPTS="${JAVA_OPTS} -XX:+ExitOnOutOfMemoryError"
export JAVA_OPTS="${JAVA_OPTS} -XX:+HeapDumpOnOutOfMemoryError"
export JAVA_OPTS="${JAVA_OPTS} -XX:HeapDumpPath=./logs/"
export JAVA_OPTS="${JAVA_OPTS} -Djava.security.egd=file:/dev/./urandom"

# Enable JMX monitoring
export JAVA_OPTS="${JAVA_OPTS} -Dcom.sun.management.jmxremote"
export JAVA_OPTS="${JAVA_OPTS} -Dcom.sun.management.jmxremote.port=9999"
export JAVA_OPTS="${JAVA_OPTS} -Dcom.sun.management.jmxremote.authenticate=false"
export JAVA_OPTS="${JAVA_OPTS} -Dcom.sun.management.jmxremote.ssl=false"

# Create logs directory
mkdir -p logs

# Start application
java $JAVA_OPTS -jar snowflake-kafka-cdc-*.jar
