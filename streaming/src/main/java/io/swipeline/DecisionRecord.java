package io.swipeline;

/** Original bytes and Kafka coordinates, retained for quarantine records. */
public record DecisionRecord(byte[] value, int partition, long offset) implements java.io.Serializable {}
