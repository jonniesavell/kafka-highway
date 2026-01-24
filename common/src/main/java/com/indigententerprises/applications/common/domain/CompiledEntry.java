package com.indigententerprises.applications.common.domain;

import com.networknt.schema.Schema;

public final class CompiledEntry {
    private final String eventType;
    private final int version;
    private final Class<?> payloadClass;
    private final Schema schema;

    public CompiledEntry(String eventType, int version, Class<?> payloadClass, Schema schema) {
        this.eventType = eventType;
        this.version = version;
        this.payloadClass = payloadClass;
        this.schema = schema;
    }

    public String getEventType() { return eventType; }
    public int getVersion() { return version; }
    public Class<?> getPayloadClass() { return payloadClass; }
    public Schema getSchema() { return schema; }
}
