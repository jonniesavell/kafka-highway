package com.indigententerprises.applications.common.domain;

public final class RegistryRow {
    private final String eventType;
    private final int version;
    private final String payloadClass;
    private final String schemaId;

    public RegistryRow(
            final String eventType,
            final int version,
            final String payloadClass,
            final String schemaId) {
        this.eventType = eventType;
        this.version = version;
        this.payloadClass = payloadClass;
        this.schemaId = schemaId;
    }

    public String getEventType() {
        return eventType;
    }

    public int getVersion() {
        return version;
    }

    public String getPayloadClass() {
        return payloadClass;
    }

    public String getSchemaId() {
        return schemaId;
    }
}
