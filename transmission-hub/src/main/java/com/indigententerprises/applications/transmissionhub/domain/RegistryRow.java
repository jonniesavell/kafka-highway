package com.indigententerprises.applications.transmissionhub.domain;

import com.fasterxml.jackson.databind.JsonNode;

public final class RegistryRow {
    private final String eventType;
    private final int version;
    private final String payloadClass;
    private final JsonNode jsonSchema;

    public RegistryRow(String eventType, int version, String payloadClass, JsonNode jsonSchema) {
        this.eventType = eventType;
        this.version = version;
        this.payloadClass = payloadClass;
        this.jsonSchema = jsonSchema;
    }

    public String getEventType() { return eventType; }
    public int getVersion() { return version; }
    public String getPayloadClass() { return payloadClass; }
    public JsonNode getJsonSchema() { return jsonSchema; }
}
