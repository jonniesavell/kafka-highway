package com.indigententerprises.applications.common.domain;

public final class SchemaRow {
    private final String schemaId;
    private final String jsonSchema;

    public SchemaRow(final String schemaId, final String jsonSchema) {
        this.schemaId = schemaId;
        this.jsonSchema = jsonSchema;
    }

    public String getSchemaId() {
        return schemaId;
    }

    public String getJsonSchema() {
        return jsonSchema;
    }
}
