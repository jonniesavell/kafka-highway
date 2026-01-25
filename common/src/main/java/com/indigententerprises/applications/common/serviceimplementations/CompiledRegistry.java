package com.indigententerprises.applications.common.serviceimplementations;

import com.indigententerprises.applications.common.infrastructure.SchemaRegistryFactory;
import com.indigententerprises.applications.common.domain.CompiledEntry;
import com.indigententerprises.applications.common.domain.RegistryRow;

import com.fasterxml.jackson.databind.JsonNode;

import com.networknt.schema.Schema;
import com.networknt.schema.InputFormat;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
//import com.networknt.schema.resource.

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class CompiledRegistry {
    private final Map<String, CompiledEntry> entriesByKey;

    public CompiledRegistry(final List<RegistryRow> rows) {
        //final SchemaRegistry schemaRegistry = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12);
        final SchemaRegistry schemaRegistry = SchemaRegistryFactory.createHttpRefRegistry();
        final Map<String, CompiledEntry> map = new HashMap<>();

        for (RegistryRow row : rows) {
            final String key = key(row.getEventType(), row.getVersion());
            final JsonNode schemaNode = row.getJsonSchema();
            final Schema schema = schemaRegistry.getSchema(schemaNode.toString(), InputFormat.JSON);;
            final Class<?> payloadClass;

            try {
                payloadClass = Class.forName(row.getPayloadClass());
                map.put(key, new CompiledEntry(row.getEventType(), row.getVersion(), payloadClass, schema));
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException("payload_class not found on classpath: " + row.getPayloadClass(), e);
            }
        }

        this.entriesByKey = Map.copyOf(map);
    }

    public CompiledEntry require(String eventType, int version) throws IllegalArgumentException {
        final String key = key(eventType, version);
        final CompiledEntry entry = entriesByKey.get(key);

        if (entry == null) {
            throw new IllegalArgumentException("unknown event type/version: " + eventType + " v" + version);
        } else {
            return entry;
        }
    }

    private static String key(String eventType, int version) {
        return eventType + "#" + version;
    }
}
