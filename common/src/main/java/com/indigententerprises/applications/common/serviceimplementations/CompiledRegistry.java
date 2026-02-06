package com.indigententerprises.applications.common.serviceimplementations;

import com.indigententerprises.applications.common.infrastructure.SchemaRegistryFactory;
import com.indigententerprises.applications.common.domain.CompiledEntry;
import com.indigententerprises.applications.common.domain.RegistryRow;

import com.networknt.schema.Schema;
import com.networknt.schema.InputFormat;
import com.networknt.schema.SchemaRegistry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public final class CompiledRegistry {
    private final Map<String, CompiledEntry> entriesByKey;

    public CompiledRegistry(
            final List<RegistryRow> rows,
            final Function<String, String> urnToSchemaText) {
        final SchemaRegistry schemaRegistry = SchemaRegistryFactory.createRefRegistry(urnToSchemaText);
        final Map<String, CompiledEntry> map = new HashMap<>();

        for (RegistryRow row : rows) {
            final String key = key(row.getEventType(), row.getVersion());
            final String schemaText = urnToSchemaText.apply(row.getSchemaId());

            if (schemaText == null || schemaText.isBlank()) {
                throw new IllegalStateException("schema not found for schemaId: " + row.getSchemaId());
            } else {
                final Schema schema = schemaRegistry.getSchema(schemaText, InputFormat.JSON);
                final Class<?> payloadClass;

                try {
                    payloadClass = Class.forName(row.getPayloadClass());
                    map.put(key, new CompiledEntry(row.getEventType(), row.getVersion(), payloadClass, schema));
                } catch (ClassNotFoundException e) {
                    final String message =
                            String.format(
                                    "payload_class not found on classpath: %s",
                                    row.getPayloadClass()
                            );
                    throw new IllegalStateException(message, e);
                }
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
