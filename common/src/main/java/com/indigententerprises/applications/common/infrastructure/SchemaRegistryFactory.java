package com.indigententerprises.applications.common.infrastructure;

import com.networknt.schema.SchemaRegistry;

import java.util.function.Function;

public final class SchemaRegistryFactory {

    private SchemaRegistryFactory() {}

    public static SchemaRegistry createRefRegistry(final Function<String, String> schemaLookup) {
        final Function<String, String> fetchSchemaText = (String iri) -> {
            // safety: only allow your local schema
            // TODO: what if we later allow other schema? we will have to relax this constraint.
            if (!iri.startsWith("urn:indigententerprises")) {
                throw new IllegalArgumentException("reference not allowed: " + iri);
            } else {
                return schemaLookup.apply(iri);
            }
        };

        // dialect id: use the official meta-schema URL for 2020-12
        final String dialectId = "https://json-schema.org/draft/2020-12/schema";

        return SchemaRegistry.builder()
                .defaultDialectId(dialectId)
                .schemaCacheEnabled(true)
                .schemas(fetchSchemaText)
                .build();
    }
}
