package com.indigententerprises.applications.common.infrastructure;

import com.networknt.schema.SchemaRegistry;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.function.Function;

public final class SchemaRegistryFactory {

    private SchemaRegistryFactory() {}

    public static SchemaRegistry createHttpRefRegistry() {
        final HttpClient httpClient = HttpClient.newHttpClient();

        final Function<String, String> fetchSchemaText = (String iri) -> {
            // safety: only allow your local schema server
            if (!iri.startsWith("http://localhost:8082/")) {
                throw new IllegalArgumentException("Ref fetch blocked (not allowed): " + iri);
            } else {
                try {
                    final HttpRequest request = HttpRequest.newBuilder()
                            .uri(URI.create(iri))
                            .GET()
                            .build();
                    final HttpResponse<String> response =
                            httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                    final int status = response.statusCode();

                    if (status < 200 || status >= 300) {
                        throw new IllegalStateException("ref fetch failed: " + iri + " HTTP " + status);
                    } else {
                        return response.body();
                    }
                } catch (IOException e) {
                    throw new IllegalStateException("ref fetch IO error: " + iri, e);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("ref fetch interrupted: " + iri, e);
                }
            }
        };

        // Dialect id: use the official meta-schema URL for 2020-12
        final String dialectId = "https://json-schema.org/draft/2020-12/schema";

        return SchemaRegistry.builder()
                .defaultDialectId(dialectId)
                .schemaCacheEnabled(true)
                .schemas(fetchSchemaText)
                .build();
    }
}

//    public static SchemaRegistry create() {
//        final ResourceLoaders resourceLoaders = ResourceLoaders.builder()
//                .add(new IriResourceLoader())
//                .build();
//
//        return SchemaRegistry.builder()
//                .defaultDialect(SpecificationVersion.DRAFT_2020_12)
//                .resourceLoaders(resourceLoaders)
//                .build();
//    }
//    public static SchemaRegistry create() {
//        // adjust builder method names to what IntelliJ offers in your 2.0.1 jar
//        return SchemaRegistry.builder()
//                .defaultDialect(SpecificationVersion.DRAFT_2020_12)
//                .schemaLoaders(schemaLoaders -> {
//                    schemaLoaders.add(
//                            SchemaLoader.builder()
//                                    .resourceLoader(new IriResourceLoader())
//                                    .build()
//                    );
//
//                    // If you have ClasspathResourceLoader, keep it too:
//                    // schemaLoaders.add(SchemaLoader.builder()
//                    //        .resourceLoader(new ClasspathResourceLoader())
//                    //        .build());
//                })
//                .build();
//    }