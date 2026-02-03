package com.indigententerprises.applications.shared.infrastructure;

import org.jsonschema2pojo.ContentResolver;

import com.fasterxml.jackson.databind.JsonNode;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

public class UrnContentResolver extends ContentResolver {

    private final Map<URI, JsonNode> uriToJson;

    public UrnContentResolver(final Map<URI, JsonNode> uriToJson) {
        super();

        this.uriToJson = uriToJson;
    }

    public JsonNode resolve(final URI uri) {

        if ("urn".equalsIgnoreCase(uri.getScheme())) {
            try {
                final URI base = new URI(uri.getScheme(), uri.getSchemeSpecificPart(), null);
                return uriToJson.get(base);
            } catch (URISyntaxException e) {
                return super.resolve(uri);
            }
        } else {
            return super.resolve(uri);
        }
    }
}
