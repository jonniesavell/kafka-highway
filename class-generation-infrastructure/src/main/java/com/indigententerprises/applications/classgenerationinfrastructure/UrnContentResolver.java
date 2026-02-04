package com.indigententerprises.applications.classgenerationinfrastructure;

import org.jsonschema2pojo.ContentResolver;

import com.fasterxml.jackson.databind.JsonNode;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

public class UrnContentResolver extends ContentResolver {

    private final Map<URI, JsonNode> uriToJson;
    private final CustomRuleLogger customRuleLogger;

    public UrnContentResolver(
            final Map<URI, JsonNode> uriToJson,
            final CustomRuleLogger customRuleLogger) {
        super();

        this.uriToJson = uriToJson;
        this.customRuleLogger = customRuleLogger;
    }

    @Override
    public JsonNode resolve(final URI uri) {
        customRuleLogger.info("uri: " + uri);

        if ("urn".equalsIgnoreCase(uri.getScheme())) {
            try {
                final URI base = new URI(uri.getScheme(), uri.getSchemeSpecificPart(), null);

                if (uriToJson.containsKey(base)) {
                    return uriToJson.get(base);
                } else {
                    throw new IllegalArgumentException(String.format("%s is not a valid URN", base));
                }
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(String.format("%s is not a valid URI", uri), e);
            }
        } else {
            return super.resolve(uri);
        }
    }
}
