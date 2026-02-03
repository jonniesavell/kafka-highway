package com.indigententerprises.applications.shared.infrastructure;

import org.jsonschema2pojo.SchemaStore;
import org.jsonschema2pojo.rules.RuleFactory;

import com.fasterxml.jackson.databind.JsonNode;

import java.net.URI;
import java.util.HashMap;

public class CustomRuleFactory extends RuleFactory {

    public CustomRuleFactory() {
        super();

        final HashMap<URI, JsonNode> uriToJson = new HashMap<>();
        // iterate over a portion of the filesystem and populate the Map:

        super.setSchemaStore(
                new SchemaStore(
                        new UrnContentResolver(uriToJson),
                        new CustomRuleLogger()
                )
        );
    }
}
