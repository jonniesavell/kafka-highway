package com.indigententerprises.applications.classgenerationinfrastructure;

import org.jsonschema2pojo.DefaultGenerationConfig;
import org.jsonschema2pojo.Jackson2Annotator;
import org.jsonschema2pojo.SchemaStore;
import org.jsonschema2pojo.rules.RuleFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

public class CustomRuleFactory extends RuleFactory {

    private static final HashMap<URI, JsonNode> URI_TO_JSON = new HashMap<>();
    private static final CustomRuleLogger CUSTOM_RULE_LOGGER = new CustomRuleLogger();

    static {
        if (System.getProperty("user.dir") == null) {
            throw new IllegalStateException("system property user.dir is not set");
        } else {
            final ObjectMapper mapper = new ObjectMapper();
            final Path base = Paths.get(System.getProperty("user.dir")).resolve("shared").resolve("contracts");

            final File [] files = base.toFile().listFiles();

            if (files == null) {
                throw new IllegalStateException("files underneath contracts not found");
            } else {
                for (final File file : files) {
                    if (file.isDirectory()) {
                        final File directory = file;
                        final File [] jsonFiles = directory.listFiles(new FileFilter() {
                            @Override
                            public boolean accept(final File pathname) {
                                return pathname.isFile() && pathname.getName().endsWith(".json");
                            }
                        });

                        if (jsonFiles != null) {
                            // assume shallow directories
                            for (final File jsonFile : jsonFiles) {
                                try {
                                    final JsonNode jsonNode = mapper.readTree(jsonFile);
                                    final JsonNode id = jsonNode.get("$id");
                                    final String idValue = id.asText();
                                    final URI uri = URI.create(idValue);
                                    URI_TO_JSON.put(uri, jsonNode);

                                    CUSTOM_RULE_LOGGER.info(String.format("uri: %s", uri.toString()));
                                } catch (final IOException | IllegalArgumentException ignored) {
                                    CUSTOM_RULE_LOGGER.info(String.format("json-file bombed: %s", jsonFile.getAbsolutePath()));
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public CustomRuleFactory() {
        super(
                new DefaultGenerationConfig(),
                new Jackson2Annotator(new DefaultGenerationConfig()),
                new CustomSchemaStore(
                        new UrnContentResolver(URI_TO_JSON, CUSTOM_RULE_LOGGER),
                        CUSTOM_RULE_LOGGER
                )
        );
    }

    @Override
    public void setSchemaStore(SchemaStore schemaStore) {
        // do nothing. keep my schema-store!
        CUSTOM_RULE_LOGGER.info(String.format("their schemaStore: %s (not mine)", schemaStore));
    }
}
