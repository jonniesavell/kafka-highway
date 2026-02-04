package com.indigententerprises.applications.classgenerationinfrastructure;

import com.fasterxml.jackson.databind.JsonNode;
import org.jsonschema2pojo.ContentResolver;
import org.jsonschema2pojo.RuleLogger;
import org.jsonschema2pojo.Schema;
import org.jsonschema2pojo.SchemaStore;

import java.net.URI;
import java.net.URISyntaxException;

import static org.apache.commons.lang3.StringUtils.stripEnd;
import static org.apache.commons.lang3.StringUtils.substringAfter;
import static org.apache.commons.lang3.StringUtils.substringBefore;

public class CustomSchemaStore extends SchemaStore {
    private final ContentResolver contentResolver;
    private final RuleLogger ruleLogger;

    public CustomSchemaStore(final ContentResolver contentResolver, final RuleLogger logger) {
        super(contentResolver, logger);

        this.contentResolver = contentResolver;
        this.ruleLogger = logger;
    }

    @Override
    public synchronized Schema create(URI id, String refFragmentPathDelimiters) {
        ruleLogger.info("content-resolver inspection: " + (super.contentResolver == this.contentResolver));

        URI normalizedId = id.normalize();

        if (!schemas.containsKey(normalizedId)) {

            URI baseId = removeFragment(id).normalize();
            if (!schemas.containsKey(baseId)) {
                logger.debug("Reading schema: " + baseId);
                final JsonNode baseContent = this.contentResolver.resolve(baseId);
                schemas.put(baseId, new Schema(baseId, baseContent, null));
            }

            final Schema baseSchema = schemas.get(baseId);
            if (normalizedId.toString().contains("#")) {
                JsonNode childContent = fragmentResolver.resolve(baseSchema.getContent(), '#' + id.getFragment(), refFragmentPathDelimiters);
                schemas.put(normalizedId, new Schema(normalizedId, childContent, baseSchema));
            }
        }

        return schemas.get(normalizedId);
    }

    @Override
    public Schema create(Schema parent, String path, String refFragmentPathDelimiters) {
        ruleLogger.info("content-resolver inspection: " + (super.contentResolver == this.contentResolver));

        if (!path.equals("#")) {
            // if path is an empty string then resolving it below results in jumping up a level. e.g. "/path/to/file.json" becomes "/path/to"
            path = stripEnd(path, "#?&/");
        }

        // encode the fragment for any funny characters
        if (path.contains("#")) {
            String pathExcludingFragment = substringBefore(path, "#");
            String fragment = substringAfter(path, "#");
            URI fragmentURI;
            try {
                fragmentURI = new URI(null, null, fragment);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid fragment: " + fragment + " in path: " + path);
            }
            path = pathExcludingFragment + "#" + fragmentURI.getRawFragment();
        }

        URI id = (parent == null || parent.getId() == null) ? URI.create(path) : parent.getId().resolve(path);

        String stringId = id.toString();
        if (stringId.endsWith("#")) {
            try {
                id = new URI(stripEnd(stringId, "#"));
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Bad path: " + stringId);
            }
        }

        if (selfReferenceWithoutParentFile(parent, path) || substringBefore(stringId, "#").isEmpty()) {
            JsonNode parentContent = parent.getGrandParent().getContent();

            if (schemas.containsKey(id)) {
                return schemas.get(id);
            } else {
                Schema schema =
                        new Schema(
                                id,
                                fragmentResolver.resolve(parentContent, path, refFragmentPathDelimiters),
                                parent.getGrandParent()
                        );
                schemas.put(id, schema);
                return schema;
            }
        }

        return create(id, refFragmentPathDelimiters);
    }
}
