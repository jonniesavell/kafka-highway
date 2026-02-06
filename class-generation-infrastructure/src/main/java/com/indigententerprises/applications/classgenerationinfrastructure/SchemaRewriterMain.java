package com.indigententerprises.applications.classgenerationinfrastructure;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

/**
 * Rewrites canonical schemas (URN $id/$ref) into codegen-friendly schemas
 * (classpath $id/$ref) and writes them to a resources directory.
 *
 * Intended usage (from Maven exec plugin):
 *   java ... SchemaRewriterMain <inputDir> <outputDir> <classpathPrefix>
 *
 * Example:
 *   SchemaRewriterMain contracts src/main/resources/contracts-codegen classpath:/contracts-codegen/
 */
public final class SchemaRewriterMain {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(JsonParser.Feature.ALLOW_COMMENTS);

    private SchemaRewriterMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: SchemaRewriterMain <inputDir> <outputDir> <classpathPrefix>");
            System.err.println("Example: SchemaRewriterMain contracts src/main/resources/contracts-codegen classpath:/contracts-codegen/");
            System.exit(2);
        }

        Path inputDir = Paths.get(args[0]).toAbsolutePath().normalize();
        Path outputDir = Paths.get(args[1]).toAbsolutePath().normalize();
        String classpathPrefix = args[2];

        if (!classpathPrefix.startsWith("classpath:/")) {
            throw new IllegalArgumentException("classpathPrefix must start with 'classpath:/'");
        }
        if (!classpathPrefix.endsWith("/")) {
            classpathPrefix = classpathPrefix + "/";
        }
        if (!Files.isDirectory(inputDir)) {
            throw new IllegalArgumentException("inputDir does not exist or is not a directory: " + inputDir);
        }

        System.out.println("[schema-rewrite] inputDir     = " + inputDir);
        System.out.println("[schema-rewrite] outputDir    = " + outputDir);
        System.out.println("[schema-rewrite] classpathUri = " + classpathPrefix);

        // 1) Build mapping: schemaUrn ($id) -> relative file path
        Map<String, String> urnToRelPath = buildUrnToRelPath(inputDir);

        // 2) Rewrite all schemas
        rewriteAllSchemas(inputDir, outputDir, classpathPrefix, urnToRelPath);

        System.out.println("[schema-rewrite] done. mapped " + urnToRelPath.size() + " schema ids.");
    }

    private static Map<String, String> buildUrnToRelPath(Path inputDir) throws IOException {
        Map<String, String> map = new LinkedHashMap<>();

        Files.walk(inputDir)
                .filter(Files::isRegularFile)
                .filter(p -> p.getFileName().toString().endsWith(".json"))
                .forEach(path -> {
                    try {
                        JsonNode root = MAPPER.readTree(path.toFile());
                        JsonNode idNode = root.get("$id");
                        if (idNode == null || !idNode.isTextual()) {
                            throw new IllegalStateException("Missing or non-textual $id in: " + path);
                        }
                        String urn = idNode.asText().trim();
                        if (!urn.startsWith("urn:")) {
                            throw new IllegalStateException("$id is not a URN in canonical schema: " + urn + " (file: " + path + ")");
                        }

                        String relPath = inputDir.relativize(path).toString().replace('\\', '/');

                        if (map.containsKey(urn) && !map.get(urn).equals(relPath)) {
                            throw new IllegalStateException("Duplicate $id URN with different files: " + urn +
                                    " -> " + map.get(urn) + " and " + relPath);
                        }

                        map.put(urn, relPath);
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to parse JSON: " + path, e);
                    }
                });

        if (map.isEmpty()) {
            throw new IllegalStateException("No schemas discovered under: " + inputDir);
        }

        // Print a small summary for sanity
        System.out.println("[schema-rewrite] discovered canonical schema ids:");
        for (Map.Entry<String, String> e : map.entrySet()) {
            System.out.println("  " + e.getKey() + " -> " + e.getValue());
        }

        return map;
    }

    private static void rewriteAllSchemas(
            Path inputDir,
            Path outputDir,
            String classpathPrefix,
            Map<String, String> urnToRelPath
    ) throws IOException {

        Files.createDirectories(outputDir);

        Files.walk(inputDir)
                .filter(Files::isRegularFile)
                .filter(p -> p.getFileName().toString().endsWith(".json"))
                .forEach(path -> {
                    try {
                        ObjectNode root = (ObjectNode) MAPPER.readTree(path.toFile());

                        // Rewrite $id (URN -> classpath:/.../relativePath)
                        rewriteId(root, inputDir, path, classpathPrefix);

                        // Rewrite all $ref values anywhere in the tree (URN -> classpath)
                        JsonNode rewritten = rewriteRefsRecursively(root, classpathPrefix, urnToRelPath);

                        // Keep canonical URN around as metadata (optional but useful)
                        // If you dislike it, delete these two lines.
                        String canonicalUrn = MAPPER.readTree(path.toFile()).get("$id").asText();
                        ((ObjectNode) rewritten).put("x-canonical-urn", canonicalUrn);

                        // Write to output path preserving directory structure
                        Path rel = inputDir.relativize(path);
                        Path outPath = outputDir.resolve(rel);
                        Files.createDirectories(outPath.getParent());

                        MAPPER.writerWithDefaultPrettyPrinter().writeValue(outPath.toFile(), rewritten);

                    } catch (IOException e) {
                        throw new RuntimeException("Failed rewriting schema: " + path, e);
                    }
                });
    }

    private static void rewriteId(ObjectNode root, Path inputDir, Path filePath, String classpathPrefix) {
        JsonNode idNode = root.get("$id");
        if (idNode == null || !idNode.isTextual()) {
            throw new IllegalStateException("Missing or non-textual $id in: " + filePath);
        }

        String relPath = inputDir.relativize(filePath).toString().replace('\\', '/');
        String newId = classpathPrefix + relPath;

        root.put("$id", newId);
    }

    private static JsonNode rewriteRefsRecursively(
            JsonNode node,
            String classpathPrefix,
            Map<String, String> urnToRelPath
    ) {
        if (node == null) return NullNode.getInstance();

        if (node.isObject()) {
            ObjectNode obj = (ObjectNode) node.deepCopy();
            Iterator<Map.Entry<String, JsonNode>> fields = obj.fields();
            List<String> fieldNames = new ArrayList<>();
            while (fields.hasNext()) {
                fieldNames.add(fields.next().getKey());
            }

            for (String name : fieldNames) {
                JsonNode child = obj.get(name);

                if ("$ref".equals(name) && child != null && child.isTextual()) {
                    String ref = child.asText().trim();
                    obj.put("$ref", rewriteRefValue(ref, classpathPrefix, urnToRelPath));
                } else {
                    obj.set(name, rewriteRefsRecursively(child, classpathPrefix, urnToRelPath));
                }
            }
            return obj;
        }

        if (node.isArray()) {
            ArrayNode arr = (ArrayNode) node.deepCopy();
            for (int i = 0; i < arr.size(); i++) {
                arr.set(i, rewriteRefsRecursively(arr.get(i), classpathPrefix, urnToRelPath));
            }
            return arr;
        }

        return node; // primitive
    }

    private static String rewriteRefValue(
            String ref,
            String classpathPrefix,
            Map<String, String> urnToRelPath
    ) {
        // Leave fragment-only and relative refs alone
        if (ref.startsWith("#") || (!ref.startsWith("urn:") && !ref.contains(":"))) {
            return ref;
        }

        // Already classpath/http/file? Leave it.
        if (ref.startsWith("classpath:") || ref.startsWith("file:") || ref.startsWith("http:") || ref.startsWith("https:")) {
            return ref;
        }

        // Handle URN with optional fragment
        if (ref.startsWith("urn:")) {
            String base = ref;
            String fragment = "";

            int hash = ref.indexOf('#');
            if (hash >= 0) {
                base = ref.substring(0, hash);
                fragment = ref.substring(hash); // includes '#'
            }

            String relPath = urnToRelPath.get(base);
            if (relPath == null) {
                throw new IllegalArgumentException("Unknown URN in $ref: " + base + " (full ref: " + ref + ")");
            }

            return classpathPrefix + relPath + fragment;
        }

        // Anything else: keep as-is (conservative).
        return ref;
    }
}

