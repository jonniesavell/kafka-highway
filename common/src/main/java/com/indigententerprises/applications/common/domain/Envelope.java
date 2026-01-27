package com.indigententerprises.applications.common.domain;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;

/**
 * TODO: if you can control your envelope schema later, the nicest approach is often:
 *       make the field optional (not required)
 *         and have your serializer omit nulls (so the JSON doesn’t even include it)
 *       that keeps payloads smaller and avoids ambiguity.
 */
public record Envelope(
        String type,
        int v,
        String id,
        Instant ts,
        String correlationId,
        JsonNode payload
) {}
