package com.indigententerprises.applications.common.domain;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;

public record Envelope(
        String type,
        int v,
        String id,
        Instant ts,
        String correlationId,
        JsonNode payload
) {}
