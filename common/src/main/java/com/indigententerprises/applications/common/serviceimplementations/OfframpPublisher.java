package com.indigententerprises.applications.common.serviceimplementations;

import com.indigententerprises.applications.common.serviceinterfaces.TypeConformanceException;
import com.indigententerprises.applications.common.domain.CompiledEntry;
import com.indigententerprises.applications.common.domain.Envelope;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.networknt.schema.Error;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public final class OfframpPublisher {

    private final ObjectMapper objectMapper;
    private final CompiledRegistry compiledRegistry;
    private final KafkaProducer<String, String> producer;

    public OfframpPublisher(
            final ObjectMapper objectMapper,
            final CompiledRegistry compiledRegistry,
            final KafkaProducer<String, String> producer) {
        this.objectMapper = objectMapper;
        this.compiledRegistry = compiledRegistry;
        this.producer = producer;
    }

    public <T> void send(
            final String topic,
            final String key,
            final String type,
            final int version,
            final T payload,
            final String correlationId
    ) throws Exception {
        final JsonNode payloadNode = objectMapper.valueToTree(payload);

        try {
            final CompiledEntry compiledEntry = compiledRegistry.require(type, version);
            final List<Error> errors = compiledEntry.getSchema().validate(payloadNode);

            if (!errors.isEmpty()) {
                throw new TypeConformanceException(errors.getFirst().getMessage(), null);
            } else {
                final Envelope env =
                        new Envelope(
                                type,
                                version,
                                UUID.randomUUID().toString(),
                                Instant.now(),
                                correlationId,
                                payloadNode
                        );
                final String json = objectMapper.writeValueAsString(env);
                producer.send(new ProducerRecord<>(topic, key, json));
            }
        } catch (IllegalArgumentException e) {
            throw new TypeConformanceException("invalid type @JsonPublisher", e);
        }
    }
}
