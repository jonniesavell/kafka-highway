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

/**
 * TODO: this is a synchronous transmission; look at the send call ...
 * TODO: change this later.
 */
public final class OfframpPublisher {

    private final ObjectMapper objectMapper;
    private final CompiledRegistry compiledRegistry;
    private final KafkaProducer<String, String> producer;
    private final String offrampTopic;

    public OfframpPublisher(
            final ObjectMapper objectMapper,
            final CompiledRegistry compiledRegistry,
            final KafkaProducer<String, String> producer,
            final String offrampTopic) {
        this.objectMapper = objectMapper;
        this.compiledRegistry = compiledRegistry;
        this.producer = producer;
        this.offrampTopic = offrampTopic;
    }

    public <T> void send(
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
                final String jsonOutput = objectMapper.writeValueAsString(env);
                producer.send(new ProducerRecord<>(offrampTopic, key, jsonOutput)).get();
            }
        } catch (IllegalArgumentException e) {
            throw new TypeConformanceException("invalid type", e);
        }
    }
}
