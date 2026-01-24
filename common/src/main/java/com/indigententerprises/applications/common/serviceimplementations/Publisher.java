package com.indigententerprises.applications.common.serviceimplementations;

import com.indigententerprises.applications.common.domain.Envelope;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.UUID;

public final class Publisher {

    private final ObjectMapper objectMapper;
    private final KafkaProducer<String, String> producer;

    public Publisher(
            final ObjectMapper objectMapper,
            final KafkaProducer<String, String> producer) {
        this.objectMapper = objectMapper;
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
        final Envelope env =
                new Envelope(type, version, UUID.randomUUID().toString(), Instant.now(), correlationId, payloadNode);
        final String json = objectMapper.writeValueAsString(env);
        producer.send(new ProducerRecord<>(topic, key, json));
    }
}
