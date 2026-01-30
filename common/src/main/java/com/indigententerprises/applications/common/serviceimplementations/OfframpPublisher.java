package com.indigententerprises.applications.common.serviceimplementations;

import com.indigententerprises.applications.common.domain.OutboxRecord;
import com.indigententerprises.applications.common.serviceinterfaces.TypeConformanceException;
import com.indigententerprises.applications.common.domain.CompiledEntry;
import com.indigententerprises.applications.common.domain.Envelope;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.networknt.schema.Error;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * TODO: this is a synchronous transmission; look at the send call ...
 * TODO: change this later.
 */
public final class OfframpPublisher {

    private final ObjectMapper objectMapper;
    private final CompiledRegistry compiledRegistry;
    private final KafkaProducer<String, String> producer;
    private final String offrampTopic;
    private final long timeoutInMillis;

    public OfframpPublisher(
            final ObjectMapper objectMapper,
            final CompiledRegistry compiledRegistry,
            final KafkaProducer<String, String> producer,
            final String offrampTopic,
            final long timeoutInMillis) {
        this.objectMapper = objectMapper;
        this.compiledRegistry = compiledRegistry;
        this.producer = producer;
        this.offrampTopic = offrampTopic;
        this.timeoutInMillis = timeoutInMillis;
    }

    public <T> boolean send(
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
                final Future<RecordMetadata> sendFuture =
                        producer.send(new ProducerRecord<>(offrampTopic, key, jsonOutput));
                try {
                    sendFuture.get(timeoutInMillis, TimeUnit.MILLISECONDS);
                    return true;
                } catch (TimeoutException | CancellationException e) {
                    return false;
                }
            }
        } catch (IllegalArgumentException e) {
            throw new TypeConformanceException("invalid type", e);
        }
    }

    public boolean send(final OutboxRecord outboxRecord) throws InterruptedException, ExecutionException {
        final String key = outboxRecord.getDestinationKey();
        final String payload = outboxRecord.getEnvelopeJson();
        final Future<RecordMetadata> sendFuture =
                producer.send(new ProducerRecord<>(offrampTopic, key, payload));
        try {
            sendFuture.get(timeoutInMillis, TimeUnit.MILLISECONDS);
            return true;
        } catch (TimeoutException | CancellationException e) {
            return false;
        }
    }
}
