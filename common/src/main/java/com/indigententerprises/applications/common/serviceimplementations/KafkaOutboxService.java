package com.indigententerprises.applications.common.serviceimplementations;

import com.fasterxml.jackson.databind.JsonNode;
import com.indigententerprises.applications.common.domain.CompiledEntry;
import com.indigententerprises.applications.common.repositories.OutboxRepository;
import com.indigententerprises.applications.common.serviceinterfaces.DuplicateEntryException;
import com.indigententerprises.applications.common.domain.DestinationKind;
import com.indigententerprises.applications.common.domain.ErrorKind;
import com.indigententerprises.applications.common.domain.OutboxRecord;
import com.indigententerprises.applications.common.domain.OutboxStatus;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.transaction.annotation.Transactional;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.networknt.schema.Error;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;

public class KafkaOutboxService
        implements com.indigententerprises.applications.common.serviceinterfaces.KafkaOutboxService {

    private final ObjectMapper objectMapper;
    private final OutboxRepository outboxRepository;
    private final CompiledRegistry registry;
    private final String offrampTopic;
    private final String dltTopic;

    public KafkaOutboxService(
            final ObjectMapper objectMapper,
            final OutboxRepository outboxRepository,
            final CompiledRegistry registry,
            final String offrampTopic,
            final String dltTopic
    ) {
        this.objectMapper = objectMapper;
        this.outboxRepository = outboxRepository;
        this.registry = registry;
        this.offrampTopic = offrampTopic;
        this.dltTopic = dltTopic;
    }

    @Override
    @Transactional
    public OutboxRecord insert(final ConsumerRecord<String, String> consumerRecord) throws DuplicateEntryException {
        final OutboxRecord outboxRecord = new OutboxRecord();
        outboxRecord.setSourceTopic(consumerRecord.topic());
        outboxRecord.setSourcePartition(consumerRecord.partition());
        outboxRecord.setSourceOffset(consumerRecord.offset());

        outboxRecord.setDestinationKey(consumerRecord.key()); // preserve

        outboxRecord.setEnvelopeJson(consumerRecord.value());
        outboxRecord.setStatus(OutboxStatus.PENDING.toString());

        try {
            final JsonNode root = objectMapper.readTree(consumerRecord.value());
            final JsonNode typeNode = root.get("type");
            final JsonNode versionNode = root.get("v");
            final JsonNode payloadNode = root.get("payload");
            final String eventType = typeNode == null ? null : typeNode.asText();
            final Integer version = versionNode == null ? null : versionNode.asInt();

            if (eventType == null || version == null) {
                outboxRecord.setDestinationKind(DestinationKind.DLT.name());
                outboxRecord.setDestinationTopic(dltTopic);
                outboxRecord.setEventType(eventType);
                outboxRecord.setVersion(version);
                outboxRecord.setValidationOk(false);
                outboxRecord.setErrorKind(ErrorKind.UNKNOWN_TYPE_VERSION.toString());
                outboxRecord.setErrorDetail("event type or version is null");
            } else {
                try {
                    final CompiledEntry entry = registry.require(eventType, version);
                    outboxRecord.setEventType(entry.getEventType());
                    outboxRecord.setVersion(entry.getVersion());

                    final List<Error> errors = entry.getSchema().validate(payloadNode);
                    outboxRecord.setValidationOk(errors.isEmpty());

                    if (errors.isEmpty()) {
                        outboxRecord.setDestinationKind(DestinationKind.OFFRAMP.name());
                        outboxRecord.setDestinationTopic(offrampTopic);
                        outboxRecord.setErrorKind(null);
                        outboxRecord.setErrorDetail(null);
                    } else {
                        outboxRecord.setDestinationKind(DestinationKind.DLT.name());
                        outboxRecord.setDestinationTopic(dltTopic);
                        outboxRecord.setErrorKind(ErrorKind.SCHEMA_INVALID.toString());
                        outboxRecord.setErrorDetail(errors.getFirst().getMessage());
                    }
                } catch (IllegalArgumentException e) {
                    outboxRecord.setDestinationKind(DestinationKind.DLT.name());
                    outboxRecord.setDestinationTopic(dltTopic);
                    outboxRecord.setEventType(eventType);
                    outboxRecord.setVersion(version);
                    outboxRecord.setValidationOk(false);
                    outboxRecord.setErrorKind(ErrorKind.UNKNOWN_TYPE_VERSION.toString());
                    outboxRecord.setErrorDetail("no meaningful information provided");
                }
            }
        } catch (JsonProcessingException e) {
            outboxRecord.setDestinationKind(DestinationKind.DLT.name());
            outboxRecord.setDestinationTopic(dltTopic);
            outboxRecord.setEventType(null);
            outboxRecord.setVersion(null);
            outboxRecord.setValidationOk(false);
            outboxRecord.setErrorKind(ErrorKind.UNKNOWN_TYPE_VERSION.toString());
            outboxRecord.setErrorDetail("cannot even extract event-type and version from malformed payload");
        }

        outboxRecord.setAttemptCount(0);
        outboxRecord.setLastAttemptedAt(null);

        try {
            return outboxRepository.saveAndFlush(outboxRecord);
        } catch (DataIntegrityViolationException e) {
            throw new DuplicateEntryException("", e);
        }
    }
}
