package com.indigententerprises.applications.common.infrastructure;

import com.indigententerprises.applications.common.serviceimplementations.CompiledRegistry;
import com.indigententerprises.applications.common.serviceimplementations.DltPublisher;
import com.indigententerprises.applications.common.serviceimplementations.OfframpPublisher;
import com.indigententerprises.applications.common.serviceinterfaces.DuplicateEntryException;
import com.indigententerprises.applications.common.serviceinterfaces.IgnoredEntryException;
import com.indigententerprises.applications.common.serviceinterfaces.KafkaOutboxService;
import com.indigententerprises.applications.common.domain.CompiledEntry;

import org.springframework.beans.BeansException;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.networknt.schema.Error;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public final class HighwayConsumer implements Runnable, ApplicationContextAware {

    private static final Logger log = LoggerFactory.getLogger(HighwayConsumer.class);

    private final KafkaConsumer<String, String> consumer;
    private final KafkaOutboxService kafkaOutboxService;
    private final ObjectMapper objectMapper;
    private final CompiledRegistry registry;
    private final OfframpPublisher offrampPublisher;
    private final DltPublisher dltPublisher;
    private final String highwayTopic;

    private ApplicationContext applicationContext;

    public HighwayConsumer(
            final String bootstrapServers,
            final String groupId,
            final String highwayTopic,
            final KafkaOutboxService kafkaOutboxService,
            final ObjectMapper objectMapper,
            final CompiledRegistry registry,
            final OfframpPublisher offrampPublisher,
            final DltPublisher dltPublisher
    ) {
        this.highwayTopic = highwayTopic;
        this.kafkaOutboxService = kafkaOutboxService;
        this.objectMapper = objectMapper;
        this.registry = registry;
        this.offrampPublisher = offrampPublisher;
        this.dltPublisher = dltPublisher;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");

        this.consumer = new KafkaConsumer<String, String>(props);
    }

    @Override
    public void setApplicationContext(final ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(Collections.singletonList(highwayTopic));

            try {
                while (!Thread.currentThread().isInterrupted()) {
                    final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();

                    for (final ConsumerRecord<String, String> record : records) {
                        final TopicPartition tp = new TopicPartition(record.topic(), record.partition());

                        try {
                            kafkaOutboxService.insert(record);
                        } catch (DuplicateEntryException | IgnoredEntryException ignore) {}

                        offsetsToCommit.put(tp, new OffsetAndMetadata(record.offset() + 1));
                    }

                    consumer.commitSync(offsetsToCommit);
                }
            } catch (WakeupException ignored) {
                // shutdown
            } finally {
                consumer.close();
            }
        } catch (RuntimeException e) {
            // TODO: policy: KafkaOutboxService throws RuntimeException
            log.error("unexpected error occurred during consumption", e);

            SpringApplication.exit(applicationContext, () -> 1);
            System.exit(1);
        }
    }

    /**
     * TODO: dead code
     */
    private boolean handleRecord(final ConsumerRecord<String, String> record) {
        final String key = record.key();
        final String json = record.value();

        try {
            final JsonNode root = objectMapper.readTree(json);
            final JsonNode typeNode = root.get("type");
            final JsonNode versionNode = root.get("v");
            final JsonNode payloadNode = root.get("payload");
            final JsonNode correlationIdNode = root.get("correlationId");

            if (typeNode == null || versionNode == null || payloadNode == null) {
                dltPublisher.publishBlocking(
                        key,
                        json,
                        "ENVELOPE_INVALID",
                        "Missing required fields: type, v, payload",
                        record.topic(),
                        record.partition(),
                        record.offset()
                );
                return true;
            }

            final String eventType = typeNode.asText();
            final int version = versionNode.asInt();
            final String correlationId = correlationIdNode == null ? null : correlationIdNode.asText();
            final CompiledEntry entry;

            try {
                entry = registry.require(eventType, version);
            } catch (IllegalArgumentException e) {
                dltPublisher.publishBlocking(
                        key,
                        json,
                        "UNKNOWN_TYPE_VERSION",
                        e.getMessage(),
                        record.topic(),
                        record.partition(),
                        record.offset()
                );
                return true;
            }

            // validate BEFORE binding
            final List<Error> errors = entry.getSchema().validate(payloadNode);

            if (!errors.isEmpty()) {
                // abbreviated error
                final String errorDetail = errors.iterator().next().getMessage();
                dltPublisher.publishBlocking(
                        key,
                        json,
                        "SCHEMA_INVALID",
                        errorDetail,
                        record.topic(),
                        record.partition(),
                        record.offset()
                );
                return true;
            }

            // TODO: route/transform to roadways here: at highway speed, keep it lean
            final Object payloadPojo = objectMapper.treeToValue(payloadNode, entry.getPayloadClass());
            offrampPublisher.send(
                    record.key(),
                    eventType,
                    version,
                    payloadPojo,
                    correlationId
            );

            return true;
        } catch (Exception e) {
            try {
                dltPublisher.publishBlocking(
                        key,
                        json,
                        "EXCEPTION",
                        e.getClass().getName() + ": " + e.getMessage(),
                        record.topic(),
                        record.partition(),
                        record.offset()
                );
                return true;
            } catch (Exception dltEx) {
                // DLT publish failed; do NOT commit. Let re-delivery happen.
                return false;
            }
        }
    }
}
