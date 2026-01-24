package com.indigententerprises.applications.common.infrastructure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.indigententerprises.applications.common.serviceimplementations.DltPublisher;
import com.indigententerprises.applications.common.serviceimplementations.CompiledRegistry;
import com.indigententerprises.applications.common.domain.CompiledEntry;

import com.networknt.schema.Error;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public final class HighwayConsumer implements Runnable {
    private final KafkaConsumer<String, String> consumer;
    private final ObjectMapper objectMapper;
    private final CompiledRegistry registry;
    private final DltPublisher dltPublisher;
    private final String highwayTopic;
    private final String dltTopic;

    public HighwayConsumer(
            String bootstrapServers,
            String groupId,
            String highwayTopic,
            String dltTopic,
            ObjectMapper objectMapper,
            CompiledRegistry registry,
            DltPublisher dltPublisher
    ) {
        this.highwayTopic = highwayTopic;
        this.dltTopic = dltTopic;
        this.objectMapper = objectMapper;
        this.registry = registry;
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
    public void run() {
        consumer.subscribe(Collections.singletonList(highwayTopic));

        try {
            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<TopicPartition, OffsetAndMetadata>();

                for (ConsumerRecord<String, String> record : records) {
                    TopicPartition tp = new TopicPartition(record.topic(), record.partition());

                    boolean okToCommit = handleRecord(record);

                    if (okToCommit) {
                        // commit “next offset”
                        offsetsToCommit.put(tp, new OffsetAndMetadata(record.offset() + 1));
                    } else {
                        // If we couldn't safely handle (including DLT publish), do not commit;
                        // letting the process restart will re-deliver.
                        // sadly, we cannot do that. the SHOW must go on!
                        offsetsToCommit.put(tp, new OffsetAndMetadata(record.offset() + 1));
                    }
                }

                if (!offsetsToCommit.isEmpty()) {
                    consumer.commitSync(offsetsToCommit);
                }
            }
        } catch (WakeupException ignored) {
            // shutdown
        } finally {
            consumer.close();
        }
    }

    private boolean handleRecord(ConsumerRecord<String, String> record) {
        String json = record.value();
        String key = record.key();

        try {
            JsonNode root = objectMapper.readTree(json);

            JsonNode typeNode = root.get("type");
            JsonNode versionNode = root.get("v");
            JsonNode payloadNode = root.get("payload");

            if (typeNode == null || versionNode == null || payloadNode == null) {
                dltPublisher.publishBlocking(
                        dltTopic,
                        record.partition(),
                        key,
                        json,
                        "ENVELOPE_INVALID",
                        "Missing required fields: type, v, payload",
                        record.topic(),
                        record.offset()
                );
                return true;
            }

            final String eventType = typeNode.asText();
            final int version = versionNode.asInt();
            final CompiledEntry entry;

            try {
                entry = registry.require(eventType, version);
            } catch (IllegalArgumentException ex) {
                dltPublisher.publishBlocking(
                        dltTopic,
                        record.partition(),
                        key,
                        json,
                        "UNKNOWN_TYPE_VERSION",
                        ex.getMessage(),
                        record.topic(),
                        record.offset()
                );
                return true;
            }

            // Validate BEFORE binding
            final List<Error> errors = entry.getSchema().validate(payloadNode);

            if (!errors.isEmpty()) {
                // abbreviated error
                final String errorDetail = errors.iterator().next().getMessage();
                dltPublisher.publishBlocking(
                        dltTopic,
                        record.partition(),
                        key,
                        json,
                        "SCHEMA_INVALID",
                        errorDetail,
                        record.topic(),
                        record.offset()
                );
                return true;
            }

            // Bind after validation
            final Object payloadPojo = objectMapper.treeToValue(payloadNode, entry.getPayloadClass());

            // TODO: route/transform to roadways here.
            // (At highway speed, keep it lean; avoid per-message DB calls.)

            return true;
        } catch (Exception ex) {
            try {
                dltPublisher.publishBlocking(
                        dltTopic,
                        record.partition(),
                        key,
                        json,
                        "EXCEPTION",
                        ex.getClass().getName() + ": " + ex.getMessage(),
                        record.topic(),
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
