package com.indigententerprises.applications.common.infrastructure;

import com.indigententerprises.applications.common.serviceimplementations.CompiledRegistry;
import com.indigententerprises.applications.common.serviceimplementations.DltPublisher;
import com.indigententerprises.applications.common.serviceimplementations.OfframpPublisher;
import com.indigententerprises.applications.common.serviceinterfaces.DuplicateEntryException;
import com.indigententerprises.applications.common.serviceinterfaces.IgnoredEntryException;
import com.indigententerprises.applications.common.serviceinterfaces.KafkaOutboxService;

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

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public final class HighwayConsumer implements Runnable, ApplicationContextAware {

    private static final Logger log = LoggerFactory.getLogger(HighwayConsumer.class);

    private final KafkaConsumer<String, String> consumer;
    private final KafkaOutboxService kafkaOutboxService;
    private final String highwayTopic;

    private ApplicationContext applicationContext;

    public HighwayConsumer(
            final String bootstrapServers,
            final String groupId,
            final String highwayTopic,
            final KafkaOutboxService kafkaOutboxService
    ) {
        this.highwayTopic = highwayTopic;
        this.kafkaOutboxService = kafkaOutboxService;

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
}
