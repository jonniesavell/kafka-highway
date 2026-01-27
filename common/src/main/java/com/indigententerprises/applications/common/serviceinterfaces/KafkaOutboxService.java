package com.indigententerprises.applications.common.serviceinterfaces;

import com.indigententerprises.applications.common.domain.OutboxRecord;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface KafkaOutboxService {
    OutboxRecord insert(final ConsumerRecord<String, String> consumerRecord)
            throws DuplicateEntryException, IgnoredEntryException;
}
