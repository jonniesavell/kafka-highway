package com.indigententerprises.applications.common.serviceinterfaces;

import com.indigententerprises.applications.common.domain.DestinationKind;
import com.indigententerprises.applications.common.domain.OutboxRecord;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface KafkaOutboxService {
    OutboxRecord insert(
            final ConsumerRecord<String, String> consumerRecord,
            final DestinationKind destinationKind,
            final String destinationTopic
    ) throws DuplicateEntryException;
}
