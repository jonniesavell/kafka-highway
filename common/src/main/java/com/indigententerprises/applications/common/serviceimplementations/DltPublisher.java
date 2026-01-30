package com.indigententerprises.applications.common.serviceimplementations;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public final class DltPublisher implements AutoCloseable {
    private final KafkaProducer<String, String> producer;
    private final String dltTopic;

    public DltPublisher(
            final KafkaProducer<String, String> producer,
            final String dltTopic
    ) {
        this.producer = producer;
        this.dltTopic = dltTopic;
    }

    /**
     * publish to DLT, forcing the DLT partition to match the source partition.
     *
     * important: your DLT topic must have at least as many partitions as the source topic,
     * otherwise this can fail with an invalid partition error.
     */
    public void publishBlocking(
            final String key,
            final String originalJson,
            final String errorKind,
            final String errorDetail,
            final String sourceTopic,
            final int sourcePartition,
            final long sourceOffset
    ) throws ExecutionException, InterruptedException {
        final ProducerRecord<String, String> record =
                new ProducerRecord<>(dltTopic, key, originalJson);

        record.headers().add("dlt.errorKind", errorKind.getBytes(StandardCharsets.UTF_8));
        record.headers().add("dlt.errorDetail", errorDetail.getBytes(StandardCharsets.UTF_8));
        record.headers().add("dlt.sourceTopic", sourceTopic.getBytes(StandardCharsets.UTF_8));
        record.headers().add("dlt.sourcePartition", Integer.toString(sourcePartition).getBytes(StandardCharsets.UTF_8));
        record.headers().add("dlt.sourceOffset", Long.toString(sourceOffset).getBytes(StandardCharsets.UTF_8));

        final Future<RecordMetadata> future = producer.send(record);
        future.get(); // ensure DLT write completes before committing source offsets
    }

    @Override
    public void close() throws Exception {
        producer.close();
    }
}

