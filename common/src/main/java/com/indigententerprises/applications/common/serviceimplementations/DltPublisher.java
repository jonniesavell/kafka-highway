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

    public DltPublisher(String bootstrapServers) {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // DLT should be durable
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        this.producer = new KafkaProducer<>(props);
    }

    /**
     * publish to DLT, forcing the DLT partition to match the source partition.
     *
     * important: your DLT topic must have at least as many partitions as the source topic,
     * otherwise this can fail with an invalid partition error.
     */
    public void publishBlocking(
            final String dltTopic,
            final int sourcePartition,
            final String key,
            final String originalJson,
            final String errorKind,
            final String errorDetail,
            final String sourceTopic,
            final long sourceOffset
    ) throws ExecutionException, InterruptedException {

        final Integer destinationPartition = Integer.valueOf(sourcePartition);
        final ProducerRecord<String, String> record =
                new ProducerRecord<>(dltTopic, destinationPartition, key, originalJson);

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

