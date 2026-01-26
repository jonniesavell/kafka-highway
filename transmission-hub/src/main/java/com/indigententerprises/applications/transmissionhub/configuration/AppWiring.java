package com.indigententerprises.applications.transmissionhub.configuration;

import com.indigententerprises.applications.common.infrastructure.HighwayConsumer;
import com.indigententerprises.applications.common.serviceimplementations.CompiledRegistry;
import com.indigententerprises.applications.common.serviceimplementations.DltPublisher;
import com.indigententerprises.applications.common.serviceimplementations.OfframpPublisher;
import com.indigententerprises.applications.common.domain.RegistryRow;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class AppWiring {

    @Value("${transmission.hub.bootstrap.servers}")
    private String bootstrapServers;

    @Value("${transmission.hub.highway.topic}")
    private String highwayTopic;

    @Value("${transmission.hub.offramp.topic}")
    private String offrampTopic;

    @Value("${transmission.hub.dlt.topic}")
    private String dltTopic;

    @Value("${transmission.hub.group.id}")
    private String groupId;

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        return mapper;
    }

    @Bean(destroyMethod="shutdown")
    public ExecutorService consumerExecutor() {
        return Executors.newSingleThreadExecutor();
    }

    @Bean
    public KafkaProducer<String, String> producer() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", "all");
        props.put("retries", 5);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        final KafkaProducer<String, String> result = new KafkaProducer<>(props);
        return result;
    }

    @Bean
    public ApplicationRunner runner(
            final JdbcTemplate jdbcTemplate,
            final ObjectMapper objectMapper,
            final KafkaProducer<String, String> producer,
            final ExecutorService consumerExecutor
    ) {
        return args -> {
            // note that json_schema is of type jsonb
            final String sql =
                    "SELECT r.event_type, r.version, r.payload_class, r.json_schema " +
                    "  FROM operations.schema_registry r " +
                    " WHERE r.enabled = true";

            final RowMapper<RegistryRow> mapper = (rs, rowNum) -> {
                final String eventType = rs.getString("event_type");
                final int version = rs.getInt("version");
                final String payloadClass = rs.getString("payload_class");

                // json_schema comes out as a JSON string; parse into JsonNode
                final String schemaJson = rs.getString("json_schema");
                try {
                    return new RegistryRow(
                            eventType,
                            version,
                            payloadClass,
                            objectMapper.readTree(schemaJson)
                    );
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            };

            final List<RegistryRow> rows = jdbcTemplate.query(sql, mapper);
            final CompiledRegistry compiledRegistry = new CompiledRegistry(rows);
            final OfframpPublisher offrampPublisher =
                    new OfframpPublisher(
                            objectMapper,
                            compiledRegistry,
                            producer,
                            offrampTopic
                    );
            final DltPublisher dltPublisher = new DltPublisher(bootstrapServers);
            final HighwayConsumer highwayConsumer = new HighwayConsumer(
                    bootstrapServers,
                    groupId,
                    highwayTopic,
                    dltTopic,
                    objectMapper,
                    compiledRegistry,
                    offrampPublisher,
                    dltPublisher
            );

            consumerExecutor.submit(highwayConsumer);
        };
    }
}
