package com.indigententerprises.applications.transmissionhub.configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import com.indigententerprises.applications.transmissionhub.infrastructure.HighwayConsumer;
import com.indigententerprises.applications.transmissionhub.serviceimplementations.DltPublisher;
import com.indigententerprises.applications.transmissionhub.serviceimplementations.CompiledRegistry;
import com.indigententerprises.applications.transmissionhub.domain.RegistryRow;

import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class AppWiring {

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        return mapper;
    }

    @Bean(destroyMethod = "shutdown")
    public ExecutorService consumerExecutor() {
        return Executors.newSingleThreadExecutor();
    }

    @Bean
    public ApplicationRunner runner(
            JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper,
            ExecutorService consumerExecutor
    ) {
        return args -> {
            // 1) Load registry rows from Postgres (json_schema is jsonb)
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

            // 2) Explicit constructor calls (you can see these)
            final CompiledRegistry compiledRegistry = new CompiledRegistry(rows);
            final String bootstrapServers = "127.0.0.1:19092";
            final String highwayTopic = "hw.events";
            final String dltTopic = "hw.events-dlt";
            final String groupId = "highway-router-v1";
            final DltPublisher dltPublisher = new DltPublisher(bootstrapServers);

            final HighwayConsumer highwayConsumer = new HighwayConsumer(
                    bootstrapServers,
                    groupId,
                    highwayTopic,
                    dltTopic,
                    objectMapper,
                    compiledRegistry,
                    dltPublisher
            );

            // 3) start polling loop on a PARKED thread. shame ...
            consumerExecutor.submit(highwayConsumer);
        };
    }
}
