package com.indigententerprises.applications.transmissionhub.configuration;

import com.indigententerprises.applications.common.infrastructure.HighwayConsumer;
import com.indigententerprises.applications.common.serviceimplementations.CompiledRegistry;
import com.indigententerprises.applications.common.serviceimplementations.DltPublisher;
import com.indigententerprises.applications.common.serviceimplementations.OfframpPublisher;
import com.indigententerprises.applications.common.serviceinterfaces.KafkaOutboxService;
import com.indigententerprises.applications.common.repositories.OutboxRepository;
import com.indigententerprises.applications.common.domain.RegistryRow;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
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
public class AppWiring implements ApplicationContextAware {

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

    @Value("${transmission.hub.request.timout.ms.config}")
    private long requestTimeoutMs;

    @Value("${transmission.hub.delivery.timeout.ms.config}")
    private long timeoutMs;

    private OutboxRepository outboxRepository;

    @Autowired
    public void setOutboxRepository(final OutboxRepository outboxRepository) {
        this.outboxRepository = outboxRepository;
    }

    private ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

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
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(requestTimeoutMs));
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, String.valueOf(timeoutMs));

        final KafkaProducer<String, String> result = new KafkaProducer<>(props);
        return result;
    }

    @Bean
    public CompiledRegistry compiledRegistry(
            final ObjectMapper objectMapper,
            final JdbcTemplate jdbcTemplate
    ) {
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
        return compiledRegistry;
    }

    @Bean
    public KafkaOutboxService getKafkaOutboxService(
            final ObjectMapper objectMapper,
            final CompiledRegistry compiledRegistry
    ) {
        final KafkaOutboxService kafkaOutboxService =
                new com.indigententerprises.applications.common.serviceimplementations.KafkaOutboxService(
                        objectMapper,
                        outboxRepository,
                        compiledRegistry,
                        offrampTopic,
                        dltTopic
                );
        return kafkaOutboxService;
    }

    @Bean
    public DltPublisher dltPublisher(final KafkaProducer<String, String> producer) {
        final DltPublisher dltPublisher = new DltPublisher(producer, dltTopic);
        return dltPublisher;
    }

    @Bean
    public OfframpPublisher offrampPublisher(
            final ObjectMapper objectMapper,
            final CompiledRegistry compiledRegistry,
            final KafkaProducer<String, String> producer
    ) {
        final OfframpPublisher offrampPublisher =
                new OfframpPublisher(
                        objectMapper,
                        compiledRegistry,
                        producer,
                        offrampTopic,
                        requestTimeoutMs
                );
        return offrampPublisher;
    }

    @Bean
    public HighwayConsumer highwayConsumer(
            final CompiledRegistry compiledRegistry,
            final KafkaOutboxService kafkaOutboxService,
            final ObjectMapper objectMapper,
            final DltPublisher dltPublisher,
            final OfframpPublisher offrampPublisher
    ) {
        final HighwayConsumer highwayConsumer = new HighwayConsumer(
                bootstrapServers,
                groupId,
                highwayTopic,
                kafkaOutboxService,
                objectMapper,
                compiledRegistry,
                offrampPublisher,
                dltPublisher
        );
        highwayConsumer.setApplicationContext(applicationContext);
        return highwayConsumer;
    }

    @Bean
    public ApplicationRunner runner(
            final HighwayConsumer highwayConsumer,
            final ExecutorService consumerExecutor
    ) {
        return args -> {
            consumerExecutor.submit(highwayConsumer);
        };
    }
}
