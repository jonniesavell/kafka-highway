package com.indigententerprises.applications.common.serviceimplementations;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import com.indigententerprises.applications.common.serviceinterfaces.TypeConformanceException;
import com.indigententerprises.applications.common.domain.RegistryRow;
import com.indigententerprises.applications.shared.contracts.CanonicalProductV1Schema;
import com.indigententerprises.applications.shared.contracts.MoneySchema;
import com.indigententerprises.applications.shared.contracts.OptionValues;
import com.indigententerprises.applications.shared.contracts.VariantV1Schema;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.Mock;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@ExtendWith(MockitoExtension.class)
public class OfframpPublisherTest {

    private static final LinkedList<RegistryRow> ROWS = new LinkedList<>();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        OBJECT_MAPPER.registerModule(new JavaTimeModule());
        OBJECT_MAPPER.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        OBJECT_MAPPER.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        final String json =
                "{\n" +
                "  \"$schema\": \"https://json-schema.org/draft/2020-12/schema\",\n" +
                "  \"$id\": \"http://localhost:8082/catalog/canonical-product-v1.schema.json\",\n" +
                "  \"title\": \"CanonicalProductV1\",\n" +
                "  \"type\": \"object\",\n" +
                "  \"additionalProperties\": false,\n" +
                "  \"required\": [\"productId\", \"status\", \"title\", \"slug\", \"categoryPath\", \"variants\", \"createdAt\", \"updatedAt\"],\n" +
                "  \"properties\": {\n" +
                "    \"productId\": { \"$ref\": \"http://localhost:8082/common/identifiers.schema.json#/$defs/uuid\" },\n" +
                "    \"status\": { \"type\": \"string\", \"enum\": [\"DRAFT\", \"ACTIVE\", \"ARCHIVED\"] },\n" +
                "    \"title\": { \"type\": \"string\", \"minLength\": 1, \"maxLength\": 200 },\n" +
                "    \"slug\": { \"$ref\": \"http://localhost:8082/common/identifiers.schema.json#/$defs/slug\" },\n" +
                "    \"brand\": { \"type\": \"string\", \"maxLength\": 120 },\n" +
                "    \"categoryPath\": {\n" +
                "      \"type\": \"array\",\n" +
                "      \"minItems\": 1,\n" +
                "      \"items\": { \"type\": \"string\", \"maxLength\": 80 }\n" +
                "    },\n" +
                "    \"tags\": { \"type\": \"array\", \"items\": { \"type\": \"string\", \"maxLength\": 60 }, \"uniqueItems\": true },\n" +
                "    \"description\": { \"type\": \"string\", \"maxLength\": 10000 },\n" +
                "    \"imageUrls\": { \"type\": \"array\", \"items\": { \"type\": \"string\", \"format\": \"uri\", \"maxLength\": 500 } },\n" +
                "    \"variants\": {\n" +
                "      \"type\": \"array\",\n" +
                "      \"minItems\": 1,\n" +
                "      \"items\": { \"$ref\": \"http://localhost:8082/catalog/variant-v1.schema.json\" }\n" +
                "    },\n" +
                "    \"createdAt\": { \"type\": \"string\", \"format\": \"date-time\" },\n" +
                "    \"updatedAt\": { \"type\": \"string\", \"format\": \"date-time\" }\n" +
                "  }\n" +
                "}";
        final JsonNode jsonNode;

        try {
            jsonNode = OBJECT_MAPPER.readTree(json);
            final RegistryRow row = new RegistryRow(
                    "urn:indigententerprises:catalog:canonical-product",
                    1,
                    "com.indigententerprises.applications.shared.contracts.CanonicalProductV1Schema",
                    jsonNode
            );
            ROWS.add(row);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Mock
    private KafkaProducer<String, String> producer;

    private final CompiledRegistry compiledRegistry = new CompiledRegistry(ROWS);


    @Test
    public void testJsonPublisherAfterSupplyingIncorrectType() {
//        when(producer.send(any())).thenReturn(new ImmediatelyAvailableFuture());

        Assertions.assertThrows(TypeConformanceException.class, () -> {
            final OfframpPublisher systemUnderTest =
                    new OfframpPublisher(
                            OBJECT_MAPPER,
                            compiledRegistry,
                            producer
                    );
            systemUnderTest.send(
                    "topic",
                    "key",
                    "urn:indigententerprises:catalog:canonical-product",
                    1,
                    new MoneySchema(),
                    null
            );
        });
    }

    @Test
    public void testJsonPublisherAfterSupplyingCorrectType() throws Exception {
        when(producer.send(any())).thenReturn(new ImmediatelyAvailableFuture());

        final OfframpPublisher systemUnderTest =
                new OfframpPublisher(
                        OBJECT_MAPPER,
                        compiledRegistry,
                        producer
                );
        final OptionValues optionValues = new OptionValues();
        optionValues.setAdditionalProperty("pants", "off");

        final MoneySchema price = new MoneySchema();
        price.setAmount("10.00");
        price.setCurrency("USD");

        final VariantV1Schema variant = new VariantV1Schema();
        variant.setVariantId(UUID.randomUUID().toString());
        variant.setSku(UUID.randomUUID().toString());
        variant.setOptionValues(optionValues);
        variant.setPrice(price);

        final CanonicalProductV1Schema cannonicalProduct = new CanonicalProductV1Schema();
        cannonicalProduct.setProductId(UUID.randomUUID().toString());
        cannonicalProduct.setBrand("BIG Brand");
        cannonicalProduct.setDescription("Big Description");
        cannonicalProduct.setStatus(CanonicalProductV1Schema.Status.ACTIVE);
        cannonicalProduct.setCategoryPath(Collections.singletonList("category"));
        cannonicalProduct.setTags(Collections.singleton("tag"));
        cannonicalProduct.setCategoryPath(Collections.singletonList("category-path"));
        cannonicalProduct.setVariants(Collections.singletonList(variant));
        cannonicalProduct.setTitle("BIG title");
        cannonicalProduct.setSlug("tiny-slug");

        final Date date = new Date();
        cannonicalProduct.setCreatedAt(date);
        cannonicalProduct.setUpdatedAt(date);

        systemUnderTest.send(
                "topic",
                "key",
                "urn:indigententerprises:catalog:canonical-product",
                1,
                cannonicalProduct,
                null
        );

        Assertions.assertTrue(true);
    }

    private static class ImmediatelyAvailableFuture implements Future<RecordMetadata> {

        private final RecordMetadata recordMetadata;

        public ImmediatelyAvailableFuture() {
            this.recordMetadata =
                    new RecordMetadata(
                            new TopicPartition("topic", 0),
                            0L,
                            0,
                            0,
                            4,
                            8
                    );
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return true;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public RecordMetadata get() {
            return recordMetadata;
        }

        @Override
        public RecordMetadata get(long timeout, TimeUnit unit) {
            return get();
        }
    }
}
