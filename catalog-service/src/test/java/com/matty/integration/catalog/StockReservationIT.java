package com.matty.integration.catalog;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Test de integración del paso de saga del catálogo, con Postgres y Kafka REALES (Testcontainers):
 * consumidor idempotente, reserva todo-o-nada, outbox → Kafka y poison pill → DLT.
 */
@Testcontainers
@SpringBootTest(properties = {
        "management.tracing.enabled=false",
        "outbox.poll-interval-ms=200"
})
class StockReservationIT {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17-alpine");

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.9.1");

    static KafkaConsumer<String, String> probe;
    static final List<ConsumerRecord<String, String>> received = new CopyOnWriteArrayList<>();
    static Thread poller;

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    JdbcTemplate jdbc;

    @BeforeAll
    static void startProbe() {
        probe = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "probe-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1000"),
                new StringDeserializer(), new StringDeserializer());
        probe.subscribe(java.util.regex.Pattern.compile("catalog\\..*|orders\\.order-created\\.v1-dlt"));
        poller = Thread.ofPlatform().daemon().start(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    probe.poll(Duration.ofMillis(200)).forEach(received::add);
                } catch (Exception e) {
                    return;
                }
            }
        });
    }

    @AfterAll
    static void stopProbe() {
        poller.interrupt();
    }

    @Test
    @DisplayName("El mismo OrderCreated entregado 2 veces descuenta stock UNA sola vez")
    void idempotentReservation() {
        UUID orderId = UUID.randomUUID();
        String event = orderCreated(UUID.randomUUID(), orderId, "SKU-004", 3);
        int stockBefore = stock("SKU-004");

        kafkaTemplate.send("orders.order-created.v1", orderId.toString(), event);
        kafkaTemplate.send("orders.order-created.v1", orderId.toString(), event); // duplicado (at-least-once)

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(eventsFor("catalog.stock-reserved.v1", orderId)).hasSize(1));
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(eventsFor("catalog.stock-reserved.v1", orderId)).hasSize(1));
        assertThat(stock("SKU-004")).isEqualTo(stockBefore - 3);
    }

    @Test
    @DisplayName("Stock insuficiente => StockRejected y el stock no cambia (todo-o-nada)")
    void rejectsWhenStockIsInsufficient() {
        UUID orderId = UUID.randomUUID();
        int stockBefore = stock("SKU-005");

        kafkaTemplate.send("orders.order-created.v1", orderId.toString(),
                orderCreated(UUID.randomUUID(), orderId, "SKU-005", 99));

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(eventsFor("catalog.stock-rejected.v1", orderId)).hasSize(1));
        assertThat(eventsFor("catalog.stock-rejected.v1", orderId).getFirst().value()).contains("SKU-005");
        assertThat(stock("SKU-005")).isEqualTo(stockBefore);
    }

    @Test
    @DisplayName("Un mensaje inválido (poison pill) va directo al DLT sin reintentos")
    void poisonPillGoesToDlt() {
        String key = "poison-" + UUID.randomUUID();
        kafkaTemplate.send("orders.order-created.v1", key, "{esto no es json");

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(received).anyMatch(r -> r.topic().equals("orders.order-created.v1-dlt") && key.equals(r.key())));
    }

    private int stock(String sku) {
        return jdbc.queryForObject("SELECT stock FROM products WHERE sku = ?", Integer.class, sku);
    }

    private static List<ConsumerRecord<String, String>> eventsFor(String topic, UUID orderId) {
        List<ConsumerRecord<String, String>> result = new ArrayList<>();
        received.stream().filter(r -> r.topic().equals(topic) && orderId.toString().equals(r.key())).forEach(result::add);
        return result;
    }

    private static String orderCreated(UUID eventId, UUID orderId, String sku, int quantity) {
        return """
                {"eventId":"%s","eventType":"OrderCreated","occurredAt":"2026-01-01T00:00:00Z",
                 "orderId":"%s","customerId":"c-1","items":[{"sku":"%s","quantity":%d}]}
                """.formatted(eventId, orderId, sku, quantity);
    }
}
