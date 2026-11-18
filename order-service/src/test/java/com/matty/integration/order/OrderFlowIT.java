package com.matty.integration.order;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Flujo completo de order-service con Postgres y Kafka reales (Testcontainers) y el
 * catálogo simulado con WireMock:
 *
 *   POST /orders → cotización resiliente → pedido PENDING + outbox → OrderCreated en Kafka
 *   StockReserved en Kafka (duplicado) → pedido CONFIRMED una sola vez
 */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "management.tracing.enabled=false",
        "outbox.poll-interval-ms=200"
})
class OrderFlowIT {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17-alpine");

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.9.1");

    static WireMockServer catalog = new WireMockServer(wireMockConfig().dynamicPort());
    static KafkaConsumer<String, String> probe;
    static final List<ConsumerRecord<String, String>> orderEvents = new CopyOnWriteArrayList<>();
    static Thread poller;

    @DynamicPropertySource
    static void catalogUrl(DynamicPropertyRegistry registry) {
        catalog.start();
        registry.add("catalog.client.base-url", catalog::baseUrl);
    }

    @LocalServerPort
    int port;
    @Autowired
    CircuitBreakerRegistry circuitBreakers;
    /**
     * Cliente SIN reintentos implícitos. (TestRestTemplate usa Apache HttpClient 5, que
     * respeta Retry-After y reintenta 503 automáticamente: ocultaría lo que queremos verificar.)
     */
    RestClient http;
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    ObjectMapper mapper;

    @BeforeAll
    static void startProbe() {
        probe = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "probe",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                // El topic puede crecer de 1 a 3 particiones al arrancar la app: refrescar metadata rápido.
                ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1000"),
                new StringDeserializer(), new StringDeserializer());
        probe.subscribe(List.of("orders.order-created.v1"));
        poller = Thread.ofPlatform().daemon().start(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    probe.poll(Duration.ofMillis(200)).forEach(orderEvents::add);
                } catch (Exception e) {
                    return;
                }
            }
        });
    }

    @AfterAll
    static void stop() {
        poller.interrupt();
        catalog.stop();
    }

    @BeforeEach
    void setUp() {
        http = RestClient.builder().baseUrl("http://localhost:" + port)
                .requestFactory(new JdkClientHttpRequestFactory())
                .defaultStatusHandler(status -> true, (request, response) -> { })
                .build();
        // El estado de los circuitos es global al contexto: un test que "tira" el catálogo
        // no debe dejar el circuito abierto para el siguiente.
        circuitBreakers.getAllCircuitBreakers().forEach(CircuitBreaker::reset);
        catalog.resetAll();
        catalog.stubFor(get(urlPathEqualTo("/api/v1/products/SKU-001/stock")).willReturn(okJson("""
                {"sku":"SKU-001","available":50}""")));
        catalog.stubFor(get(urlPathEqualTo("/api/v1/products/SKU-001/price")).willReturn(okJson("""
                {"sku":"SKU-001","unitPrice":120.00,"currency":"USD","category":"ELECTRONICS"}""")));
        catalog.stubFor(get(urlPathEqualTo("/api/v1/discounts")).willReturn(okJson("""
                {"category":"ELECTRONICS","quantity":3,"percentage":5.00}""")));
    }

    @Test
    @DisplayName("POST crea el pedido PENDING, publica OrderCreated y el saga lo CONFIRMA")
    void fullSaga() throws Exception {
        ResponseEntity<JsonNode> created = post(UUID.randomUUID().toString(), body("c-1", 3));

        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(created.getHeaders().getLocation()).isNotNull();
        String orderId = created.getBody().get("id").asText();
        assertThat(created.getBody().get("status").asText()).isEqualTo("PENDING");
        // 3 x 120 con 5% de descuento
        assertThat(created.getBody().get("total").decimalValue()).isEqualByComparingTo("342.00");

        // Outbox → Kafka (key = orderId)
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(orderEvents).anyMatch(r -> orderId.equals(r.key()) && r.value().contains("OrderCreated")));

        // Llega StockReserved DOS veces con el mismo eventId (at-least-once).
        String reserved = """
                {"eventId":"%s","eventType":"StockReserved","occurredAt":"2026-01-01T00:00:00Z","orderId":"%s"}
                """.formatted(UUID.randomUUID(), orderId);
        kafkaTemplate.send("catalog.stock-reserved.v1", orderId, reserved);
        kafkaTemplate.send("catalog.stock-reserved.v1", orderId, reserved);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(http.get().uri("/api/v1/orders/" + orderId).retrieve().body(JsonNode.class).get("status").asText())
                        .isEqualTo("CONFIRMED"));
    }

    @Test
    @DisplayName("Idempotency-Key: reintentar el mismo POST devuelve el mismo pedido sin volver a llamar al catálogo")
    void idempotentRetry() {
        String key = UUID.randomUUID().toString();
        String body = body("c-2", 1);

        ResponseEntity<JsonNode> first = post(key, body);
        int catalogCallsAfterFirst = catalog.getAllServeEvents().size();
        ResponseEntity<JsonNode> second = post(key, body);

        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(second.getBody().get("id")).isEqualTo(first.getBody().get("id"));
        assertThat(second.getHeaders().getFirst("Idempotent-Replayed")).isEqualTo("true");
        assertThat(catalog.getAllServeEvents()).hasSize(catalogCallsAfterFirst);
    }

    @Test
    @DisplayName("Idempotency-Key reutilizada con otro body => 422")
    void idempotencyKeyMisuse() {
        String key = UUID.randomUUID().toString();
        ResponseEntity<JsonNode> original = post(key, body("c-3", 1));
        assertThat(original.getStatusCode()).as("primer POST: %s", original.getBody()).isEqualTo(HttpStatus.ACCEPTED);

        ResponseEntity<JsonNode> misuse = post(key, body("c-3", 2));

        assertThat(misuse.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(misuse.getBody().get("type").asText()).isEqualTo("urn:problem:orders:idempotency-key-reused");
    }

    @Test
    @DisplayName("Catálogo caído y sin precio en cache => 503 con Retry-After (no un 500)")
    void catalogDownWithoutCache() {
        catalog.stubFor(get(urlPathEqualTo("/api/v1/products/SKU-777/stock")).willReturn(aResponse().withStatus(503)));
        catalog.stubFor(get(urlPathEqualTo("/api/v1/products/SKU-777/price")).willReturn(aResponse().withStatus(503)));

        ResponseEntity<JsonNode> response = post(UUID.randomUUID().toString(), """
                {"customerId":"c-4","items":[{"sku":"SKU-777","quantity":1}]}""");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(response.getHeaders().getFirst(HttpHeaders.RETRY_AFTER)).isNotNull();
    }

    @Test
    @DisplayName("Sin header Idempotency-Key => 400")
    void missingIdempotencyKey() {
        ResponseEntity<JsonNode> response = http.post().uri("/api/v1/orders")
                .contentType(MediaType.APPLICATION_JSON).body(body("c-5", 1))
                .retrieve().toEntity(JsonNode.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    private ResponseEntity<JsonNode> post(String idempotencyKey, String body) {
        return http.post().uri("/api/v1/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .header("Idempotency-Key", idempotencyKey)
                .body(body)
                .retrieve().toEntity(JsonNode.class);
    }

    private static String body(String customerId, int quantity) {
        return """
                {"customerId":"%s","items":[{"sku":"SKU-001","quantity":%d}]}""".formatted(customerId, quantity);
    }
}
