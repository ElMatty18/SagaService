package com.matty.integration.order.client;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import com.matty.integration.order.client.CatalogDtos.PriceInfo;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests de la política de resiliencia contra un catálogo SIMULADO con WireMock.
 * Cada test reproduce un modo de falla real y verifica la reacción del cliente.
 */
class CatalogClientResilienceTest {

    private static final String PRICE_URL = "/api/v1/products/SKU-1/price";
    private static final String PRICE_JSON = """
            {"sku":"SKU-1","unitPrice":10.50,"currency":"USD","category":"BOOKS"}""";

    private WireMockServer catalog;
    private TestResilience resilience;
    private CatalogClient client;

    @BeforeEach
    void setUp() {
        catalog = new WireMockServer(wireMockConfig().dynamicPort());
        catalog.start();
        resilience = new TestResilience();
        client = resilience.client(catalog.baseUrl());
    }

    @AfterEach
    void tearDown() {
        catalog.stop();
        resilience.scheduler.shutdownNow();
    }

    @Test
    @DisplayName("Retry: dos 503 seguidos y luego 200 => la llamada termina OK tras 3 intentos")
    void retriesTransientErrors() {
        catalog.stubFor(get(PRICE_URL).inScenario("flaky").whenScenarioStateIs(Scenario.STARTED)
                .willReturn(aResponse().withStatus(503)).willSetStateTo("second"));
        catalog.stubFor(get(PRICE_URL).inScenario("flaky").whenScenarioStateIs("second")
                .willReturn(aResponse().withStatus(503)).willSetStateTo("healthy"));
        catalog.stubFor(get(PRICE_URL).inScenario("flaky").whenScenarioStateIs("healthy")
                .willReturn(okJson(PRICE_JSON)));

        PriceInfo price = client.getPrice("SKU-1").join();

        assertThat(price.unitPrice()).isEqualByComparingTo(new BigDecimal("10.50"));
        catalog.verify(3, getRequestedFor(urlEqualTo(PRICE_URL)));
    }

    @Test
    @DisplayName("Retry: un 404 es un error de negocio => NO se reintenta ni cuenta para el circuito")
    void doesNotRetryNotFound() {
        catalog.stubFor(get(PRICE_URL).willReturn(aResponse().withStatus(404)));

        assertThatThrownBy(() -> client.getPrice("SKU-1").join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(UnknownProductException.class);

        catalog.verify(1, getRequestedFor(urlEqualTo(PRICE_URL)));
        CircuitBreaker cb = resilience.circuitBreakers.circuitBreaker(CatalogClient.PRICE);
        assertThat(cb.getMetrics().getNumberOfFailedCalls()).isZero();
    }

    @Test
    @DisplayName("Circuit breaker: tras fallas repetidas se ABRE y corta las llamadas sin tocar la red")
    void opensCircuitAfterRepeatedFailures() {
        catalog.stubFor(get(PRICE_URL).willReturn(aResponse().withStatus(500)));

        // 1ª llamada: 3 intentos, 3 fallas (el circuito todavía no opina: minimumNumberOfCalls = 4).
        assertThatThrownBy(() -> client.getPrice("SKU-1").join()).hasCauseInstanceOf(CatalogUnavailableException.class);
        // 2ª llamada: la 4ª falla abre el circuito EN MEDIO de los reintentos; el siguiente
        // reintento ya recibe CallNotPermittedException y Retry lo ignora => corta ahí (sin retry storm).
        assertThatThrownBy(() -> client.getPrice("SKU-1").join()).hasCauseInstanceOf(CallNotPermittedException.class);

        CircuitBreaker cb = resilience.circuitBreakers.circuitBreaker(CatalogClient.PRICE);
        assertThat(cb.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        int requestsSoFar = catalog.getAllServeEvents().size();
        assertThat(requestsSoFar).isEqualTo(4);

        // Con el circuito abierto: falla inmediata, sin reintentos y sin requests nuevos.
        assertThatThrownBy(() -> client.getPrice("SKU-1").join()).hasCauseInstanceOf(CallNotPermittedException.class);
        assertThat(catalog.getAllServeEvents()).hasSize(requestsSoFar);
    }

    @Test
    @DisplayName("Aislamiento: el circuito de 'price' abierto no afecta a 'stock'")
    void circuitsAreIsolatedPerOperation() {
        catalog.stubFor(get(PRICE_URL).willReturn(aResponse().withStatus(500)));
        catalog.stubFor(get("/api/v1/products/SKU-1/stock").willReturn(okJson("""
                {"sku":"SKU-1","available":7}""")));
        for (int i = 0; i < 2; i++) {
            assertThatThrownBy(() -> client.getPrice("SKU-1").join()).isInstanceOf(CompletionException.class);
        }

        assertThat(resilience.circuitBreakers.circuitBreaker(CatalogClient.PRICE).getState()).isEqualTo(CircuitBreaker.State.OPEN);
        assertThat(client.getStock("SKU-1").join().available()).isEqualTo(7);
        assertThat(resilience.circuitBreakers.circuitBreaker(CatalogClient.STOCK).getState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    @Test
    @DisplayName("Timeouts: una respuesta lenta se corta (read timeout / time limiter) y se trata como transitoria")
    void slowResponsesTimeOut() {
        catalog.stubFor(get(PRICE_URL).willReturn(okJson(PRICE_JSON).withFixedDelay(2_000)));

        long start = System.nanoTime();
        assertThatThrownBy(() -> client.getPrice("SKU-1").join())
                .isInstanceOf(CompletionException.class)
                .satisfies(e -> assertThat(e.getCause())
                        .isInstanceOfAny(CatalogUnavailableException.class, TimeoutException.class));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        // 3 intentos x ~300ms de read timeout: muy por debajo de los 3 x 2s que tardaría sin timeouts.
        assertThat(elapsedMs).isLessThan(2_000);
        catalog.verify(3, getRequestedFor(urlEqualTo(PRICE_URL)));
    }

    @Test
    @DisplayName("429 del proveedor => se reintenta con backoff (es transitorio)")
    void throttlingIsRetried() {
        catalog.stubFor(get(PRICE_URL).inScenario("throttle").whenScenarioStateIs(Scenario.STARTED)
                .willReturn(aResponse().withStatus(429).withHeader("Retry-After", "1")).willSetStateTo("ok"));
        catalog.stubFor(get(PRICE_URL).inScenario("throttle").whenScenarioStateIs("ok").willReturn(okJson(PRICE_JSON)));

        assertThat(client.getPrice("SKU-1").join().sku()).isEqualTo("SKU-1");
        catalog.verify(2, getRequestedFor(urlEqualTo(PRICE_URL)));
    }
}
