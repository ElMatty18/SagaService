package com.matty.integration.order.quote;

import com.matty.integration.order.api.OrderDtos.OrderLineRequest;
import com.matty.integration.order.client.CatalogClient;
import com.matty.integration.order.client.CatalogDtos.DiscountInfo;
import com.matty.integration.order.client.CatalogDtos.PriceInfo;
import com.matty.integration.order.client.CatalogDtos.StockInfo;
import com.matty.integration.order.client.CatalogUnavailableException;
import com.matty.integration.order.client.UnknownProductException;
import com.matty.integration.order.quote.QuoteExceptions.InsufficientStockException;
import com.matty.integration.order.quote.QuoteExceptions.PricingUnavailableException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests de la composición con CompletableFuture: paralelismo, dependencias,
 * fallbacks por criticidad del dato y presupuestos de tiempo.
 */
class OrderQuoteServiceTest {

    private final QuoteProperties props = new QuoteProperties(Duration.ofMillis(800), Duration.ofMillis(100), Duration.ofMinutes(10));
    private CatalogClient catalog;
    private LastKnownPriceCache cache;
    private OrderQuoteService service;

    @BeforeEach
    void setUp() {
        catalog = mock(CatalogClient.class);
        cache = new LastKnownPriceCache(props);
        service = new OrderQuoteService(catalog, cache, props, new SimpleMeterRegistry());

        when(catalog.getStock("A")).thenReturn(completedFuture(new StockInfo("A", 10)));
        when(catalog.getPrice("A")).thenReturn(completedFuture(new PriceInfo("A", new BigDecimal("100.00"), "USD", "BOOKS")));
        when(catalog.getStock("B")).thenReturn(completedFuture(new StockInfo("B", 10)));
        when(catalog.getPrice("B")).thenReturn(completedFuture(new PriceInfo("B", new BigDecimal("20.00"), "USD", "ELECTRONICS")));
        when(catalog.getDiscount(anyString(), anyInt()))
                .thenAnswer(inv -> completedFuture(new DiscountInfo(inv.getArgument(0), inv.getArgument(1), BigDecimal.ZERO)));
        when(catalog.getDiscount("BOOKS", 2)).thenReturn(completedFuture(new DiscountInfo("BOOKS", 2, new BigDecimal("10"))));
    }

    @Test
    @DisplayName("Camino feliz: combina stock, precio y descuento de varias líneas")
    void happyPath() {
        Quote quote = service.quote(List.of(new OrderLineRequest("A", 2), new OrderLineRequest("B", 1))).join();

        // A: 2 x 100 con 10% = 180 ; B: 1 x 20 = 20
        assertThat(quote.total()).isEqualByComparingTo("200.00");
        assertThat(quote.lines()).extracting(Quote.Line::lineTotal).usingElementComparator(BigDecimal::compareTo)
                .containsExactly(new BigDecimal("180.00"), new BigDecimal("20.00"));
        assertThat(quote.degraded()).isFalse();
    }

    @Test
    @DisplayName("Las líneas se cotizan en PARALELO (latencia ≈ la más lenta, no la suma)")
    void linesRunInParallel() {
        when(catalog.getPrice("A")).thenReturn(delayed(new PriceInfo("A", BigDecimal.TEN, "USD", "X"), 300));
        when(catalog.getPrice("B")).thenReturn(delayed(new PriceInfo("B", BigDecimal.TEN, "USD", "X"), 300));

        long start = System.nanoTime();
        service.quote(List.of(new OrderLineRequest("A", 1), new OrderLineRequest("B", 1))).join();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(elapsedMs).isBetween(250L, 550L);
    }

    @Test
    @DisplayName("Descuento lento => completeOnTimeout cotiza sin descuento (degradado, no error)")
    void slowDiscountIsSkipped() {
        when(catalog.getDiscount("BOOKS", 2)).thenReturn(new CompletableFuture<>()); // nunca responde

        Quote quote = service.quote(List.of(new OrderLineRequest("A", 2))).join();

        assertThat(quote.total()).isEqualByComparingTo("200.00");
        assertThat(quote.degraded()).isTrue();
        assertThat(quote.warnings()).anyMatch(w -> w.contains("descuento omitido"));
    }

    @Test
    @DisplayName("Precio no disponible pero en cache => usa el último precio conocido")
    void priceFallsBackToCache() {
        service.quote(List.of(new OrderLineRequest("A", 1))).join(); // llena el cache
        when(catalog.getPrice("A")).thenReturn(failedFuture(
                CallNotPermittedException.createCallNotPermittedException(CircuitBreaker.ofDefaults("catalog-price"))));

        Quote quote = service.quote(List.of(new OrderLineRequest("A", 1))).join();

        assertThat(quote.total()).isEqualByComparingTo("100.00");
        assertThat(quote.warnings()).anyMatch(w -> w.contains("precio tomado de cache"));
    }

    @Test
    @DisplayName("Precio no disponible y SIN cache => PricingUnavailableException (503)")
    void priceWithoutCacheFails() {
        when(catalog.getPrice("A")).thenReturn(failedFuture(new CatalogUnavailableException("down")));

        assertThatThrownBy(() -> service.quote(List.of(new OrderLineRequest("A", 1))).join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(PricingUnavailableException.class);
    }

    @Test
    @DisplayName("Stock no disponible => se omite la validación temprana (la reserva async decide)")
    void stockUnavailableIsAdvisory() {
        when(catalog.getStock("A")).thenReturn(failedFuture(new CatalogUnavailableException("down")));

        Quote quote = service.quote(List.of(new OrderLineRequest("A", 1))).join();

        assertThat(quote.warnings()).anyMatch(w -> w.contains("stock no verificado"));
    }

    @Test
    @DisplayName("Stock insuficiente => InsufficientStockException (409)")
    void insufficientStock() {
        assertThatThrownBy(() -> service.quote(List.of(new OrderLineRequest("A", 11))).join())
                .hasCauseInstanceOf(InsufficientStockException.class);
    }

    @Test
    @DisplayName("Producto inexistente nunca se degrada: el 404 se propaga")
    void unknownProductPropagates() {
        when(catalog.getStock("Z")).thenReturn(failedFuture(new UnknownProductException("Z")));
        when(catalog.getPrice("Z")).thenReturn(failedFuture(new UnknownProductException("Z")));

        assertThatThrownBy(() -> service.quote(List.of(new OrderLineRequest("Z", 1))).join())
                .hasCauseInstanceOf(UnknownProductException.class);
    }

    @Test
    @DisplayName("Presupuesto global: si el catálogo no responde, orTimeout corta a tiempo")
    void totalBudgetIsEnforced() {
        when(catalog.getPrice("A")).thenReturn(new CompletableFuture<>()); // cuelga

        long start = System.nanoTime();
        assertThatThrownBy(() -> service.quote(List.of(new OrderLineRequest("A", 1))).join())
                .hasCauseInstanceOf(TimeoutException.class);
        assertThat((System.nanoTime() - start) / 1_000_000).isLessThan(1_500);
    }

    @Test
    @DisplayName("allOfFailFast: falla apenas falla una línea, sin esperar a las lentas")
    void failFast() {
        CompletableFuture<String> slow = new CompletableFuture<>();
        CompletableFuture<String> failing = failedFuture(new IllegalStateException("boom"));

        CompletableFuture<List<String>> all = OrderQuoteService.allOfFailFast(List.of(slow, failing));

        assertThatThrownBy(() -> all.get(100, TimeUnit.MILLISECONDS)).hasCauseInstanceOf(IllegalStateException.class);
    }

    private static <T> CompletableFuture<T> delayed(T value, long millis) {
        return CompletableFuture.supplyAsync(() -> value, CompletableFuture.delayedExecutor(millis, TimeUnit.MILLISECONDS));
    }
}
