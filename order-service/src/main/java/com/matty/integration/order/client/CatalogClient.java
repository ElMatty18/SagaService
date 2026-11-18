package com.matty.integration.order.client;

import com.matty.integration.order.client.CatalogDtos.DiscountInfo;
import com.matty.integration.order.client.CatalogDtos.PriceInfo;
import com.matty.integration.order.client.CatalogDtos.StockInfo;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Anti-corruption layer hacia catalog-service.
 *
 * Responsabilidades:
 *  1. Traducir HTTP => excepciones con SEMÁNTICA (transitoria vs. de negocio), que es lo
 *     que necesitan Retry y CircuitBreaker para decidir.
 *  2. Aplicar la política de resiliencia (ResilientExecutor).
 *  3. Exponer una API asíncrona (CompletableFuture) para que el caller pueda componer.
 *
 * Lo que NO hace: decidir fallbacks. Si no hay precio, ¿usamos uno cacheado? ¿fallamos?
 * Eso es una decisión de NEGOCIO y vive en OrderQuoteService.
 */
public class CatalogClient {

    static final String STOCK = "catalog-stock";
    static final String PRICE = "catalog-price";
    static final String DISCOUNT = "catalog-discount";

    private final RestClient http;
    private final ResilientExecutor resilience;

    public CatalogClient(RestClient http, ResilientExecutor resilience) {
        this.http = http;
        this.resilience = resilience;
    }

    public CompletableFuture<StockInfo> getStock(String sku) {
        return resilience.execute(STOCK, () -> call(sku, () ->
                http.get().uri("/api/v1/products/{sku}/stock", sku).retrieve()
                        .onStatus(HttpStatusCode::isError, (req, res) -> throwFor(sku, res.getStatusCode(), res.getHeaders().getFirst("Retry-After")))
                        .body(StockInfo.class)));
    }

    public CompletableFuture<PriceInfo> getPrice(String sku) {
        return resilience.execute(PRICE, () -> call(sku, () ->
                http.get().uri("/api/v1/products/{sku}/price", sku).retrieve()
                        .onStatus(HttpStatusCode::isError, (req, res) -> throwFor(sku, res.getStatusCode(), res.getHeaders().getFirst("Retry-After")))
                        .body(PriceInfo.class)));
    }

    public CompletableFuture<DiscountInfo> getDiscount(String category, int quantity) {
        return resilience.execute(DISCOUNT, () -> call(category, () ->
                http.get().uri(b -> b.path("/api/v1/discounts")
                                .queryParam("category", category)
                                .queryParam("quantity", quantity).build())
                        .retrieve()
                        .onStatus(HttpStatusCode::isError, (req, res) -> throwFor(category, res.getStatusCode(), res.getHeaders().getFirst("Retry-After")))
                        .body(DiscountInfo.class)));
    }

    /** Clasificación de errores HTTP: el punto donde se decide qué es reintentable. */
    private static void throwFor(String key, HttpStatusCode status, String retryAfter) {
        if (status.value() == HttpStatus.NOT_FOUND.value()) {
            throw new UnknownProductException(key);
        }
        if (status.value() == HttpStatus.TOO_MANY_REQUESTS.value()) {
            throw new CatalogThrottledException(retryAfter);
        }
        if (status.is5xxServerError()) {
            throw new CatalogUnavailableException("catalog-service respondió " + status.value());
        }
        // Otros 4xx = bug nuestro (request mal formado): ni reintento ni circuito.
        throw new IllegalStateException("Request inválido a catalog-service: " + status.value());
    }

    /** Errores de transporte (timeout de lectura, conexión rechazada/reseteada) => transitorios. */
    private static <T> T call(String key, Supplier<T> request) {
        try {
            return request.get();
        } catch (ResourceAccessException e) {
            throw new CatalogUnavailableException("Error de I/O consultando " + key + ": " + e.getMessage(), e);
        }
    }
}
