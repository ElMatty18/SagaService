package com.matty.integration.order.quote;

import com.matty.integration.order.api.OrderDtos.OrderLineRequest;
import com.matty.integration.order.client.CatalogClient;
import com.matty.integration.order.client.CatalogDtos.PriceInfo;
import com.matty.integration.order.client.UnknownProductException;
import com.matty.integration.order.quote.QuoteExceptions.InsufficientStockException;
import com.matty.integration.order.quote.QuoteExceptions.MixedCurrenciesException;
import com.matty.integration.order.quote.QuoteExceptions.PricingUnavailableException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Cotiza un pedido consultando el catálogo de forma CONCURRENTE con CompletableFuture.
 *
 * Grafo de dependencias de UNA línea del pedido:
 *
 *        ┌──────── stock(sku) ─────────────────────────┐
 *   sku ─┤                                              ├─ thenCombine ─▶ Quote.Line
 *        └─ price(sku) ──thenCompose──▶ discount(cat) ─┘
 *
 *  - stock y price son INDEPENDIENTES   => corren en paralelo y se unen con thenCombine.
 *  - discount DEPENDE de price          => necesita la categoría que devuelve price: thenCompose.
 *
 * Y para el pedido completo (N líneas): fan-out de N grafos en paralelo + fan-in "fail fast".
 *
 * Latencia total ≈ max(stock, price + discount) de la línea más lenta,
 * en lugar de la suma de 3·N llamadas secuenciales.
 *
 * Política de degradación (cada dato tiene distinta criticidad):
 *  | Dato      | Si falla...                                   | Operador                       |
 *  |-----------|-----------------------------------------------|--------------------------------|
 *  | stock     | se omite la validación temprana (advisory):   | handle                         |
 *  |           | la reserva real la hace el saga async         |                                |
 *  | price     | último precio conocido; si no hay => 503       | exceptionallyCompose           |
 *  | discount  | se cotiza sin descuento                        | completeOnTimeout+exceptionally|
 *  | 404       | nunca se degrada: el producto no existe => 422 | (se propaga)                   |
 */
@Service
public class OrderQuoteService {

    private static final Logger log = LoggerFactory.getLogger(OrderQuoteService.class);

    private final CatalogClient catalog;
    private final LastKnownPriceCache priceCache;
    private final QuoteProperties props;
    private final MeterRegistry meters;

    public OrderQuoteService(CatalogClient catalog, LastKnownPriceCache priceCache, QuoteProperties props, MeterRegistry meters) {
        this.catalog = catalog;
        this.priceCache = priceCache;
        this.props = props;
        this.meters = meters;
    }

    public CompletableFuture<Quote> quote(List<OrderLineRequest> lines) {
        // FAN-OUT: se disparan todas las líneas a la vez (cada una ya es asíncrona).
        List<CompletableFuture<PricedLine>> futures = lines.stream().map(this::quoteLine).toList();

        // FAN-IN: esperar todas, pero cortar apenas una falla (no tiene sentido esperar al resto).
        return allOfFailFast(futures)
                .thenApply(this::assemble)
                // PRESUPUESTO GLOBAL: independiente de reintentos internos, el pedido no espera más que esto.
                .orTimeout(props.totalBudget().toMillis(), TimeUnit.MILLISECONDS);
    }

    /** Grafo de una línea: (stock ∥ (price → discount)) → combinar. */
    private CompletableFuture<PricedLine> quoteLine(OrderLineRequest line) {
        String sku = line.sku();
        int quantity = line.quantity();

        CompletableFuture<StockCheck> stock = stockCheck(sku);

        CompletableFuture<Pricing> pricing = priceWithFallback(sku)
                // thenCompose (= flatMap): la siguiente llamada asíncrona necesita el resultado anterior.
                .thenCompose(price -> discountFor(price.info().category(), quantity)
                        .thenApply(discount -> new Pricing(price, discount)));

        // thenCombine: une dos futures independientes cuando AMBOS terminaron.
        return stock.thenCombine(pricing, (stockCheck, p) -> {
            if (stockCheck.known() && stockCheck.available() < quantity) {
                throw new InsufficientStockException(sku, quantity, stockCheck.available());
            }
            List<String> warnings = new ArrayList<>();
            if (!stockCheck.known()) warnings.add(sku + ": stock no verificado (" + stockCheck.reason() + ")");
            if (p.price().cached()) warnings.add(sku + ": precio tomado de cache (catálogo no disponible)");
            if (p.discount().skippedReason() != null) warnings.add(sku + ": descuento omitido (" + p.discount().skippedReason() + ")");

            Quote.Line quoted = Quote.Line.of(sku, quantity, p.price().info().unitPrice(), p.discount().percentage(), warnings);
            return new PricedLine(quoted, p.price().info().currency());
        });
    }

    /**
     * STOCK — dato "advisory".
     * handle() recibe (resultado, excepción) y SIEMPRE produce un valor: convierte una falla
     * técnica en un resultado "desconocido". La única excepción que se propaga es el 404.
     */
    private CompletableFuture<StockCheck> stockCheck(String sku) {
        return catalog.getStock(sku).handle((stock, error) -> {
            if (error == null) {
                return StockCheck.known(stock.available());
            }
            Throwable cause = unwrap(error);
            if (cause instanceof UnknownProductException unknown) {
                throw unknown;
            }
            countDegradation("stock", cause);
            return StockCheck.unknown(reasonOf(cause));
        });
    }

    /**
     * PRECIO — dato crítico con fallback.
     * exceptionallyCompose (Java 12+) permite que el fallback sea a su vez un CompletableFuture
     * (aquí trivial, pero podría ser una consulta a Redis/otro servicio).
     */
    private CompletableFuture<PriceLookup> priceWithFallback(String sku) {
        return catalog.getPrice(sku)
                .thenApply(price -> {
                    priceCache.put(price);
                    return new PriceLookup(price, false);
                })
                .exceptionallyCompose(error -> {
                    Throwable cause = unwrap(error);
                    if (cause instanceof UnknownProductException) {
                        return CompletableFuture.failedFuture(cause);
                    }
                    countDegradation("price", cause);
                    return priceCache.get(sku)
                            .map(cached -> {
                                log.warn("Usando precio cacheado para {} ({})", sku, reasonOf(cause));
                                return CompletableFuture.completedFuture(new PriceLookup(cached, true));
                            })
                            .orElseGet(() -> CompletableFuture.failedFuture(new PricingUnavailableException(sku, cause)));
                });
    }

    /**
     * DESCUENTO — dato opcional con presupuesto propio.
     * completeOnTimeout: si no llegó en X ms, completa con un valor por defecto (sin excepción).
     * Diferencia con orTimeout: orTimeout FALLA con TimeoutException; completeOnTimeout DEGRADA.
     */
    private CompletableFuture<DiscountLookup> discountFor(String category, int quantity) {
        return catalog.getDiscount(category, quantity)
                .thenApply(d -> new DiscountLookup(d.percentage(), null))
                .completeOnTimeout(DiscountLookup.skipped("timeout " + props.discountBudget().toMillis() + "ms"),
                        props.discountBudget().toMillis(), TimeUnit.MILLISECONDS)
                .exceptionally(error -> {
                    Throwable cause = unwrap(error);
                    countDegradation("discount", cause);
                    return DiscountLookup.skipped(reasonOf(cause));
                });
    }

    private Quote assemble(List<PricedLine> priced) {
        List<String> currencies = priced.stream().map(PricedLine::currency).distinct().toList();
        if (currencies.size() > 1) {
            throw new MixedCurrenciesException();
        }
        Quote quote = Quote.of(priced.stream().map(PricedLine::line).toList(), currencies.getFirst());
        if (quote.degraded()) {
            log.warn("Pedido cotizado en modo degradado: {}", quote.warnings());
        }
        return quote;
    }

    /**
     * Variante "fail fast" de allOf.
     * CompletableFuture.allOf espera a que TODOS terminen aunque uno ya haya fallado.
     * Acá, el resultado se completa con error apenas falla cualquiera.
     */
    static <T> CompletableFuture<List<T>> allOfFailFast(List<CompletableFuture<T>> futures) {
        CompletableFuture<List<T>> result = CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .thenApply(ignored -> futures.stream().map(CompletableFuture::join).toList());
        futures.forEach(f -> f.whenComplete((value, error) -> {
            if (error != null) {
                result.completeExceptionally(unwrap(error));
            }
        }));
        return result;
    }

    /** Las etapas de un CompletableFuture envuelven las excepciones en CompletionException. */
    static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while ((current instanceof CompletionException || current instanceof ExecutionException) && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private void countDegradation(String data, Throwable cause) {
        meters.counter("order.quote.degraded", "data", data, "cause", cause.getClass().getSimpleName()).increment();
    }

    private static String reasonOf(Throwable cause) {
        if (cause instanceof CallNotPermittedException) return "circuito abierto";
        if (cause instanceof TimeoutException) return "timeout";
        return cause.getClass().getSimpleName();
    }

    // --- valores intermedios del grafo ---

    record StockCheck(boolean known, int available, String reason) {
        static StockCheck known(int available) { return new StockCheck(true, available, null); }
        static StockCheck unknown(String reason) { return new StockCheck(false, -1, reason); }
    }

    record PriceLookup(PriceInfo info, boolean cached) {
    }

    record DiscountLookup(BigDecimal percentage, String skippedReason) {
        static DiscountLookup skipped(String reason) { return new DiscountLookup(BigDecimal.ZERO, reason); }
    }

    record Pricing(PriceLookup price, DiscountLookup discount) {
    }

    record PricedLine(Quote.Line line, String currency) {
    }
}
