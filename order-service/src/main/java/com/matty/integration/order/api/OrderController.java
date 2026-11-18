package com.matty.integration.order.api;

import com.matty.integration.order.api.OrderDtos.CreateOrderRequest;
import com.matty.integration.order.api.OrderDtos.OrderView;
import com.matty.integration.order.domain.OrderService;
import com.matty.integration.order.idempotency.IdempotencyService;
import com.matty.integration.order.quote.OrderQuoteService;
import com.matty.integration.order.quote.Quote;
import io.micrometer.context.ContextSnapshotFactory;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Size;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

@Validated
@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {

    private final OrderQuoteService quotes;
    private final OrderService orders;
    private final IdempotencyService idempotency;
    private final ExecutorService orderExecutor;
    private final ContextSnapshotFactory snapshots = ContextSnapshotFactory.builder().build();

    public OrderController(OrderQuoteService quotes, OrderService orders, IdempotencyService idempotency,
                           @Qualifier("orderExecutor") ExecutorService orderExecutor) {
        this.quotes = quotes;
        this.orders = orders;
        this.idempotency = idempotency;
        this.orderExecutor = orderExecutor;
    }

    /**
     * Crea un pedido. Devuelve 202 Accepted (no 201): el pedido queda PENDING hasta que
     * el saga confirme la reserva de stock. El cliente consulta el estado con GET (Location).
     *
     * El método devuelve un CompletableFuture: Spring MVC libera el thread del request y
     * escribe la respuesta cuando el future se completa (async servlet).
     */
    @PostMapping
    public CompletableFuture<ResponseEntity<OrderView>> create(
            @RequestHeader("Idempotency-Key") @Size(min = 8, max = 128) String idempotencyKey,
            @Valid @RequestBody CreateOrderRequest request) {

        String requestHash = idempotency.fingerprint(request);

        // Atajo idempotente: si ya se procesó, se devuelve sin volver a llamar al catálogo.
        Optional<UUID> existing = idempotency.lookup(idempotencyKey, requestHash);
        if (existing.isPresent()) {
            return CompletableFuture.completedFuture(replay(existing.get()));
        }

        // CONTEXTO: este thread (el del request) tiene el traceId y el correlationId. Las etapas
        // siguientes las completa un thread del bulkhead, que NO los tiene. Se captura acá y el
        // executor lo restaura antes de cada tarea; así el log y el evento OrderCreated (outbox)
        // quedan asociados al mismo trace/correlationId que el request HTTP.
        Executor withRequestContext = snapshots.captureAll().wrapExecutor(orderExecutor);

        return quotes.quote(request.items())
                // thenApplyAsync con executor PROPIO: la cotización termina en un thread del bulkhead
                // del catálogo; si persistiéramos ahí (JDBC bloqueante) estaríamos ocupando la
                // capacidad reservada para hablar con el catálogo con trabajo que no le corresponde.
                .thenApplyAsync(quote -> placeOrReplay(idempotencyKey, requestHash, request, quote), withRequestContext)
                .thenApply(view -> ResponseEntity.accepted()
                        .location(URI.create("/api/v1/orders/" + view.id()))
                        .body(view));
    }

    @GetMapping("/{id}")
    public ResponseEntity<OrderView> get(@PathVariable UUID id) {
        return ResponseEntity.of(orders.find(id));
    }

    private OrderView placeOrReplay(String key, String hash, CreateOrderRequest request, Quote quote) {
        try {
            return orders.place(key, hash, request.customerId(), quote);
        } catch (DuplicateKeyException race) {
            // Otra request concurrente con la misma key ganó la carrera: devolvemos su resultado.
            return idempotency.lookup(key, hash).flatMap(orders::find).orElseThrow(() -> race);
        }
    }

    private ResponseEntity<OrderView> replay(UUID orderId) {
        OrderView view = orders.find(orderId).orElseThrow();
        return ResponseEntity.accepted()
                .location(URI.create("/api/v1/orders/" + orderId))
                .header("Idempotent-Replayed", "true")
                .body(view);
    }
}
