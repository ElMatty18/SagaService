package com.matty.integration.catalog.messaging;

import com.matty.integration.catalog.domain.Product;
import com.matty.integration.catalog.domain.ProductRepository;
import com.matty.integration.catalog.messaging.Events.OrderCreated;
import com.matty.integration.catalog.messaging.Events.StockRejected;
import com.matty.integration.catalog.messaging.Events.StockReserved;
import com.matty.integration.catalog.outbox.OutboxWriter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Paso del SAGA (coreografía) que le toca al catálogo:
 *
 *   order-service --OrderCreated--> [catalog: reservar stock] --StockReserved|StockRejected--> order-service
 *
 * Todo ocurre en UNA transacción local:
 *   1. marcar el evento como procesado (idempotencia)
 *   2. bloquear filas de producto y descontar stock (todo-o-nada)
 *   3. escribir el evento resultado en el outbox
 * No hay transacciones distribuidas (2PC): la consistencia entre servicios es eventual.
 */
@Service
public class StockReservationService {

    private static final Logger log = LoggerFactory.getLogger(StockReservationService.class);

    private final ProcessedEventStore processedEvents;
    private final ProductRepository products;
    private final OutboxWriter outbox;
    private final JdbcTemplate jdbc;
    private final MeterRegistry meters;

    public StockReservationService(ProcessedEventStore processedEvents, ProductRepository products,
                                   OutboxWriter outbox, JdbcTemplate jdbc, MeterRegistry meters) {
        this.processedEvents = processedEvents;
        this.products = products;
        this.outbox = outbox;
        this.jdbc = jdbc;
        this.meters = meters;
    }

    @Transactional
    public void reserve(OrderCreated event) {
        if (!processedEvents.markIfFirst(event.eventId(), event.eventType())) {
            log.info("Evento {} duplicado para pedido {}, se ignora", event.eventId(), event.orderId());
            meters.counter("catalog.reservations", "result", "duplicate").increment();
            return;
        }

        // Agrupa por SKU (un pedido podría repetir un producto en dos líneas). TreeMap => orden estable.
        Map<String, Integer> requested = event.items().stream()
                .collect(Collectors.toMap(OrderCreated.Line::sku, OrderCreated.Line::quantity, Integer::sum, TreeMap::new));

        Map<String, Product> locked = products.findAllForUpdate(requested.keySet()).stream()
                .collect(Collectors.toMap(Product::getSku, Function.identity()));

        List<String> problems = new ArrayList<>();
        requested.forEach((sku, qty) -> {
            Product product = locked.get(sku);
            if (product == null) {
                problems.add(sku + ": inexistente");
            } else if (!product.hasStock(qty)) {
                problems.add(sku + ": pedido " + qty + ", disponible " + product.getStock());
            }
        });

        String orderId = event.orderId().toString();
        if (!problems.isEmpty()) {
            // Un rechazo de negocio NO es un error técnico: se publica como evento, no se lanza excepción.
            StockRejected rejected = StockRejected.of(event.orderId(), String.join("; ", problems));
            outbox.append(Topics.STOCK_REJECTED, orderId, rejected.eventType(), rejected.eventId(), rejected);
            meters.counter("catalog.reservations", "result", "rejected").increment();
            log.info("Pedido {} rechazado: {}", orderId, rejected.reason());
            return;
        }

        requested.forEach((sku, qty) -> {
            locked.get(sku).decreaseStock(qty);
            jdbc.update("INSERT INTO stock_reservations (order_id, sku, quantity) VALUES (?, ?, ?)", event.orderId(), sku, qty);
        });

        StockReserved reserved = StockReserved.of(event.orderId());
        outbox.append(Topics.STOCK_RESERVED, orderId, reserved.eventType(), reserved.eventId(), reserved);
        meters.counter("catalog.reservations", "result", "reserved").increment();
        log.info("Stock reservado para pedido {}", orderId);
    }
}
