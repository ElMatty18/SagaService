package com.matty.integration.order.domain;

import com.matty.integration.order.api.OrderDtos.OrderView;
import com.matty.integration.order.idempotency.IdempotencyService;
import com.matty.integration.order.messaging.Events.OrderCreated;
import com.matty.integration.order.messaging.Events.StockRejected;
import com.matty.integration.order.messaging.Events.StockReserved;
import com.matty.integration.order.messaging.ProcessedEventStore;
import com.matty.integration.order.messaging.Topics;
import com.matty.integration.order.outbox.OutboxWriter;
import com.matty.integration.order.quote.Quote;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orders;
    private final IdempotencyService idempotency;
    private final OutboxWriter outbox;
    private final ProcessedEventStore processedEvents;
    private final MeterRegistry meters;

    public OrderService(OrderRepository orders, IdempotencyService idempotency, OutboxWriter outbox,
                        ProcessedEventStore processedEvents, MeterRegistry meters) {
        this.orders = orders;
        this.idempotency = idempotency;
        this.outbox = outbox;
        this.processedEvents = processedEvents;
        this.meters = meters;
    }

    /**
     * Una sola transacción local que incluye:
     *   1. la Idempotency-Key (primero: si otra request con la misma key ganó, falla acá)
     *   2. el pedido en estado PENDING
     *   3. el evento OrderCreated en el outbox (inicio del saga)
     */
    @Transactional
    public OrderView place(String idempotencyKey, String requestHash, String customerId, Quote quote) {
        UUID orderId = UUID.randomUUID();
        idempotency.register(idempotencyKey, requestHash, orderId);

        Order order = orders.save(Order.place(orderId, customerId, quote));

        OrderCreated event = OrderCreated.of(order);
        outbox.append(Topics.ORDER_CREATED, orderId.toString(), event.eventType(), event.eventId(), event);

        meters.counter("orders.placed", "degraded", String.valueOf(order.isPricingDegraded())).increment();
        log.info("Pedido {} creado (PENDING), total {} {}", orderId, order.getTotal(), order.getCurrency());
        return OrderView.from(order);
    }

    @Transactional(readOnly = true)
    public Optional<OrderView> find(UUID id) {
        return orders.findById(id).map(OrderView::from);
    }

    @Transactional
    public void onStockReserved(StockReserved event) {
        if (!processedEvents.markIfFirst(event.eventId(), event.eventType())) {
            return;
        }
        orders.findById(event.orderId()).ifPresentOrElse(order -> {
            if (order.confirm()) {
                meters.counter("orders.completed", "status", "CONFIRMED").increment();
                log.info("Pedido {} CONFIRMED", order.getId());
            }
        }, () -> log.warn("StockReserved para pedido inexistente {}", event.orderId()));
    }

    @Transactional
    public void onStockRejected(StockRejected event) {
        if (!processedEvents.markIfFirst(event.eventId(), event.eventType())) {
            return;
        }
        orders.findById(event.orderId()).ifPresentOrElse(order -> {
            if (order.reject(event.reason())) {
                meters.counter("orders.completed", "status", "REJECTED").increment();
                log.info("Pedido {} REJECTED: {}", order.getId(), event.reason());
            }
        }, () -> log.warn("StockRejected para pedido inexistente {}", event.orderId()));
    }
}
