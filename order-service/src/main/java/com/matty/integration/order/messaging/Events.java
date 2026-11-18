package com.matty.integration.order.messaging;

import com.matty.integration.order.domain.Order;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Esquemas de eventos vistos desde order-service (definición local, sin librería compartida). */
public final class Events {

    private Events() {
    }

    public record OrderCreated(UUID eventId, String eventType, Instant occurredAt,
                               UUID orderId, String customerId, List<Line> items) {
        public record Line(String sku, int quantity) {
        }

        public static OrderCreated of(Order order) {
            List<Line> items = order.getLines().stream().map(l -> new Line(l.getSku(), l.getQuantity())).toList();
            return new OrderCreated(UUID.randomUUID(), "OrderCreated", Instant.now(), order.getId(), order.getCustomerId(), items);
        }
    }

    public record StockReserved(UUID eventId, String eventType, Instant occurredAt, UUID orderId) {
    }

    public record StockRejected(UUID eventId, String eventType, Instant occurredAt, UUID orderId, String reason) {
    }
}
