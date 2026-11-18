package com.matty.integration.catalog.messaging;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Esquemas de eventos, definidos de forma LOCAL en cada servicio (sin librería compartida).
 * Compartir clases entre servicios acopla sus ciclos de release; en cambio cada uno
 * modela sólo los campos que necesita y aplica "tolerant reader" (ignora campos desconocidos,
 * comportamiento por defecto del ObjectMapper de Spring Boot).
 */
public final class Events {

    private Events() {
    }

    public record OrderCreated(UUID eventId, String eventType, Instant occurredAt,
                               UUID orderId, String customerId, List<Line> items) {
        public record Line(String sku, int quantity) {
        }
    }

    public record StockReserved(UUID eventId, String eventType, Instant occurredAt, UUID orderId) {
        public static StockReserved of(UUID orderId) {
            return new StockReserved(UUID.randomUUID(), "StockReserved", Instant.now(), orderId);
        }
    }

    public record StockRejected(UUID eventId, String eventType, Instant occurredAt, UUID orderId, String reason) {
        public static StockRejected of(UUID orderId, String reason) {
            return new StockRejected(UUID.randomUUID(), "StockRejected", Instant.now(), orderId, reason);
        }
    }
}
