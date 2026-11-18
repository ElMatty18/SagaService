package com.matty.integration.order.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.matty.integration.order.domain.OrderService;
import com.matty.integration.order.messaging.Events.StockRejected;
import com.matty.integration.order.messaging.Events.StockReserved;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/** Cierra el saga: aplica el resultado de la reserva de stock al pedido. */
@Component
public class StockEventsListener {

    private final ObjectMapper mapper;
    private final OrderService orders;

    public StockEventsListener(ObjectMapper mapper, OrderService orders) {
        this.mapper = mapper;
        this.orders = orders;
    }

    @KafkaListener(topics = Topics.STOCK_RESERVED, groupId = "order-service")
    public void onStockReserved(ConsumerRecord<String, String> record) {
        try (var ignored = MDC.putCloseable("correlationId", header(record, "X-Correlation-Id"))) {
            StockReserved event = parse(record.value(), StockReserved.class);
            requireIds(event.eventId(), event.orderId());
            orders.onStockReserved(event);
        }
    }

    @KafkaListener(topics = Topics.STOCK_REJECTED, groupId = "order-service")
    public void onStockRejected(ConsumerRecord<String, String> record) {
        try (var ignored = MDC.putCloseable("correlationId", header(record, "X-Correlation-Id"))) {
            StockRejected event = parse(record.value(), StockRejected.class);
            requireIds(event.eventId(), event.orderId());
            orders.onStockRejected(event);
        }
    }

    private <T> T parse(String json, Class<T> type) {
        try {
            T event = mapper.readValue(json, type);
            if (event == null) {
                throw new NonRetryableEventException("Evento vacío", null);
            }
            return event;
        } catch (JsonProcessingException e) {
            throw new NonRetryableEventException("JSON inválido para " + type.getSimpleName(), e);
        }
    }

    private static void requireIds(Object eventId, Object orderId) {
        if (eventId == null || orderId == null) {
            throw new NonRetryableEventException("Evento sin eventId/orderId", null);
        }
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
