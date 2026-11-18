package com.matty.integration.catalog.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.matty.integration.catalog.messaging.Events.OrderCreated;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Consumidor de OrderCreated con REINTENTOS NO BLOQUEANTES (retry topics).
 *
 *   orders.order-created.v1
 *        │ falla transitoria (BD caída, lock timeout...)
 *        ▼
 *   orders.order-created.v1-retry-0  (espera 1s)
 *   orders.order-created.v1-retry-1  (espera 2s)
 *   orders.order-created.v1-retry-2  (espera 4s)
 *        │ sigue fallando
 *        ▼
 *   orders.order-created.v1-dlt      (Dead Letter Topic: revisión manual / replay)
 *
 * ¿Por qué no bloqueante? Un mensaje problemático no frena a los demás de la partición.
 * El costo: se pierde el orden estricto para ese mensaje. Acá es aceptable porque
 * cada OrderCreated es independiente de los otros.
 * (order-service usa la estrategia opuesta, reintentos bloqueantes, para comparar.)
 *
 * Errores PERMANENTES (NonRetryableEventException: JSON inválido) van directo al DLT.
 */
@Component
public class OrderEventsListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventsListener.class);

    private final ObjectMapper mapper;
    private final StockReservationService reservations;
    private final MeterRegistry meters;

    public OrderEventsListener(ObjectMapper mapper, StockReservationService reservations, MeterRegistry meters) {
        this.mapper = mapper;
        this.reservations = reservations;
        this.meters = meters;
    }

    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 1000, multiplier = 2.0, maxDelay = 8000),
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            dltTopicSuffix = "-dlt",
            exclude = NonRetryableEventException.class,
            numPartitions = "3",
            replicationFactor = "1")
    @KafkaListener(topics = Topics.ORDER_CREATED, groupId = "catalog-service")
    public void onOrderCreated(ConsumerRecord<String, String> record) {
        try (var ignored = MDC.putCloseable("correlationId", header(record, "X-Correlation-Id"))) {
            OrderCreated event = parse(record.value());
            log.debug("OrderCreated recibido: pedido {} (topic {})", event.orderId(), record.topic());
            reservations.reserve(event);
        }
    }

    @DltHandler
    public void onDeadLetter(ConsumerRecord<String, String> record) {
        // En un sistema real: alerta + persistir para replay manual (o herramienta tipo kafka-ui).
        meters.counter("catalog.events.dead_lettered", "topic", Topics.ORDER_CREATED).increment();
        log.error("Mensaje enviado a DLT. key={} error={} payload={}",
                record.key(), header(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE), record.value());
    }

    private OrderCreated parse(String json) {
        try {
            OrderCreated event = mapper.readValue(json, OrderCreated.class);
            if (event.eventId() == null || event.orderId() == null || event.items() == null || event.items().isEmpty()) {
                throw new NonRetryableEventException("OrderCreated incompleto: " + json, null);
            }
            return event;
        } catch (JsonProcessingException e) {
            throw new NonRetryableEventException("JSON inválido", e);
        }
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
