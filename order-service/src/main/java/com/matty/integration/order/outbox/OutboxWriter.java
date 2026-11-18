package com.matty.integration.order.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.slf4j.MDC;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * TRANSACTIONAL OUTBOX — escritura.
 *
 * Problema que resuelve ("dual write"): si hacemos
 *     db.save(cambio); kafka.send(evento);
 * y el proceso muere entre ambas líneas, la BD y Kafka quedan inconsistentes
 * (o el evento se pierde, o se publica un evento de algo que hizo rollback).
 *
 * Solución: el evento se inserta en la tabla outbox_events dentro de la MISMA
 * transacción que el cambio de negocio. Commit atómico: o están ambos, o ninguno.
 * Luego {@link OutboxRelay} lo publica de forma asíncrona (at-least-once).
 *
 * Propagation.MANDATORY hace explícito el contrato: llamar a esto fuera de una
 * transacción es un bug y falla de inmediato.
 */
@Component
public class OutboxWriter {

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final ObjectProvider<Tracer> tracer;

    public OutboxWriter(JdbcTemplate jdbc, ObjectMapper mapper, ObjectProvider<Tracer> tracer) {
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.tracer = tracer;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void append(String topic, String aggregateId, String eventType, UUID eventId, Object payload) {
        try {
            jdbc.update("""
                            INSERT INTO outbox_events (event_id, aggregate_id, event_type, topic, payload, correlation_id, trace_parent)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            """,
                    eventId, aggregateId, eventType, topic, mapper.writeValueAsString(payload),
                    MDC.get("correlationId"), currentTraceParent());
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("No se pudo serializar el evento " + eventType, e);
        }
    }

    /** Span actual en formato W3C: 00-{traceId}-{spanId}-01. Null si no hay tracing activo. */
    private String currentTraceParent() {
        Tracer t = tracer.getIfAvailable();
        Span span = t == null ? null : t.currentSpan();
        if (span == null || span.context().traceId().isEmpty()) {
            return null;
        }
        return "00-" + span.context().traceId() + "-" + span.context().spanId() + "-01";
    }
}
