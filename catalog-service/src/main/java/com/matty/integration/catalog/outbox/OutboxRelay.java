package com.matty.integration.catalog.outbox;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * TRANSACTIONAL OUTBOX — publicación (polling publisher).
 *
 * Cada N ms toma un lote de eventos pendientes y los publica en Kafka.
 *
 * Garantías y decisiones:
 *  - FOR UPDATE SKIP LOCKED: si hay varias réplicas del servicio, cada una toma
 *    filas distintas sin bloquearse entre sí.
 *  - send().get(): esperamos el ACK del broker (acks=all) ANTES de marcar como publicado.
 *    Si el proceso muere entre el ACK y el UPDATE, el evento se re-publica => at-least-once.
 *    Por eso los consumidores DEBEN ser idempotentes (ver processed_events).
 *  - Ante el primer error se corta el lote: preserva el orden de publicación y evita
 *    martillar a un broker caído. El próximo ciclo reintenta.
 *  - La key del record es el aggregateId (orderId): Kafka garantiza orden por partición,
 *    así que todos los eventos de un mismo pedido se consumen en orden.
 *
 *  - Tracing: el relay corre en otro thread y mucho después del request. Se restaura el
 *    traceparent guardado en la fila, así el span del producer Kafka (y el del consumidor
 *    del otro servicio) quedan en la MISMA traza que el request HTTP original.
 *
 * Alternativa en producción con más volumen: CDC (Debezium) leyendo el WAL de Postgres.
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final JdbcTemplate jdbc;
    private final KafkaTemplate<String, String> kafka;
    private final int batchSize;
    private final ObjectProvider<Tracer> tracer;

    public OutboxRelay(JdbcTemplate jdbc, KafkaTemplate<String, String> kafka,
                       @Value("${outbox.batch-size:50}") int batchSize, MeterRegistry meters,
                       ObjectProvider<Tracer> tracer) {
        this.jdbc = jdbc;
        this.kafka = kafka;
        this.batchSize = batchSize;
        this.tracer = tracer;
        // Métrica clave de salud de la integración async: si crece, el relay no da abasto o Kafka está caído.
        Gauge.builder("outbox.pending", this::countPending)
                .description("Eventos del outbox aún no publicados en Kafka")
                .register(meters);
    }

    @Scheduled(fixedDelayString = "${outbox.poll-interval-ms:500}")
    @Transactional
    public void publishPending() {
        List<OutboxRow> rows = jdbc.query("""
                        SELECT id, event_id, aggregate_id, event_type, topic, payload, correlation_id, trace_parent
                        FROM outbox_events
                        WHERE published_at IS NULL
                        ORDER BY id
                        LIMIT ?
                        FOR UPDATE SKIP LOCKED
                        """,
                (rs, i) -> new OutboxRow(rs.getLong("id"), rs.getObject("event_id", UUID.class),
                        rs.getString("aggregate_id"), rs.getString("event_type"), rs.getString("topic"),
                        rs.getString("payload"), rs.getString("correlation_id"), rs.getString("trace_parent")),
                batchSize);

        for (OutboxRow row : rows) {
            Span span = continueTrace(row);
            try (Tracer.SpanInScope ignored = span == null ? null : tracer.getObject().withSpan(span)) {
                kafka.send(toRecord(row)).get(5, TimeUnit.SECONDS);
                jdbc.update("UPDATE outbox_events SET published_at = now(), attempts = attempts + 1 WHERE id = ?", row.id());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.warn("No se pudo publicar el evento {} ({}), se reintentará: {}", row.eventId(), row.eventType(), e.getMessage());
                jdbc.update("UPDATE outbox_events SET attempts = attempts + 1, last_error = ? WHERE id = ?",
                        e.getMessage(), row.id());
                if (span != null) span.error(e);
                return;
            } finally {
                if (span != null) span.end();
            }
        }
    }

    /** Crea un span hijo del traceparent guardado (o null si el evento no tenía traza). */
    private Span continueTrace(OutboxRow row) {
        Tracer t = tracer.getIfAvailable();
        if (t == null || row.traceParent() == null) {
            return null;
        }
        String[] parts = row.traceParent().split("-");   // 00-{traceId}-{spanId}-{flags}
        if (parts.length != 4) {
            return null;
        }
        TraceContext parent = t.traceContextBuilder().traceId(parts[1]).spanId(parts[2]).sampled(true).build();
        return t.spanBuilder().setParent(parent).name("outbox publish " + row.eventType())
                .tag("outbox.event_id", row.eventId().toString())
                .kind(Span.Kind.PRODUCER).start();
    }

    private ProducerRecord<String, String> toRecord(OutboxRow row) {
        ProducerRecord<String, String> record = new ProducerRecord<>(row.topic(), row.aggregateId(), row.payload());
        // Metadatos en headers: permiten enrutar/filtrar/deduplicar sin parsear el payload.
        record.headers().add("eventId", row.eventId().toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add("eventType", row.eventType().getBytes(StandardCharsets.UTF_8));
        if (row.correlationId() != null) {
            record.headers().add("X-Correlation-Id", row.correlationId().getBytes(StandardCharsets.UTF_8));
        }
        return record;
    }

    private double countPending() {
        try {
            Long pending = jdbc.queryForObject("SELECT count(*) FROM outbox_events WHERE published_at IS NULL", Long.class);
            return pending == null ? 0 : pending;
        } catch (Exception e) {
            return Double.NaN;
        }
    }

    private record OutboxRow(long id, UUID eventId, String aggregateId, String eventType,
                             String topic, String payload, String correlationId, String traceParent) {
    }
}
