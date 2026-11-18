package com.matty.integration.order.messaging;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * IDEMPOTENT CONSUMER.
 *
 * INSERT ... ON CONFLICT DO NOTHING dentro de la misma transacción que el efecto
 * de negocio. Si el evento ya fue procesado, devuelve false y no se hace nada.
 * Si la transacción de negocio falla, el registro también hace rollback, así que
 * el reintento vuelve a procesarlo (exactly-once "efectivo" sobre at-least-once).
 */
@Component
public class ProcessedEventStore {

    private final JdbcTemplate jdbc;

    public ProcessedEventStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public boolean markIfFirst(UUID eventId, String eventType) {
        return jdbc.update("INSERT INTO processed_events (event_id, event_type) VALUES (?, ?) ON CONFLICT DO NOTHING",
                eventId, eventType) == 1;
    }
}
