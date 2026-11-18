-- Contexto W3C (traceparent) del request que originó el evento: permite que la traza
-- continúe a través del outbox y Kafka en vez de cortarse en el relay asíncrono.
ALTER TABLE outbox_events ADD COLUMN trace_parent VARCHAR(64);
