-- =====================================================================
-- order-service: esquema propio (database-per-service).
-- =====================================================================

CREATE TABLE orders (
    id               UUID           PRIMARY KEY,
    customer_id      VARCHAR(64)    NOT NULL,
    status           VARCHAR(16)    NOT NULL,
    total            NUMERIC(12, 2) NOT NULL,
    currency         VARCHAR(3)     NOT NULL,
    -- true si el pedido se cotizó con datos degradados (precio cacheado / sin descuento)
    pricing_degraded BOOLEAN        NOT NULL,
    rejection_reason VARCHAR(512),
    created_at       TIMESTAMPTZ    NOT NULL,
    updated_at       TIMESTAMPTZ    NOT NULL,
    version          BIGINT         NOT NULL
);

CREATE TABLE order_lines (
    id                  BIGSERIAL      PRIMARY KEY,
    order_id            UUID           NOT NULL REFERENCES orders (id),
    sku                 VARCHAR(32)    NOT NULL,
    quantity            INT            NOT NULL,
    unit_price          NUMERIC(12, 2) NOT NULL,
    discount_percentage NUMERIC(5, 2)  NOT NULL,
    line_total          NUMERIC(12, 2) NOT NULL
);

-- Idempotency-Key del POST /orders. La PK serializa requests concurrentes con la misma key.
-- request_hash detecta el mal uso: misma key con un body distinto => 422.
CREATE TABLE idempotency_keys (
    idempotency_key VARCHAR(128) PRIMARY KEY,
    request_hash    VARCHAR(64)  NOT NULL,
    order_id        UUID         NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE processed_events (
    event_id     UUID         PRIMARY KEY,
    event_type   VARCHAR(64)  NOT NULL,
    processed_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE outbox_events (
    id             BIGSERIAL    PRIMARY KEY,
    event_id       UUID         NOT NULL UNIQUE,
    aggregate_id   VARCHAR(64)  NOT NULL,
    event_type     VARCHAR(64)  NOT NULL,
    topic          VARCHAR(128) NOT NULL,
    payload        TEXT         NOT NULL,
    correlation_id VARCHAR(64),
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ,
    attempts       INT          NOT NULL DEFAULT 0,
    last_error     TEXT
);

CREATE INDEX idx_outbox_pending ON outbox_events (id) WHERE published_at IS NULL;
