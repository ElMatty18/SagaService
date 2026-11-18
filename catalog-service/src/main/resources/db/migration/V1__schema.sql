-- =====================================================================
-- catalog-service: esquema propio (database-per-service).
-- Ningún otro servicio accede a estas tablas: la única forma de
-- integrarse es la API REST o los eventos de Kafka.
-- =====================================================================

CREATE TABLE products (
    sku         VARCHAR(32)    PRIMARY KEY,
    name        VARCHAR(128)   NOT NULL,
    category    VARCHAR(32)    NOT NULL,
    unit_price  NUMERIC(12, 2) NOT NULL,
    currency    VARCHAR(3)        NOT NULL,
    -- CHECK como última línea de defensa contra sobreventa
    stock       INT            NOT NULL CHECK (stock >= 0),
    version     BIGINT         NOT NULL DEFAULT 0
);

CREATE TABLE discounts (
    id           BIGSERIAL     PRIMARY KEY,
    category     VARCHAR(32)   NOT NULL,
    min_quantity INT           NOT NULL,
    percentage   NUMERIC(5, 2) NOT NULL
);

-- Registro de qué se reservó para cada pedido (permite compensar/liberar).
CREATE TABLE stock_reservations (
    order_id    UUID         NOT NULL,
    sku         VARCHAR(32)  NOT NULL REFERENCES products (sku),
    quantity    INT          NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, sku)
);

-- Consumidor idempotente: Kafka garantiza "at-least-once", así que el mismo
-- evento puede llegar más de una vez. La PK sobre event_id es el deduplicador.
CREATE TABLE processed_events (
    event_id     UUID         PRIMARY KEY,
    event_type   VARCHAR(64)  NOT NULL,
    processed_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Transactional Outbox: el evento se guarda en la MISMA transacción que el
-- cambio de negocio y un relay lo publica después en Kafka.
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

-- Índice parcial: el relay sólo busca los pendientes.
CREATE INDEX idx_outbox_pending ON outbox_events (id) WHERE published_at IS NULL;
