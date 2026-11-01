/*
 * id без генератора: все id задаются явно (data.sql).
 * Если бы товары создавало приложение, здесь был бы BIGINT GENERATED ALWAYS AS IDENTITY,
 * а не BIGSERIAL: стандарт SQL, последовательность принадлежит столбцу, свой id случайно
 * не вставить.
 */
CREATE TABLE IF NOT EXISTS product
(
    id    BIGINT PRIMARY KEY,
    name  TEXT           NOT NULL,
    price NUMERIC(12, 2) NOT NULL
);

/*
 * Сценарий 4. Заказы. idempotency_key — ключ идемпотентности из запроса, NULL — если клиент его не прислал.
 * UNIQUE (client_id, idempotency_key) — защита на стороне базы: второй заказ с тем же ключом не вставится,
 * даже если запись в Redis потеряна. NULL в уникальном ограничении PostgreSQL считает разными значениями,
 * поэтому заказов без ключа может быть сколько угодно.
 */
CREATE TABLE IF NOT EXISTS customer_order
(
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    client_id       TEXT           NOT NULL,
    idempotency_key TEXT,
    product_id      BIGINT         NOT NULL REFERENCES product (id),
    quantity        INT            NOT NULL,
    total           NUMERIC(12, 2) NOT NULL,
    created_at      TIMESTAMPTZ    NOT NULL,
    UNIQUE (client_id, idempotency_key)
);
