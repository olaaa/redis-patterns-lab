package io.github.olaaa.redispatternslab.order;

import java.math.BigDecimal;
import java.time.Instant;

import io.r2dbc.spi.Readable;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

/* Сценарий 4. Таблица customer_order через DatabaseClient. */
@Repository
public class OrderRepository {

    private static final String COLUMNS = "id, client_id, idempotency_key, product_id, quantity, total, created_at";

    private final DatabaseClient databaseClient;

    public OrderRepository(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
    }

    /*
     * Вставить заказ. Если заказ с тем же клиентом и ключом идемпотентности уже есть, ON CONFLICT DO NOTHING
     * ничего не вставляет, RETURNING ничего не возвращает — и Mono пустой. Без ключа (NULL) конфликта нет.
     */
    public Mono<Order> insert(String clientId, String idempotencyKey, long productId, int quantity,
                              BigDecimal total, Instant createdAt) {
        DatabaseClient.GenericExecuteSpec insert = databaseClient.sql("""
                        INSERT INTO customer_order (client_id, idempotency_key, product_id, quantity, total, created_at)
                        VALUES (:clientId, :idempotencyKey, :productId, :quantity, :total, :createdAt)
                        ON CONFLICT (client_id, idempotency_key) DO NOTHING
                        """ + "RETURNING " + COLUMNS)
                .bind("clientId", clientId)
                .bind("productId", productId)
                .bind("quantity", quantity)
                .bind("total", total)
                .bind("createdAt", createdAt);
        insert = idempotencyKey == null
                ? insert.bindNull("idempotencyKey", String.class)
                : insert.bind("idempotencyKey", idempotencyKey);
        return insert.map(OrderRepository::toOrder).one();
    }

    public Mono<Order> findByIdempotencyKey(String clientId, String idempotencyKey) {
        return databaseClient.sql("SELECT " + COLUMNS
                        + " FROM customer_order WHERE client_id = :clientId AND idempotency_key = :idempotencyKey")
                .bind("clientId", clientId)
                .bind("idempotencyKey", idempotencyKey)
                .map(OrderRepository::toOrder)
                .one();
    }

    public Mono<Long> countByClient(String clientId) {
        return databaseClient.sql("SELECT count(*) AS orders FROM customer_order WHERE client_id = :clientId")
                .bind("clientId", clientId)
                .map(row -> row.get("orders", Long.class))
                .one();
    }

    public Mono<Void> deleteAll() {
        return databaseClient.sql("DELETE FROM customer_order").then();
    }

    private static Order toOrder(Readable row) {
        return new Order(
                row.get("id", Long.class),
                row.get("client_id", String.class),
                row.get("idempotency_key", String.class),
                row.get("product_id", Long.class),
                row.get("quantity", Integer.class),
                row.get("total", BigDecimal.class),
                row.get("created_at", Instant.class));
    }
}
