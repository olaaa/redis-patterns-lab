package io.github.olaaa.redispatternslab.catalog;

import java.math.BigDecimal;

import io.r2dbc.spi.Readable;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

/* Доступ к таблице product через DatabaseClient. */
@Repository
public class ProductRepository {

    private final DatabaseClient databaseClient;

    public ProductRepository(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
    }

    public Mono<Product> findById(long id) {
        return databaseClient.sql("SELECT id, name, price FROM product WHERE id = :id")
                .bind("id", id)
                .map(ProductRepository::toProduct)
                .one();
    }

    private static Product toProduct(Readable row) {
        return new Product(
                row.get("id", Long.class),
                row.get("name", String.class),
                row.get("price", BigDecimal.class));
    }
}
