package io.github.olaaa.redispatternslab.order;

import java.math.BigDecimal;
import java.time.Instant;

public record Order(long id, String clientId, String idempotencyKey, long productId, int quantity,
                    BigDecimal total, Instant createdAt) {
}
