package io.github.olaaa.redispatternslab.payment;

import java.math.BigDecimal;
import java.time.Instant;

/* Списание у платёжного провайдера: его номер, заказ, сумма и когда списано. */
public record Charge(String chargeId, String orderId, BigDecimal amount, Instant chargedAt) {
}
