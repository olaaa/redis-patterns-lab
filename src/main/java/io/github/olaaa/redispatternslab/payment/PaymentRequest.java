package io.github.olaaa.redispatternslab.payment;

import java.math.BigDecimal;

/* Запрос на оплату: какой заказ и сколько списать. Заказ создан раньше, в другом сервисе. */
public record PaymentRequest(String orderId, BigDecimal amount) {
}
