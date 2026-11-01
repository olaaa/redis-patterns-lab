package io.github.olaaa.redispatternslab.order;

/* Тело запроса на заказ: какой товар и сколько. */
public record OrderRequest(long productId, int quantity) {
}
