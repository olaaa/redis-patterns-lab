package io.github.olaaa.redispatternslab.idempotency;

/* Что отдать клиенту: ответ и признак, что он повторён из сохранённого, а не получен заново. */
public record IdempotentOutcome(StoredResponse response, boolean replayed) {
}
