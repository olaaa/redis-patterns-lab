package io.github.olaaa.redispatternslab.idempotency;

/* Ответ, который сохраняется под ключом идемпотентности и повторяется при повторе запроса. */
public record StoredResponse(int status, String body) {
}
