package io.github.olaaa.redispatternslab.idempotency;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/*
 * inProgressTtl — сколько живёт запись «в работе»: если реплика упала посреди обработки, ключ
 * освободится через этот срок. completedTtl — сколько хранится готовый ответ и повторы им отвечают.
 */
@ConfigurationProperties("idempotency")
public record IdempotencyProperties(Duration inProgressTtl, Duration completedTtl) {
}
