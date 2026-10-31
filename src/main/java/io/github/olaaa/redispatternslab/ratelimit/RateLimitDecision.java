package io.github.olaaa.redispatternslab.ratelimit;

import java.time.Duration;

/*
 * Решение ограничителя: пропустить запрос или нет, сколько запросов осталось в текущем окне
 * и через сколько повторить, если не пропустили.
 */
public record RateLimitDecision(boolean allowed, long remaining, Duration retryAfter) {

    /* count — каким по счёту в окне оказался этот запрос. */
    static RateLimitDecision of(long count, int limit, Duration untilWindowEnd) {
        boolean allowed = count <= limit;
        return new RateLimitDecision(allowed, Math.max(0, limit - count), allowed ? Duration.ZERO : untilWindowEnd);
    }
}
