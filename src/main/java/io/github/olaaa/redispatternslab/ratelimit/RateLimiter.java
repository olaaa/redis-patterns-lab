package io.github.olaaa.redispatternslab.ratelimit;

import reactor.core.publisher.Mono;

public interface RateLimiter {

    /* Засчитать запрос клиента и решить, пропускать ли его. */
    Mono<RateLimitDecision> tryAcquire(String clientId);

    RateLimitAlgorithm algorithm();
}
