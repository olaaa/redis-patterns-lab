package io.github.olaaa.redispatternslab.ratelimit;

/* Алгоритм ограничителя, который использует фильтр: rate-limit.algorithm в application.yml. */
public enum RateLimitAlgorithm {
    FIXED_WINDOW, SLIDING_LOG, TOKEN_BUCKET
}
