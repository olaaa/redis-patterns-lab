package io.github.olaaa.redispatternslab.ratelimit;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/*
 * Сколько запросов (limit) разрешено одному клиенту за окно (window) и каким алгоритмом считать.
 * Для ведра токенов limit — ёмкость ведра, а скорость пополнения — limit за window.
 */
@ConfigurationProperties("rate-limit")
public record RateLimitProperties(int limit, Duration window,
                                  @DefaultValue("FIXED_WINDOW") RateLimitAlgorithm algorithm) {
}
