package io.github.olaaa.redispatternslab.ratelimit;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/* Сколько запросов (limit) разрешено одному клиенту за одно окно (window). Задаются в application.yml. */
@ConfigurationProperties("rate-limit")
public record RateLimitProperties(int limit, Duration window) {
}
