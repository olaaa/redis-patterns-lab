package io.github.olaaa.redispatternslab.ratelimit;

import java.net.InetSocketAddress;
import java.util.Optional;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

/*
 * Ограничение частоты запросов к каталогу. Клиент — заголовок X-Client-Id, без него — адрес клиента.
 * Пропущенный запрос получает X-RateLimit-Remaining, отклонённый — 429 Too Many Requests
 * и Retry-After: через сколько секунд начнётся следующее окно.
 */
@Component
public class RateLimitWebFilter implements WebFilter {

    public static final String CLIENT_ID_HEADER = "X-Client-Id";
    public static final String REMAINING_HEADER = "X-RateLimit-Remaining";
    private static final String LIMITED_PATH = "/products/";

    private final RedisFixedWindowRateLimiter rateLimiter;

    public RateLimitWebFilter(RedisFixedWindowRateLimiter rateLimiter) {
        this.rateLimiter = rateLimiter;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        if (!exchange.getRequest().getPath().value().startsWith(LIMITED_PATH)) {
            return chain.filter(exchange);
        }
        return rateLimiter.tryAcquire(clientId(exchange))
                .flatMap(decision -> {
                    if (decision.allowed()) {
                        exchange.getResponse().getHeaders().set(REMAINING_HEADER, String.valueOf(decision.remaining()));
                        return chain.filter(exchange);
                    }
                    /* Retry-After — целое число секунд, поэтому остаток окна округляется вверх. */
                    long retryAfterSeconds = Math.max(1, (decision.retryAfter().toMillis() + 999) / 1000);
                    exchange.getResponse().setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
                    exchange.getResponse().getHeaders().set("Retry-After", String.valueOf(retryAfterSeconds));
                    return exchange.getResponse().setComplete();
                });
    }

    private static String clientId(ServerWebExchange exchange) {
        String header = exchange.getRequest().getHeaders().getFirst(CLIENT_ID_HEADER);
        if (header != null && !header.isBlank()) {
            return header;
        }
        return Optional.ofNullable(exchange.getRequest().getRemoteAddress())
                .map(InetSocketAddress::getHostString)
                .orElse("unknown");
    }
}
