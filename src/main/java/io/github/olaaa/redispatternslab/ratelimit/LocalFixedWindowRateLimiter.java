package io.github.olaaa.redispatternslab.ratelimit;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import reactor.core.publisher.Mono;

/*
 * Сценарий 1. Фиксированное окно в памяти одной реплики — для сравнения, бином не является.
 *
 * Внутри реплики считает верно, но у каждой реплики свой счётчик: три реплики с лимитом 10
 * пропустят одному клиенту до 30 запросов в окно.
 */
public class LocalFixedWindowRateLimiter implements RateLimiter {

    private record Counter(long windowStartMillis, AtomicLong count) {
    }

    private final Clock clock;
    private final RateLimitProperties properties;
    private final Map<String, Counter> counters = new ConcurrentHashMap<>();

    public LocalFixedWindowRateLimiter(Clock clock, RateLimitProperties properties) {
        this.clock = clock;
        this.properties = properties;
    }

    @Override
    public Mono<RateLimitDecision> tryAcquire(String clientId) {
        return Mono.fromSupplier(() -> {
            FixedWindow window = FixedWindow.containing(clock, properties.window());
            /* Новое окно — новый счётчик; compute атомарен для одного клиента. */
            Counter counter = counters.compute(clientId, (key, current) ->
                    current != null && current.windowStartMillis() == window.startMillis()
                            ? current
                            : new Counter(window.startMillis(), new AtomicLong()));
            return RateLimitDecision.of(counter.count().incrementAndGet(), properties.limit(), window.untilEnd());
        });
    }

    @Override
    public RateLimitAlgorithm algorithm() {
        return RateLimitAlgorithm.FIXED_WINDOW;
    }
}
