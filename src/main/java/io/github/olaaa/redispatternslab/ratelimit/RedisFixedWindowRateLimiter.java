package io.github.olaaa.redispatternslab.ratelimit;

import java.time.Clock;
import java.util.List;

import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/*
 * Сценарий 1. Фиксированное окно в Redis: один счётчик на клиента и окно, общий для всех реплик.
 *
 * Ключ — rate:fixed:<клиент>:<начало окна>. Каждый запрос — INCR этого ключа; число после
 * увеличения и есть номер запроса в окне. Счётчик удаляет сам Redis: при первом запросе
 * в окне ключу ставится срок жизни, равный длине окна.
 */
@Component
public class RedisFixedWindowRateLimiter implements RateLimiter {

    public static final String KEY_PREFIX = "rate:fixed:";

    /*
     * INCR и PEXPIRE — одним скриптом. Отдельными командами срок мог бы не встать: процесс упал
     * между ними — и счётчик остался бы в Redis навсегда. Здесь у каждого окна свой ключ, так что
     * это утечка памяти; при одном ключе на клиента клиент вдобавок был бы заблокирован навсегда.
     * KEYS[1] — ключ счётчика, ARGV[1] — длина окна в миллисекундах.
     */
    private static final RedisScript<Long> INCREMENT_IN_WINDOW = RedisScript.of("""
            local count = redis.call('INCR', KEYS[1])
            if count == 1 then
                redis.call('PEXPIRE', KEYS[1], ARGV[1])
            end
            return count
            """, Long.class);

    private final ReactiveStringRedisTemplate redis;
    private final Clock clock;
    private final RateLimitProperties properties;

    public RedisFixedWindowRateLimiter(ReactiveStringRedisTemplate redis, Clock clock, RateLimitProperties properties) {
        this.redis = redis;
        this.clock = clock;
        this.properties = properties;
    }

    @Override
    public Mono<RateLimitDecision> tryAcquire(String clientId) {
        return Mono.defer(() -> {
            FixedWindow window = FixedWindow.containing(clock, properties.window());
            String key = KEY_PREFIX + clientId + ":" + window.startMillis();
            return redis.execute(INCREMENT_IN_WINDOW, List.of(key),
                            List.of(String.valueOf(properties.window().toMillis())))
                    .next()
                    .map(count -> RateLimitDecision.of(count, properties.limit(), window.untilEnd()));
        });
    }

    @Override
    public RateLimitAlgorithm algorithm() {
        return RateLimitAlgorithm.FIXED_WINDOW;
    }
}
