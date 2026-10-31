package io.github.olaaa.redispatternslab.ratelimit;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/*
 * Сценарий 2. Скользящее окно по журналу запросов: в любой отрезок длиной window — не больше limit запросов.
 *
 * Журнал клиента — сортированное множество (ZSET) rate:log:<клиент>: элемент — запрос, оценка (score) —
 * время запроса в миллисекундах. На каждый запрос скрипт удаляет записи старше окна, считает оставшиеся
 * и, если их меньше лимита, записывает новый запрос. Точно, но память — до limit записей на клиента.
 */
@Component
public class RedisSlidingLogRateLimiter implements RateLimiter {

    public static final String KEY_PREFIX = "rate:log:";

    /*
     * KEYS[1] — журнал; ARGV[1] — сейчас, ARGV[2] — окно, ARGV[3] — лимит, ARGV[4] — уникальный id запроса.
     * Ответ: {пропущен 1/0, осталось, через сколько мс повторить}.
     * Отклонённый запрос в журнал не пишется: иначе клиент, который шлёт без остановки, не прошёл бы никогда.
     */
    private static final RedisScript<List> SLIDING_LOG = RedisScript.of("""
            local now = tonumber(ARGV[1])
            local window = tonumber(ARGV[2])
            local limit = tonumber(ARGV[3])
            redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - window)
            local count = redis.call('ZCARD', KEYS[1])
            if count < limit then
                redis.call('ZADD', KEYS[1], now, ARGV[4])
                redis.call('PEXPIRE', KEYS[1], window)
                return {1, limit - count - 1, 0}
            end
            local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
            return {0, 0, tonumber(oldest[2]) + window - now}
            """, List.class);

    private final ReactiveStringRedisTemplate redis;
    private final Clock clock;
    private final RateLimitProperties properties;

    public RedisSlidingLogRateLimiter(ReactiveStringRedisTemplate redis, Clock clock, RateLimitProperties properties) {
        this.redis = redis;
        this.clock = clock;
        this.properties = properties;
    }

    @Override
    public Mono<RateLimitDecision> tryAcquire(String clientId) {
        return Mono.defer(() -> redis.execute(SLIDING_LOG, List.of(KEY_PREFIX + clientId),
                        List.of(String.valueOf(clock.millis()), String.valueOf(properties.window().toMillis()),
                                String.valueOf(properties.limit()), UUID.randomUUID().toString()))
                .next()
                .map(RedisSlidingLogRateLimiter::toDecision));
    }

    @Override
    public RateLimitAlgorithm algorithm() {
        return RateLimitAlgorithm.SLIDING_LOG;
    }

    static RateLimitDecision toDecision(List<?> reply) {
        return new RateLimitDecision(((Long) reply.get(0)) == 1, (Long) reply.get(1),
                Duration.ofMillis((Long) reply.get(2)));
    }
}
