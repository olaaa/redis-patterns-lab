package io.github.olaaa.redispatternslab.ratelimit;

import java.time.Clock;
import java.util.List;

import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/*
 * Сценарий 2. Ведро токенов: ведро на limit токенов пополняется со скоростью limit за window,
 * каждый запрос забирает один токен; нет токена — запрос отклоняется.
 *
 * Ведро клиента — хеш rate:bucket:<клиент> с двумя полями: сколько токенов и когда пересчитывали.
 * Токены не начисляются по таймеру: при каждом запросе скрипт досчитывает, сколько набежало
 * с прошлого раза. Память — один хеш на клиента. Допускает всплеск до ёмкости ведра в любой момент,
 * но не больше: за любой отрезок T пройдёт не больше limit + скорость × T запросов.
 */
@Component
public class RedisTokenBucketRateLimiter implements RateLimiter {

    public static final String KEY_PREFIX = "rate:bucket:";

    /*
     * KEYS[1] — ведро; ARGV[1] — сейчас, ARGV[2] — ёмкость, ARGV[3] — окно, за которое ведро наполняется целиком.
     * Ответ: {пропущен 1/0, осталось целых токенов, через сколько мс появится токен}.
     * Срок жизни ключа — время полного наполнения: дальше ведро всё равно полное, хранить его незачем.
     */
    private static final RedisScript<List> TOKEN_BUCKET = RedisScript.of("""
            local now = tonumber(ARGV[1])
            local capacity = tonumber(ARGV[2])
            local fill_time = tonumber(ARGV[3])
            local rate = capacity / fill_time
            local state = redis.call('HMGET', KEYS[1], 'tokens', 'updated_at')
            local tokens = tonumber(state[1]) or capacity
            local updated_at = tonumber(state[2]) or now
            tokens = math.min(capacity, tokens + math.max(0, now - updated_at) * rate)
            local allowed = 0
            if tokens >= 1 then
                tokens = tokens - 1
                allowed = 1
            end
            redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'updated_at', now)
            redis.call('PEXPIRE', KEYS[1], fill_time)
            local retry_after = 0
            if allowed == 0 then
                retry_after = math.ceil((1 - tokens) / rate)
            end
            return {allowed, math.floor(tokens), retry_after}
            """, List.class);

    private final ReactiveStringRedisTemplate redis;
    private final Clock clock;
    private final RateLimitProperties properties;

    public RedisTokenBucketRateLimiter(ReactiveStringRedisTemplate redis, Clock clock, RateLimitProperties properties) {
        this.redis = redis;
        this.clock = clock;
        this.properties = properties;
    }

    @Override
    public Mono<RateLimitDecision> tryAcquire(String clientId) {
        return Mono.defer(() -> redis.execute(TOKEN_BUCKET, List.of(KEY_PREFIX + clientId),
                        List.of(String.valueOf(clock.millis()), String.valueOf(properties.limit()),
                                String.valueOf(properties.window().toMillis())))
                .next()
                .map(RedisSlidingLogRateLimiter::toDecision));
    }

    @Override
    public RateLimitAlgorithm algorithm() {
        return RateLimitAlgorithm.TOKEN_BUCKET;
    }
}
