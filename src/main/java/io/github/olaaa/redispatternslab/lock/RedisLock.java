package io.github.olaaa.redispatternslab.lock;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/*
 * Сценарий 3. Распределённая блокировка с арендой на одном Redis.
 *
 * Взять: SET lock:<имя> <маркер> NX PX <срок аренды> — ключ создаётся, только если его нет, и сам
 * исчезнет через срок аренды, даже если держатель упал. Маркер — случайная строка, своя у каждого
 * взятия. Снять и продлить можно только свою аренду: скрипт сравнивает маркер в ключе со своим.
 */
@Component
public class RedisLock {

    public static final String KEY_PREFIX = "lock:";

    /* Удалить ключ, только если в нём мой маркер. */
    private static final RedisScript<Long> RELEASE = RedisScript.of("""
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            end
            return 0
            """, Long.class);

    /* Продлить аренду, только если в ключе мой маркер. ARGV[2] — новый срок в миллисекундах. */
    private static final RedisScript<Long> EXTEND = RedisScript.of("""
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('PEXPIRE', KEYS[1], ARGV[2])
            end
            return 0
            """, Long.class);

    private final ReactiveStringRedisTemplate redis;

    public RedisLock(ReactiveStringRedisTemplate redis) {
        this.redis = redis;
    }

    /* Взять блокировку на срок аренды. Пустой Mono — блокировку держит кто-то другой. */
    public Mono<LockHandle> tryAcquire(String name, Duration lease) {
        return Mono.defer(() -> {
            String token = UUID.randomUUID().toString();
            return redis.opsForValue().setIfAbsent(KEY_PREFIX + name, token, lease)
                    .filter(Boolean::booleanValue)
                    .map(acquired -> new LockHandle(name, token));
        });
    }

    /* Снять свою блокировку. false — аренда уже истекла, и ключ, возможно, принадлежит другому. */
    public Mono<Boolean> release(LockHandle handle) {
        return redis.execute(RELEASE, List.of(KEY_PREFIX + handle.name()), List.of(handle.token()))
                .next()
                .map(deleted -> deleted == 1);
    }

    /* Продлить свою аренду. false — продлевать уже нечего: аренда истекла. */
    public Mono<Boolean> extend(LockHandle handle, Duration lease) {
        return redis.execute(EXTEND, List.of(KEY_PREFIX + handle.name()),
                        List.of(handle.token(), String.valueOf(lease.toMillis())))
                .next()
                .map(extended -> extended == 1);
    }

    /*
     * Так снимать нельзя — для сравнения в тесте: DEL без проверки маркера удалит и чужую блокировку,
     * если своя аренда уже истекла и блокировку взял другой.
     */
    public Mono<Boolean> releaseWithoutTokenCheck(LockHandle handle) {
        return redis.delete(KEY_PREFIX + handle.name()).map(deleted -> deleted == 1);
    }
}
