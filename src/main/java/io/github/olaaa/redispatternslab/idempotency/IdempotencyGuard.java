package io.github.olaaa.redispatternslab.idempotency;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/*
 * Сценарий 4. Идемпотентность запросов: запрос с тем же ключом выполняется один раз,
 * повторы получают сохранённый ответ.
 *
 * Запись о ключе — хеш idem:<клиент>:<ключ> с полями state (IN_PROGRESS или COMPLETED), token (кто
 * обрабатывает), fingerprint (отпечаток тела запроса), status и body (сохранённый ответ).
 *
 * - Ключа нет — записать IN_PROGRESS (атомарно, только если ключа нет), выполнить, сохранить ответ — COMPLETED.
 * - Ключ есть, отпечаток другой — 422: тот же ключ с другим запросом.
 * - Ключ есть, IN_PROGRESS — 409: первый запрос ещё обрабатывается.
 * - Ключ есть, COMPLETED — сохранённый ответ, без повторного выполнения.
 * - Обработка упала — запись удаляется: повтор с тем же ключом выполнится заново.
 */
@Component
public class IdempotencyGuard {

    public static final String KEY_PREFIX = "idem:";

    /* Создать запись «в работе», только если ключа ещё нет. ARGV: токен, отпечаток, срок «в работе» в мс. */
    private static final RedisScript<Long> BEGIN = RedisScript.of("""
            if redis.call('EXISTS', KEYS[1]) == 1 then
                return 0
            end
            redis.call('HSET', KEYS[1], 'state', 'IN_PROGRESS', 'token', ARGV[1], 'fingerprint', ARGV[2])
            redis.call('PEXPIRE', KEYS[1], ARGV[3])
            return 1
            """, Long.class);

    /* Сохранить ответ, только если запись своя. ARGV: токен, статус, тело, срок хранения ответа в мс. */
    private static final RedisScript<Long> COMPLETE = RedisScript.of("""
            if redis.call('HGET', KEYS[1], 'token') ~= ARGV[1] then
                return 0
            end
            redis.call('HSET', KEYS[1], 'state', 'COMPLETED', 'status', ARGV[2], 'body', ARGV[3])
            redis.call('PEXPIRE', KEYS[1], ARGV[4])
            return 1
            """, Long.class);

    /* Удалить свою запись «в работе» после сбоя. ARGV: токен. */
    private static final RedisScript<Long> RELEASE = RedisScript.of("""
            if redis.call('HGET', KEYS[1], 'token') == ARGV[1] and redis.call('HGET', KEYS[1], 'state') == 'IN_PROGRESS' then
                return redis.call('DEL', KEYS[1])
            end
            return 0
            """, Long.class);

    private final ReactiveStringRedisTemplate redis;
    private final IdempotencyProperties properties;

    public IdempotencyGuard(ReactiveStringRedisTemplate redis, IdempotencyProperties properties) {
        this.redis = redis;
        this.properties = properties;
    }

    public Mono<IdempotentOutcome> execute(String clientId, String idempotencyKey, String fingerprint,
                                           Supplier<Mono<StoredResponse>> action) {
        String key = KEY_PREFIX + clientId + ":" + idempotencyKey;
        return Mono.defer(() -> {
            String token = UUID.randomUUID().toString();
            return redis.execute(BEGIN, List.of(key),
                            List.of(token, fingerprint, String.valueOf(properties.inProgressTtl().toMillis())))
                    .next()
                    .flatMap(begun -> begun == 1
                            ? runAndRemember(key, token, action)
                            : answerFromRecord(key, fingerprint, () -> execute(clientId, idempotencyKey, fingerprint, action)));
        });
    }

    private Mono<IdempotentOutcome> runAndRemember(String key, String token, Supplier<Mono<StoredResponse>> action) {
        return Mono.defer(action)
                .flatMap(response -> redis.execute(COMPLETE, List.of(key),
                                List.of(token, String.valueOf(response.status()), response.body(),
                                        String.valueOf(properties.completedTtl().toMillis())))
                        .then(Mono.just(new IdempotentOutcome(response, false))))
                .onErrorResume(error -> redis.execute(RELEASE, List.of(key), List.of(token))
                        .then(Mono.error(error)));
    }

    /*
     * Ключ уже есть. Запись могла истечь между BEGIN и чтением — тогда HGETALL пустой,
     * и запрос пробует начать заново (retry).
     */
    private Mono<IdempotentOutcome> answerFromRecord(String key, String fingerprint,
                                                     Supplier<Mono<IdempotentOutcome>> retry) {
        return redis.<String, String>opsForHash().entries(key)
                .collectMap(Map.Entry::getKey, Map.Entry::getValue)
                .flatMap(record -> {
                    if (record.isEmpty()) {
                        return retry.get();
                    }
                    if (!fingerprint.equals(record.get("fingerprint"))) {
                        return Mono.just(new IdempotentOutcome(new StoredResponse(422,
                                "ключ идемпотентности уже использован с другим запросом"), false));
                    }
                    if ("IN_PROGRESS".equals(record.get("state"))) {
                        return Mono.just(new IdempotentOutcome(new StoredResponse(409,
                                "запрос с этим ключом идемпотентности ещё обрабатывается"), false));
                    }
                    return Mono.just(new IdempotentOutcome(
                            new StoredResponse(Integer.parseInt(record.get("status")), record.get("body")), true));
                });
    }
}
