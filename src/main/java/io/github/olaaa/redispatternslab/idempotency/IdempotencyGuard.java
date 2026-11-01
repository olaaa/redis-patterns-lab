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
//  begun == 1 Lua-запрос создал запись «в работе»
                    .flatMap(begun -> begun == 1
                            ? runAndRemember(key, token, action)
// answerFromRecord() читает запись и не выполняет оплату
                            : answerFromRecord(key, fingerprint, () -> execute(clientId, idempotencyKey, fingerprint, action)));
        });
    }

    /**
     * Выполняет действие от имени запроса, который захватил ключ, и запоминает итог в Redis.
     * <p>
     * Вызывается только после того, как скрипт {@code BEGIN} вернул 1: этот запрос создал запись
     * {@code idem:<клиент>:<ключ>} в состоянии {@code IN_PROGRESS} со своим токеном.
     * <ul>
     *     <li><b>Действие вернуло ответ</b> (в оплате — 201 со списанием или 402, если провайдер отказал).
     *     Скрипт {@code COMPLETE} переводит запись в {@code COMPLETED}, сохраняет статус и тело и ставит
     *     срок {@code idempotency.completed-ttl}. Повторы с тем же ключом получат этот ответ без нового
     *     выполнения действия.</li>
     *     <li><b>Действие завершилось ошибкой</b> (сбой, а не отказ). Скрипт {@code RELEASE} удаляет запись,
     *     и исходная ошибка уходит вызывающему. Ответ не сохраняется: повтор с тем же ключом выполнит
     *     действие заново, а от второго списания защищает ключ, переданный провайдеру.</li>
     * </ul>
     * Оба скрипта меняют запись, только если в ней токен этого запроса. Если запись за время действия
     * истекла ({@code idempotency.in-progress-ttl}) и её занял другой запрос, чужая запись не трогается.
     * Для {@code COMPLETE} это значит, что ответ уходит клиенту, но в Redis не сохраняется: результат
     * скрипта не проверяется.
     * <p>
     * Ошибка самого Redis на {@code COMPLETE} тоже попадает в {@code onErrorResume}: запись удаляется,
     * и клиент получает ошибку, хотя действие уже выполнено. Если Redis не ответит и на {@code RELEASE},
     * вызывающему уходит ошибка Redis вместо исходной, а запись {@code IN_PROGRESS} остаётся до истечения
     * срока, и повторы до этого получают 409.
     *
     * @param key    ключ записи в Redis: {@code idem:<клиент>:<ключ идемпотентности>}
     * @param token  токен этого запроса, который скрипт {@code BEGIN} записал в запись
     * @param action действие, например оплата; вызывается лениво, при подписке на результат
     * @return итог с ответом действия и {@code replayed = false}; при ошибке действия — та же ошибка
     */
    private Mono<IdempotentOutcome> runAndRemember(String key, String token, Supplier<Mono<StoredResponse>> action) {
        return Mono.defer(action)
                .flatMap(response -> redis.execute(COMPLETE, List.of(key),
                                List.of(token, String.valueOf(response.status()), response.body(),
                                        String.valueOf(properties.completedTtl().toMillis())))
                        .then(Mono.just(new IdempotentOutcome(response, false))))
                .onErrorResume(error -> redis.execute(RELEASE, List.of(key), List.of(token))
                        .then(Mono.error(error)));
    }

    /**
     * Отвечает на запрос, который не захватил ключ: скрипт {@code BEGIN} вернул 0, потому что запись
     * {@code idem:<клиент>:<ключ>} уже была. Действие здесь не выполняется — ответ строится по записи,
     * прочитанной {@code HGETALL}.
     * <ul>
     *     <li><b>Запись пустая</b> — её уже нет, вызывается {@code retry} (см. ниже).</li>
     *     <li><b>Отпечаток другой</b> — 422: тот же ключ прислан с другим запросом. Отпечаток проверяется
     *     раньше состояния, поэтому другой запрос получает 422, даже пока первый ещё в работе.</li>
     *     <li><b>{@code IN_PROGRESS}</b> — 409: первый запрос с этим ключом ещё выполняется. Не сохраняется.</li>
     *     <li><b>{@code COMPLETED}</b> — сохранённый статус и тело с {@code replayed = true}.</li>
     * </ul>
     * <b>Когда выполняется {@code retry}.</b> {@code BEGIN} и {@code HGETALL} — две отдельные команды Redis,
     * не одна атомарная операция. Между ними Redis успевает выполнить команды других запросов, и запись,
     * которая была на {@code BEGIN}, к моменту чтения может исчезнуть. Тогда {@code HGETALL} по несуществующему
     * ключу возвращает пустой хеш. Исчезнуть запись может так:
     * <ol>
     *     <li><b>Успел произойти {@code RELEASE}.</b> Запрос A захватил ключ, и его действие закончилось сбоем
     *     (не отказом провайдера — отказ сохраняется как ответ). {@code runAndRemember} вызвал {@code RELEASE},
     *     и тот удалил запись A. Порядок событий:
     *     <ol>
     *         <li>A: {@code BEGIN} вернул 1, запись {@code IN_PROGRESS}, A вызывает провайдера;</li>
     *         <li>этот запрос B: {@code BEGIN} вернул 0 — запись A есть;</li>
     *         <li>A: провайдер ответил ошибкой, {@code RELEASE} удалил запись;</li>
     *         <li>B: {@code HGETALL} вернул пустой хеш — вызывается {@code retry}.</li>
     *     </ol>
     *     Ответа у A нет, сохранять нечего, а операция свободна: B должен выполнить её сам, а не отвечать
     *     409 или ошибкой.</li>
     *     <li><b>Истёк срок записи.</b> {@code IN_PROGRESS} живёт {@code idempotency.in-progress-ttl}: реплика A
     *     упала или зависла и не дошла ни до {@code COMPLETE}, ни до {@code RELEASE}. {@code COMPLETED} живёт
     *     {@code idempotency.completed-ttl}: повтор пришёл ровно на границе срока хранения ответа.</li>
     * </ol>
     * {@code retry} — это новый вызов {@code execute}: новый токен и новый {@code BEGIN}, а не выполнение
     * действия напрямую. Если ключ свободен, {@code BEGIN} вернёт 1, и этот запрос выполнит действие сам.
     * Если за это время запись создал ещё один повтор, {@code BEGIN} вернёт 0, и ответ снова построится
     * по записи — 409 или сохранённый ответ. В обоих случаях после сбоя A действие повторяется у провайдера,
     * и от второго списания, если A успел списать деньги, защищает ключ, переданный провайдеру, а не Redis.
     *
     * @param key         ключ записи в Redis: {@code idem:<клиент>:<ключ идемпотентности>}
     * @param fingerprint отпечаток тела этого запроса
     * @param retry       повторный вызов {@code execute} с теми же аргументами, на случай пустой записи
     * @return ответ по записи: 422, 409 или сохранённый ответ; при пустой записи — результат {@code retry}
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
