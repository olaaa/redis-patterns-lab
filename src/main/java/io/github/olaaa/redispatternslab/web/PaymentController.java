package io.github.olaaa.redispatternslab.web;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

import io.github.olaaa.redispatternslab.idempotency.IdempotencyGuard;
import io.github.olaaa.redispatternslab.idempotency.StoredResponse;
import io.github.olaaa.redispatternslab.payment.PaymentDeclinedException;
import io.github.olaaa.redispatternslab.payment.PaymentRequest;
import io.github.olaaa.redispatternslab.payment.PaymentService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import tools.jackson.databind.json.JsonMapper;

/*
 * Сценарий 4. Оплата заказа. Заголовок Idempotency-Key обязателен: без него повтор после таймаута
 * списал бы деньги второй раз, поэтому без ключа — 400. Повтор с тем же ключом получает сохранённый
 * ответ с заголовком Idempotent-Replayed: true, деньги второй раз не списываются.
 */
@RestController
public class PaymentController {

    public static final String CLIENT_ID_HEADER = "X-Client-Id";
    public static final String IDEMPOTENCY_KEY_HEADER = "Idempotency-Key";
    public static final String REPLAYED_HEADER = "Idempotent-Replayed";

    private final PaymentService paymentService;
    private final IdempotencyGuard idempotencyGuard;
    private final JsonMapper jsonMapper;

    public PaymentController(PaymentService paymentService, IdempotencyGuard idempotencyGuard,
                             JsonMapper jsonMapper) {
        this.paymentService = paymentService;
        this.idempotencyGuard = idempotencyGuard;
        this.jsonMapper = jsonMapper;
    }

    @PostMapping("/payments")
    public Mono<ResponseEntity<String>> pay(@RequestHeader(CLIENT_ID_HEADER) String clientId,
                                            @RequestHeader(value = IDEMPOTENCY_KEY_HEADER, required = false) String idempotencyKey,
                                            @RequestBody PaymentRequest request) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            return Mono.just(ResponseEntity.badRequest().contentType(MediaType.TEXT_PLAIN)
                    .body("нужен заголовок " + IDEMPOTENCY_KEY_HEADER));
        }
        return idempotencyGuard.execute(clientId, idempotencyKey, fingerprint(request),
                        () -> pay(paymentService, clientId, idempotencyKey, request, jsonMapper))
                .map(outcome -> toEntity(outcome.response(), outcome.replayed()));
    }

    /*
     * Списать и превратить результат в ответ: 201 со списанием или 402, если провайдер отказал.
     * Оба — результат обработки, их сохранят и повторят. Остальные ошибки (сбой) уходят дальше.
     */
    public static Mono<StoredResponse> pay(PaymentService paymentService, String clientId, String idempotencyKey,
                                           PaymentRequest request, JsonMapper jsonMapper) {
        return paymentService.pay(clientId, idempotencyKey, request)
                .map(charge -> new StoredResponse(201, jsonMapper.writeValueAsString(charge)))
                .onErrorResume(PaymentDeclinedException.class,
                        declined -> Mono.just(new StoredResponse(402, declined.getMessage())));
    }

    /*
     * Отпечаток запроса — SHA-256 от заказа и суммы. Сумма нормализована: 2490 и 2490.00 — одна сумма.
     * Повтор с тем же ключом, но другим заказом или суммой — ошибка клиента, а не повтор.
     */
    public static String fingerprint(PaymentRequest request) {
        try {
            String canonical = request.orderId() + ":" + request.amount().stripTrailingZeros().toPlainString();
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static ResponseEntity<String> toEntity(StoredResponse response, boolean replayed) {
        ResponseEntity.BodyBuilder builder = ResponseEntity.status(response.status());
        if (replayed) {
            builder.header(REPLAYED_HEADER, "true");
        }
        MediaType contentType = response.status() == 201 ? MediaType.APPLICATION_JSON : MediaType.TEXT_PLAIN;
        return builder.contentType(contentType).body(response.body());
    }
}
