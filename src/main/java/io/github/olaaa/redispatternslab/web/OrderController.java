package io.github.olaaa.redispatternslab.web;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

import io.github.olaaa.redispatternslab.idempotency.IdempotentOutcome;
import io.github.olaaa.redispatternslab.idempotency.IdempotentRequests;
import io.github.olaaa.redispatternslab.idempotency.StoredResponse;
import io.github.olaaa.redispatternslab.order.Order;
import io.github.olaaa.redispatternslab.order.OrderRequest;
import io.github.olaaa.redispatternslab.order.OrderService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import tools.jackson.databind.json.JsonMapper;

/*
 * Сценарий 4. Создание заказа. С заголовком Idempotency-Key повтор того же запроса не создаёт второй заказ,
 * а получает сохранённый ответ с заголовком Idempotent-Replayed: true. Без заголовка каждый запрос — новый
 * заказ: так видно, от чего защищает ключ.
 */
@RestController
public class OrderController {

    public static final String CLIENT_ID_HEADER = "X-Client-Id";
    public static final String IDEMPOTENCY_KEY_HEADER = "Idempotency-Key";
    public static final String REPLAYED_HEADER = "Idempotent-Replayed";

    private final OrderService orderService;
    private final IdempotentRequests idempotentRequests;
    private final JsonMapper jsonMapper;

    public OrderController(OrderService orderService, IdempotentRequests idempotentRequests, JsonMapper jsonMapper) {
        this.orderService = orderService;
        this.idempotentRequests = idempotentRequests;
        this.jsonMapper = jsonMapper;
    }

    @PostMapping("/orders")
    public Mono<ResponseEntity<String>> create(@RequestHeader(CLIENT_ID_HEADER) String clientId,
                                               @RequestHeader(value = IDEMPOTENCY_KEY_HEADER, required = false) String idempotencyKey,
                                               @RequestBody OrderRequest request) {
        if (idempotencyKey == null) {
            return createOrder(clientId, null, request).map(response -> toEntity(response, false));
        }
        return idempotentRequests.execute(clientId, idempotencyKey, fingerprint(request),
                        () -> createOrder(clientId, idempotencyKey, request))
                .map(outcome -> toEntity(outcome.response(), outcome.replayed()));
    }

    /* Создать заказ и превратить результат в ответ: 201 с заказом или 404, если товара нет. */
    private Mono<StoredResponse> createOrder(String clientId, String idempotencyKey, OrderRequest request) {
        return orderService.create(clientId, idempotencyKey, request)
                .map(order -> new StoredResponse(201, json(order)))
                .defaultIfEmpty(new StoredResponse(404, "товар " + request.productId() + " не найден"));
    }

    /*
     * Отпечаток запроса — SHA-256 от его содержимого. Повтор с тем же ключом, но другим содержимым —
     * ошибка клиента, а не повтор.
     */
    public static String fingerprint(OrderRequest request) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest((request.productId() + ":" + request.quantity()).getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private String json(Order order) {
        return jsonMapper.writeValueAsString(order);
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
