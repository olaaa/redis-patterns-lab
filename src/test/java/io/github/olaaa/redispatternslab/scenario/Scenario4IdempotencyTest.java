package io.github.olaaa.redispatternslab.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.github.olaaa.redispatternslab.idempotency.IdempotentRequests;
import io.github.olaaa.redispatternslab.order.OrderRequest;
import io.github.olaaa.redispatternslab.web.OrderController;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.web.reactive.server.WebTestClient;

/* Сценарий 4. Идемпотентность запросов: повтор создания заказа с тем же ключом не создаёт второй заказ. */
class Scenario4IdempotencyTest extends ScenarioTestSupport {

    private static final String CLIENT = "mobile-app";
    private static final String KEY = "6f1c2a9e-order-1";
    private static final String RECORD_KEY = IdempotentRequests.KEY_PREFIX + CLIENT + ":" + KEY;
    private static final OrderRequest KETTLE_ORDER = new OrderRequest(1, 2);

    @Autowired
    OrderController orders;

    @Autowired
    ApplicationContext applicationContext;

    @Test
    void retryWithoutKeyCreatesSecondOrder() {
        ResponseEntity<String> first = orders.create(CLIENT, null, KETTLE_ORDER).block();
        ResponseEntity<String> retry = orders.create(CLIENT, null, KETTLE_ORDER).block();

        assertThat(first.getStatusCode().value()).isEqualTo(201);
        assertThat(retry.getStatusCode().value()).isEqualTo(201);
        assertThat(retry.getBody()).as("повтор создал другой заказ").isNotEqualTo(first.getBody());
        assertThat(orderCount()).as("клиент не дождался ответа, повторил — и получил два заказа").isEqualTo(2);
    }

    @Test
    void retryWithSameKeyGetsSavedResponseAndCreatesOneOrder() {
        WebTestClient client = WebTestClient.bindToApplicationContext(applicationContext).build();

        String first = client.post().uri("/orders")
                .header(OrderController.CLIENT_ID_HEADER, CLIENT)
                .header(OrderController.IDEMPOTENCY_KEY_HEADER, KEY)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(KETTLE_ORDER)
                .exchange()
                .expectStatus().isCreated()
                .expectHeader().doesNotExist(OrderController.REPLAYED_HEADER)
                .expectBody(String.class).returnResult().getResponseBody();

        client.post().uri("/orders")
                .header(OrderController.CLIENT_ID_HEADER, CLIENT)
                .header(OrderController.IDEMPOTENCY_KEY_HEADER, KEY)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(KETTLE_ORDER)
                .exchange()
                .expectStatus().isCreated()
                .expectHeader().valueEquals(OrderController.REPLAYED_HEADER, "true")
                .expectBody(String.class).isEqualTo(first);

        assertThat(orderCount()).isEqualTo(1);
    }

    @Test
    void retryWhileFirstIsInProgressGetsConflict() {
        CompletableFuture<ResponseEntity<String>> first = orders.create(CLIENT, KEY, KETTLE_ORDER).toFuture();
        await().atMost(Duration.ofSeconds(5)).until(() -> "IN_PROGRESS".equals(recordField("state")));

        ResponseEntity<String> concurrent = orders.create(CLIENT, KEY, KETTLE_ORDER).block();
        assertThat(concurrent.getStatusCode().value())
                .as("первый запрос ещё в работе — повтор получает 409, а не второй заказ")
                .isEqualTo(409);

        ResponseEntity<String> firstResponse = awaitResult(first);
        ResponseEntity<String> later = orders.create(CLIENT, KEY, KETTLE_ORDER).block();
        assertThat(later.getStatusCode().value()).isEqualTo(201);
        assertThat(later.getBody()).isEqualTo(firstResponse.getBody());
        assertThat(later.getHeaders().getFirst(OrderController.REPLAYED_HEADER)).isEqualTo("true");
        assertThat(orderCount()).isEqualTo(1);
    }

    @Test
    void sameKeyWithDifferentRequestIsRejected() {
        orders.create(CLIENT, KEY, KETTLE_ORDER).block();

        ResponseEntity<String> otherRequest = orders.create(CLIENT, KEY, new OrderRequest(1, 5)).block();

        assertThat(otherRequest.getStatusCode().value())
                .as("тот же ключ, другое тело — ошибка клиента, а не повтор")
                .isEqualTo(422);
        assertThat(orderCount()).isEqualTo(1);
    }

    @Test
    void failureAfterSavingReleasesKeyAndDatabaseKeepsOneOrder() {
        orderFaults.setFailAfterSave(true);

        assertThatThrownBy(() -> orders.create(CLIENT, KEY, KETTLE_ORDER).block())
                .hasMessageContaining("реплика упала");
        assertThat(redis.hasKey(RECORD_KEY).block())
                .as("обработка упала — запись о ключе удалена, повтор выполнится заново")
                .isFalse();
        assertThat(orderCount()).as("но заказ в базе уже записан").isEqualTo(1);

        orderFaults.setFailAfterSave(false);
        ResponseEntity<String> retry = orders.create(CLIENT, KEY, KETTLE_ORDER).block();

        assertThat(retry.getStatusCode().value()).isEqualTo(201);
        assertThat(retry.getBody())
                .as("повтор получил тот заказ, что записал первый запрос")
                .contains("\"id\":" + orderRepository.findByIdempotencyKey(CLIENT, KEY).block().id());
        assertThat(orderCount()).as("уникальный ключ в базе не пустил второй заказ").isEqualTo(1);
        assertThat(orderService.foundByIdempotencyKey()).isEqualTo(1);
    }

    @Test
    void keyHeldByCrashedReplicaFreesAfterInProgressTtl() {
        /* Реплика взяла ключ и упала до записи в базу: запись «в работе» осталась, снять её некому. */
        redis.opsForHash().putAll(RECORD_KEY, Map.of(
                "state", "IN_PROGRESS",
                "token", "упавшая реплика",
                "fingerprint", OrderController.fingerprint(KETTLE_ORDER))).block();
        redis.expire(RECORD_KEY, Duration.ofMillis(500)).block();

        assertThat(orders.create(CLIENT, KEY, KETTLE_ORDER).block().getStatusCode().value()).isEqualTo(409);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> orders.create(CLIENT, KEY, KETTLE_ORDER).block().getStatusCode().value() == 201);

        assertThat(orderCount()).as("после истечения записи «в работе» заказ создан один раз").isEqualTo(1);
    }

    @Test
    void sameKeyFromDifferentClientsCreatesTwoOrders() {
        orders.create(CLIENT, KEY, KETTLE_ORDER).block();
        orders.create("partner", KEY, KETTLE_ORDER).block();

        assertThat(orderCount()).isEqualTo(1);
        assertThat(orderRepository.countByClient("partner").block())
                .as("ключ уникален в пределах клиента: у другого клиента он не мешает")
                .isEqualTo(1);
    }

    @Test
    void notFoundResponseIsSavedAndReplayedToo() {
        OrderRequest missingProduct = new OrderRequest(999, 1);

        ResponseEntity<String> first = orders.create(CLIENT, KEY, missingProduct).block();
        ResponseEntity<String> retry = orders.create(CLIENT, KEY, missingProduct).block();

        assertThat(first.getStatusCode().value()).isEqualTo(404);
        assertThat(retry.getStatusCode().value()).isEqualTo(404);
        assertThat(retry.getHeaders().getFirst(OrderController.REPLAYED_HEADER))
                .as("обработка началась и дала ответ — он сохранён, как и успешный")
                .isEqualTo("true");
    }

    @Test
    void completedRecordKeepsResponseForDay() {
        orders.create(CLIENT, KEY, KETTLE_ORDER).block();

        assertThat(recordField("state")).isEqualTo("COMPLETED");
        assertThat(recordField("status")).isEqualTo("201");
        assertThat(redis.getExpire(RECORD_KEY).block())
                .isGreaterThan(Duration.ofHours(23))
                .isLessThanOrEqualTo(Duration.ofHours(24));
    }

    private long orderCount() {
        return orderRepository.countByClient(CLIENT).block();
    }

    private String recordField(String field) {
        Object value = redis.opsForHash().get(RECORD_KEY, field).block();
        return value == null ? null : value.toString();
    }

    private static ResponseEntity<String> awaitResult(CompletableFuture<ResponseEntity<String>> request) {
        try {
            return request.get(10, TimeUnit.SECONDS);
        } catch (Exception exception) {
            throw new IllegalStateException("запрос не завершился", exception);
        }
    }
}
