package io.github.olaaa.redispatternslab.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.github.olaaa.redispatternslab.idempotency.IdempotentOutcome;
import io.github.olaaa.redispatternslab.idempotency.IdempotencyGuard;
import io.github.olaaa.redispatternslab.payment.PaymentRequest;
import io.github.olaaa.redispatternslab.payment.PaymentService;
import io.github.olaaa.redispatternslab.web.PaymentController;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;
import tools.jackson.databind.json.JsonMapper;

/* Сценарий 4. Идемпотентность запросов: повтор оплаты с тем же ключом не списывает деньги второй раз. */
class Scenario4IdempotencyTest extends ScenarioTestSupport {

    private static final String CLIENT = "mobile-app";
    private static final String KEY = "6f1c2a9e-pay-1";
    private static final String RECORD_KEY = IdempotencyGuard.KEY_PREFIX + CLIENT + ":" + KEY;
    private static final PaymentRequest PAYMENT = new PaymentRequest("order-42", new BigDecimal("4980.00"));

    @Autowired
    PaymentController payments;

    @Autowired
    PaymentService paymentService;

    @Autowired
    IdempotencyGuard idempotencyGuard;

    @Autowired
    JsonMapper jsonMapper;

    @Autowired
    ApplicationContext applicationContext;

    @Test
    void retryWithoutIdempotencyChargesTwice() {
        /* Без слоя идемпотентности: ответ не дошёл, клиент повторил — второй вызов сервиса оплаты. */
        paymentService.pay(CLIENT, null, PAYMENT).block();
        paymentService.pay(CLIENT, null, PAYMENT).block();

        assertThat(paymentProvider.charges()).as("деньги списаны дважды").hasSize(2);
    }

    @Test
    void paymentWithoutKeyIsRejected() {
        ResponseEntity<String> response = payments.pay(CLIENT, null, PAYMENT).block();

        assertThat(response.getStatusCode().value()).as("без ключа оплата не принимается").isEqualTo(400);
        assertThat(paymentProvider.callCount()).isZero();
    }

    @Test
    void retryWithSameKeyGetsSavedResponseAndChargesOnce() {
        WebTestClient client = WebTestClient.bindToApplicationContext(applicationContext).build();

        String first = client.post().uri("/payments")
                .header(PaymentController.CLIENT_ID_HEADER, CLIENT)
                .header(PaymentController.IDEMPOTENCY_KEY_HEADER, KEY)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(PAYMENT)
                .exchange()
                .expectStatus().isCreated()
                .expectHeader().doesNotExist(PaymentController.REPLAYED_HEADER)
                .expectBody(String.class).returnResult().getResponseBody();

        client.post().uri("/payments")
                .header(PaymentController.CLIENT_ID_HEADER, CLIENT)
                .header(PaymentController.IDEMPOTENCY_KEY_HEADER, KEY)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(PAYMENT)
                .exchange()
                .expectStatus().isCreated()
                .expectHeader().valueEquals(PaymentController.REPLAYED_HEADER, "true")
                .expectBody(String.class).isEqualTo(first);

        assertThat(paymentProvider.callCount()).as("повтор до провайдера не дошёл").isEqualTo(1);
        assertThat(paymentProvider.charges()).hasSize(1);
    }

    @Test
    void retryWhileFirstIsInProgressGetsConflict() {
        CompletableFuture<ResponseEntity<String>> first = payments.pay(CLIENT, KEY, PAYMENT).toFuture();
        await().atMost(Duration.ofSeconds(5)).until(() -> "IN_PROGRESS".equals(recordField("state")));

        ResponseEntity<String> concurrent = payments.pay(CLIENT, KEY, PAYMENT).block();
        assertThat(concurrent.getStatusCode().value())
                .as("первый запрос ещё у провайдера — повтор получает 409 и провайдера не вызывает")
                .isEqualTo(409);

        ResponseEntity<String> firstResponse = awaitResult(first);
        ResponseEntity<String> later = payments.pay(CLIENT, KEY, PAYMENT).block();
        assertThat(later.getBody()).isEqualTo(firstResponse.getBody());
        assertThat(later.getHeaders().getFirst(PaymentController.REPLAYED_HEADER)).isEqualTo("true");
        assertThat(paymentProvider.callCount()).isEqualTo(1);
    }

    @Test
    void sameKeyWithDifferentRequestIsRejected() {
        payments.pay(CLIENT, KEY, PAYMENT).block();

        ResponseEntity<String> otherAmount = payments.pay(CLIENT, KEY,
                new PaymentRequest("order-42", new BigDecimal("9960.00"))).block();

        assertThat(otherAmount.getStatusCode().value())
                .as("тот же ключ, другая сумма — ошибка клиента, а не повтор")
                .isEqualTo(422);
        assertThat(paymentProvider.charges()).hasSize(1);
    }

    @Test
    void declinedPaymentIsSavedAndReplayed() {
        PaymentRequest overLimit = new PaymentRequest("order-43", new BigDecimal("250000.00"));

        ResponseEntity<String> first = payments.pay(CLIENT, KEY, overLimit).block();
        ResponseEntity<String> retry = payments.pay(CLIENT, KEY, overLimit).block();

        assertThat(first.getStatusCode().value()).isEqualTo(402);
        assertThat(retry.getStatusCode().value()).isEqualTo(402);
        assertThat(retry.getHeaders().getFirst(PaymentController.REPLAYED_HEADER))
                .as("отказ провайдера — тоже результат обработки: сохранён и повторён без нового вызова")
                .isEqualTo("true");
        assertThat(paymentProvider.callCount()).isEqualTo(1);
    }

    @Test
    void crashAfterChargeWithoutKeyForProviderChargesTwice() {
        PaymentService withoutProviderKey = new PaymentService(paymentProvider, paymentFaults, false);
        paymentFaults.setFailAfterCharge(true);

        assertThatThrownBy(() -> payViaIdempotency(withoutProviderKey).block()).hasMessageContaining("реплика упала");
        assertThat(redis.hasKey(RECORD_KEY).block()).as("сбой — запись о ключе удалена").isFalse();

        paymentFaults.setFailAfterCharge(false);
        IdempotentOutcome retry = payViaIdempotency(withoutProviderKey).block();

        assertThat(retry.response().status()).isEqualTo(201);
        assertThat(paymentProvider.charges())
                .as("Redis о первом списании не знал, провайдер ключа не получил — деньги списаны второй раз")
                .hasSize(2);
    }

    @Test
    void crashAfterChargeWithKeyForProviderChargesOnce() {
        paymentFaults.setFailAfterCharge(true);

        assertThatThrownBy(() -> payViaIdempotency(paymentService).block()).hasMessageContaining("реплика упала");

        paymentFaults.setFailAfterCharge(false);
        IdempotentOutcome retry = payViaIdempotency(paymentService).block();

        assertThat(retry.response().status()).isEqualTo(201);
        assertThat(retry.response().body()).contains("\"chargeId\":\"ch_1\"");
        assertThat(paymentProvider.charges())
                .as("провайдер узнал свой ключ и вернул прежнее списание вместо нового")
                .hasSize(1);
        assertThat(paymentProvider.callCount()).isEqualTo(2);
    }

    @Test
    void keyHeldByCrashedReplicaFreesAfterInProgressTtl() {
        /* Реплика взяла ключ и упала до вызова провайдера: запись «в работе» осталась, снять её некому. */
        redis.opsForHash().putAll(RECORD_KEY, Map.of(
                "state", "IN_PROGRESS",
                "token", "упавшая реплика",
                "fingerprint", PaymentController.fingerprint(PAYMENT))).block();
        redis.expire(RECORD_KEY, Duration.ofMillis(500)).block();

        assertThat(payments.pay(CLIENT, KEY, PAYMENT).block().getStatusCode().value()).isEqualTo(409);
        await().atMost(Duration.ofSeconds(5))
                .until(() -> payments.pay(CLIENT, KEY, PAYMENT).block().getStatusCode().value() == 201);

        assertThat(paymentProvider.charges()).hasSize(1);
    }

    @Test
    void sameKeyFromDifferentClientsChargesTwice() {
        payments.pay(CLIENT, KEY, PAYMENT).block();
        payments.pay("partner", KEY, PAYMENT).block();

        assertThat(paymentProvider.charges())
                .as("ключ уникален в пределах клиента: у другого клиента это другой платёж")
                .hasSize(2);
    }

    @Test
    void completedRecordKeepsResponseForDay() {
        payments.pay(CLIENT, KEY, PAYMENT).block();

        assertThat(recordField("state")).isEqualTo("COMPLETED");
        assertThat(recordField("status")).isEqualTo("201");
        assertThat(redis.getExpire(RECORD_KEY).block())
                .isGreaterThan(Duration.ofHours(23))
                .isLessThanOrEqualTo(Duration.ofHours(24));
    }

    /* Тот же путь, что у контроллера, но с выбранным сервисом оплаты. */
    private Mono<IdempotentOutcome> payViaIdempotency(PaymentService service) {
        return idempotencyGuard.execute(CLIENT, KEY, PaymentController.fingerprint(PAYMENT),
                () -> PaymentController.pay(service, CLIENT, KEY, PAYMENT, jsonMapper));
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
