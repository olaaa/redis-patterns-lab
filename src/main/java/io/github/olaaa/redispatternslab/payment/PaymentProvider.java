package io.github.olaaa.redispatternslab.payment;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/*
 * Сценарий 4. Внешний платёжный провайдер — имитация банка-эквайера. Это не наша система:
 * её база нам недоступна, ключ идемпотентности в неё не положить. Отвечает с задержкой, помнит
 * все списания и умеет отказывать: сумма больше LIMIT — отказ.
 *
 * Как настоящие провайдеры (Stripe), принимает свой ключ идемпотентности: списание с уже виденным
 * ключом не повторяется, возвращается прежнее. Без ключа каждый вызов — новое списание.
 */
@Component
public class PaymentProvider {

    public static final BigDecimal LIMIT = new BigDecimal("100000.00");

    private final Clock clock;
    private final Duration delay;
    private final List<Charge> charges = new CopyOnWriteArrayList<>();
    private final Map<String, Charge> chargesByKey = new ConcurrentHashMap<>();
    private final AtomicInteger callCount = new AtomicInteger();

    public PaymentProvider(Clock clock, @Value("${payments.provider-delay}") Duration delay) {
        this.clock = clock;
        this.delay = delay;
    }

    /* idempotencyKey — ключ для провайдера; null — провайдер ключа не получил. */
    public Mono<Charge> charge(String idempotencyKey, PaymentRequest request) {
        return Mono.defer(() -> {
            callCount.incrementAndGet();
            return Mono.delay(delay).then(Mono.fromCallable(() -> doCharge(idempotencyKey, request)));
        });
    }

    private synchronized Charge doCharge(String idempotencyKey, PaymentRequest request) {
        if (idempotencyKey != null && chargesByKey.containsKey(idempotencyKey)) {
            return chargesByKey.get(idempotencyKey);
        }
        if (request.amount().compareTo(LIMIT) > 0) {
            throw new PaymentDeclinedException("карта отклонена: сумма больше лимита " + LIMIT);
        }
        Charge charge = new Charge("ch_" + (charges.size() + 1), request.orderId(), request.amount(), clock.instant());
        charges.add(charge);
        if (idempotencyKey != null) {
            chargesByKey.put(idempotencyKey, charge);
        }
        return charge;
    }

    /* Все списания — сколько раз у клиента действительно сняли деньги. */
    public List<Charge> charges() {
        return List.copyOf(charges);
    }

    public int callCount() {
        return callCount.get();
    }

    public void reset() {
        charges.clear();
        chargesByKey.clear();
        callCount.set(0);
    }
}
