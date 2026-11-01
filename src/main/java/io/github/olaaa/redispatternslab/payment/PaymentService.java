package io.github.olaaa.redispatternslab.payment;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/*
 * Сценарий 4. Сервис оплаты: своей базы у него нет, вся его работа — списать деньги у внешнего
 * провайдера. Повторы с тем же ключом отсекает IdempotencyGuard по записям в Redis, общим для всех
 * реплик, — до провайдера они не доходят.
 *
 * passKeyToProvider: передавать ли провайдеру ключ идемпотентности. Redis защищает от повторов,
 * но не от сбоя между списанием и записью ответа в Redis — повтор тогда спишет второй раз.
 * От этого защищает только ключ, переданный самому провайдеру.
 */
@Service
public class PaymentService {

    private final PaymentProvider provider;
    private final PaymentFaults faults;
    private final boolean passKeyToProvider;

    @Autowired
    public PaymentService(PaymentProvider provider, PaymentFaults faults,
                          @Value("${payments.pass-key-to-provider}") boolean passKeyToProvider) {
        this.provider = provider;
        this.faults = faults;
        this.passKeyToProvider = passKeyToProvider;
    }

    /* idempotencyKey — ключ клиента; null — без ключа, каждый вызов — новое списание. */
    public Mono<Charge> pay(String clientId, String idempotencyKey, PaymentRequest request) {
        String providerKey = passKeyToProvider && idempotencyKey != null ? clientId + ":" + idempotencyKey : null;
        return provider.charge(providerKey, request)
                .flatMap(charge -> faults.failAfterCharge()
                        ? Mono.error(new IllegalStateException(
                                "реплика упала: деньги списаны (" + charge.chargeId() + "), ответ не сохранён"))
                        : Mono.just(charge));
    }
}
