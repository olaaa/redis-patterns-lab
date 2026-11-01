package io.github.olaaa.redispatternslab.web;

import java.util.List;

import io.github.olaaa.redispatternslab.lock.CatalogExportJob;
import io.github.olaaa.redispatternslab.lock.PartnerInbox;
import io.github.olaaa.redispatternslab.payment.Charge;
import io.github.olaaa.redispatternslab.payment.PaymentFaults;
import io.github.olaaa.redispatternslab.payment.PaymentProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/* Служебные ручки для ручных экспериментов. */
@RestController
@RequestMapping("/lab")
public class LabController {

    public record ExportRun(String replica, boolean exported) {
    }

    public record InboxState(List<String> exportedBy, int maxConcurrentExports) {
    }

    private final ReactiveStringRedisTemplate redis;
    private final CatalogExportJob catalogExportJob;
    private final PartnerInbox partnerInbox;
    private final String replica;
    private final PaymentProvider paymentProvider;
    private final PaymentFaults paymentFaults;

    public LabController(ReactiveStringRedisTemplate redis, CatalogExportJob catalogExportJob,
                         PartnerInbox partnerInbox, @Value("${server.port:8080}") String serverPort,
                         PaymentProvider paymentProvider, PaymentFaults paymentFaults) {
        this.redis = redis;
        this.catalogExportJob = catalogExportJob;
        this.partnerInbox = partnerInbox;
        this.replica = "replica-" + serverPort;
        this.paymentProvider = paymentProvider;
        this.paymentFaults = paymentFaults;
    }

    public record ProviderState(int calls, List<Charge> charges) {
    }

    /* Сценарий 4: сколько раз обращались к платёжному провайдеру и какие списания он сделал. */
    @GetMapping("/payments/provider")
    public ProviderState paymentProvider() {
        return new ProviderState(paymentProvider.callCount(), paymentProvider.charges());
    }

    /* Сценарий 4: «уронить» реплику после списания у провайдера, до записи ответа в Redis. */
    @PostMapping("/payments/fail-after-charge")
    public void failAfterCharge(@RequestParam boolean enabled) {
        paymentFaults.setFailAfterCharge(enabled);
    }

    /* Очищает текущую базу Redis целиком, приёмник выгрузок и списания у платёжного провайдера. */
    @PostMapping("/reset")
    public Mono<Void> reset() {
        partnerInbox.reset();
        paymentProvider.reset();
        paymentFaults.reset();
        return redis.execute(connection -> connection.serverCommands().flushDb()).then();
    }
}
