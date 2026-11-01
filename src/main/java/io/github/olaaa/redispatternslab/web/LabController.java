package io.github.olaaa.redispatternslab.web;

import java.util.List;

import io.github.olaaa.redispatternslab.lock.CatalogExportJob;
import io.github.olaaa.redispatternslab.lock.PartnerInbox;
import io.github.olaaa.redispatternslab.order.OrderFaults;
import io.github.olaaa.redispatternslab.order.OrderRepository;
import io.github.olaaa.redispatternslab.order.OrderService;
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
    private final OrderRepository orderRepository;
    private final OrderService orderService;
    private final OrderFaults orderFaults;

    public LabController(ReactiveStringRedisTemplate redis, CatalogExportJob catalogExportJob,
                         PartnerInbox partnerInbox, @Value("${server.port:8080}") String serverPort,
                         OrderRepository orderRepository, OrderService orderService, OrderFaults orderFaults) {
        this.redis = redis;
        this.catalogExportJob = catalogExportJob;
        this.partnerInbox = partnerInbox;
        this.replica = "replica-" + serverPort;
        this.orderRepository = orderRepository;
        this.orderService = orderService;
        this.orderFaults = orderFaults;
    }

    public record OrderStats(long orders, int foundByIdempotencyKey) {
    }

    /* Сценарий 4: сколько заказов у клиента в базе и сколько раз база вернула уже существующий заказ. */
    @GetMapping("/orders")
    public Mono<OrderStats> orders(@RequestParam String client) {
        return orderRepository.countByClient(client)
                .map(count -> new OrderStats(count, orderService.foundByIdempotencyKey()));
    }

    /* Сценарий 4: «уронить» реплику после записи заказа в базу, до записи ответа в Redis. */
    @PostMapping("/orders/fail-after-save")
    public void failAfterSave(@RequestParam boolean enabled) {
        orderFaults.setFailAfterSave(enabled);
    }

    /* Сценарий 3: выгрузить каталог партнёру, если блокировку взяла эта реплика. */
    @PostMapping("/catalog-export")
    public Mono<ExportRun> catalogExport() {
        return catalogExportJob.runOnce(replica).map(exported -> new ExportRun(replica, exported));
    }

    /* Сценарий 3: что получил партнёр на этой реплике. */
    @GetMapping("/partner-inbox")
    public InboxState partnerInbox() {
        return new InboxState(partnerInbox.exportedBy(), partnerInbox.maxConcurrentExports());
    }

    /* Очищает текущую базу Redis целиком, приёмник выгрузок и заказы. */
    @PostMapping("/reset")
    public Mono<Void> reset() {
        partnerInbox.reset();
        orderService.reset();
        orderFaults.reset();
        return orderRepository.deleteAll()
                .then(redis.execute(connection -> connection.serverCommands().flushDb()).then());
    }
}
