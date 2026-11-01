package io.github.olaaa.redispatternslab.order;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.olaaa.redispatternslab.catalog.ProductRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/*
 * Сценарий 4. Создание заказа: найти товар, «проверить и зарезервировать» (задержка processing-delay),
 * записать заказ в базу.
 *
 * Если заказ с этим ключом идемпотентности уже есть в базе, второй не вставится, и вернётся существующий.
 * Это вторая линия защиты: первая — запись о ключе в Redis (IdempotentRequests). Сюда повтор доходит,
 * только если запись в Redis потеряна — например, реплика упала между базой и Redis.
 */
@Service
public class OrderService {

    private final ProductRepository products;
    private final OrderRepository orders;
    private final OrderFaults faults;
    private final Clock clock;
    private final Duration processingDelay;
    private final AtomicInteger foundByIdempotencyKey = new AtomicInteger();

    public OrderService(ProductRepository products, OrderRepository orders, OrderFaults faults, Clock clock,
                        @Value("${orders.processing-delay}") Duration processingDelay) {
        this.products = products;
        this.orders = orders;
        this.faults = faults;
        this.clock = clock;
        this.processingDelay = processingDelay;
    }

    /* Пустой Mono — товара нет. */
    public Mono<Order> create(String clientId, String idempotencyKey, OrderRequest request) {
        return products.findById(request.productId())
                .flatMap(product -> Mono.delay(processingDelay)
                        .then(save(clientId, idempotencyKey, request,
                                product.price().multiply(BigDecimal.valueOf(request.quantity())))))
                .flatMap(order -> faults.failAfterSave()
                        ? Mono.error(new IllegalStateException("реплика упала: заказ " + order.id() + " записан, ответ — нет"))
                        : Mono.just(order));
    }

    private Mono<Order> save(String clientId, String idempotencyKey, OrderRequest request, BigDecimal total) {
        return orders.insert(clientId, idempotencyKey, request.productId(), request.quantity(), total, clock.instant())
                .switchIfEmpty(Mono.defer(() -> {
                    foundByIdempotencyKey.incrementAndGet();
                    return orders.findByIdempotencyKey(clientId, idempotencyKey);
                }));
    }

    /* Сколько раз заказ не вставился, потому что с этим ключом он уже был в базе. */
    public int foundByIdempotencyKey() {
        return foundByIdempotencyKey.get();
    }

    public void reset() {
        foundByIdempotencyKey.set(0);
    }
}
