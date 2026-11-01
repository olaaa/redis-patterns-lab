package io.github.olaaa.redispatternslab.scenario;

import java.time.Instant;

import io.github.olaaa.redispatternslab.MutableClock;
import io.github.olaaa.redispatternslab.MutableClockConfiguration;
import io.github.olaaa.redispatternslab.TestcontainersConfiguration;
import io.github.olaaa.redispatternslab.lock.PartnerInbox;
import io.github.olaaa.redispatternslab.order.OrderFaults;
import io.github.olaaa.redispatternslab.order.OrderRepository;
import io.github.olaaa.redispatternslab.order.OrderService;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

/*
 * Общая основа сценариев: один контекст Spring, один контейнер PostgreSQL и один Redis на все тесты.
 * Перед каждым тестом — чистая база Redis, часы ровно на начале секунды и пустой приёмник выгрузок.
 */
@SpringBootTest(properties = "orders.processing-delay=300ms")
@Import({TestcontainersConfiguration.class, MutableClockConfiguration.class})
abstract class ScenarioTestSupport {

    /* Начало окна: ровно на границе секунды. */
    static final Instant WINDOW_START = Instant.parse("2026-11-01T10:00:00Z");

    /* Прямой доступ к Redis: посмотреть ключи и срок жизни. */
    @Autowired
    ReactiveStringRedisTemplate redis;

    @Autowired
    MutableClock clock;

    @Autowired
    PartnerInbox partnerInbox;

    @Autowired
    OrderRepository orderRepository;

    @Autowired
    OrderService orderService;

    @Autowired
    OrderFaults orderFaults;

    @BeforeEach
    void resetLab() {
        redis.execute(connection -> connection.serverCommands().flushDb()).blockLast();
        clock.set(WINDOW_START);
        partnerInbox.reset();
        orderRepository.deleteAll().block();
        orderService.reset();
        orderFaults.reset();
    }
}
