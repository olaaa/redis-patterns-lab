package io.github.olaaa.redispatternslab.scenario;

import java.time.Instant;

import io.github.olaaa.redispatternslab.MutableClock;
import io.github.olaaa.redispatternslab.MutableClockConfiguration;
import io.github.olaaa.redispatternslab.TestcontainersConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

/*
 * Общая основа сценариев: один контекст Spring, один контейнер PostgreSQL и один Redis на все тесты.
 * Перед каждым тестом — чистая база Redis и часы ровно на начале секунды.
 */
@SpringBootTest
@Import({TestcontainersConfiguration.class, MutableClockConfiguration.class})
abstract class ScenarioTestSupport {

    /* Начало окна: ровно на границе секунды. */
    static final Instant WINDOW_START = Instant.parse("2026-11-01T10:00:00Z");

    /* Прямой доступ к Redis: посмотреть ключи и срок жизни. */
    @Autowired
    ReactiveStringRedisTemplate redis;

    @Autowired
    MutableClock clock;

    @BeforeEach
    void resetLab() {
        redis.execute(connection -> connection.serverCommands().flushDb()).blockLast();
        clock.set(WINDOW_START);
    }
}
