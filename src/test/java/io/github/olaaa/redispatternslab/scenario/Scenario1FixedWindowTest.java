package io.github.olaaa.redispatternslab.scenario;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;

import io.github.olaaa.redispatternslab.ratelimit.LocalFixedWindowRateLimiter;
import io.github.olaaa.redispatternslab.ratelimit.RateLimitDecision;
import io.github.olaaa.redispatternslab.ratelimit.RateLimitProperties;
import io.github.olaaa.redispatternslab.ratelimit.RateLimitWebFilter;
import io.github.olaaa.redispatternslab.ratelimit.RateLimiter;
import io.github.olaaa.redispatternslab.ratelimit.RedisFixedWindowRateLimiter;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;

/* Сценарий 1. Ограничение частоты запросов: фиксированное окно в памяти реплики и в Redis. */
class Scenario1FixedWindowTest extends ScenarioTestSupport {

    private static final String CLIENT = "mobile-app";
    private static final int REPLICAS = 3;

    @Autowired
    RedisFixedWindowRateLimiter rateLimiter;

    @Autowired
    RateLimitProperties properties;

    @Autowired
    ApplicationContext applicationContext;

    @Test
    void localLimiterOnEachReplicaLetsThroughLimitTimesReplicas() {
        List<RateLimiter> replicas = List.of(
                new LocalFixedWindowRateLimiter(clock, properties),
                new LocalFixedWindowRateLimiter(clock, properties),
                new LocalFixedWindowRateLimiter(clock, properties));

        long allowed = sendRoundRobin(replicas, REPLICAS * properties.limit());

        assertThat(allowed)
                .as("у каждой реплики свой счётчик: лимит 10 на три реплики — 30 запросов в окно")
                .isEqualTo((long) REPLICAS * properties.limit());
    }

    @Test
    void redisLimiterSharesCounterAcrossReplicas() {
        List<RateLimiter> replicas = List.of(rateLimiter,
                new RedisFixedWindowRateLimiter(redis, clock, properties),
                new RedisFixedWindowRateLimiter(redis, clock, properties));

        long allowed = sendRoundRobin(replicas, REPLICAS * properties.limit());

        assertThat(allowed)
                .as("счётчик в Redis общий: из 30 запросов через три реплики пропущено 10")
                .isEqualTo(properties.limit());
    }

    @Test
    void concurrentRequestsNeverExceedLimit() {
        List<RateLimitDecision> decisions = Flux.range(0, 100)
                .flatMap(requestNumber -> rateLimiter.tryAcquire(CLIENT), 100)
                .collectList()
                .block();

        assertThat(decisions.stream().filter(RateLimitDecision::allowed).count())
                .as("INCR атомарен: из 100 одновременных запросов пропущено ровно 10")
                .isEqualTo(properties.limit());
    }

    @Test
    void counterStartsOverInNextWindow() {
        sendSequentially(properties.limit());

        RateLimitDecision rejected = rateLimiter.tryAcquire(CLIENT).block();
        assertThat(rejected.allowed()).isFalse();
        assertThat(rejected.retryAfter())
                .as("часы стоят на начале окна: до следующего — целая секунда")
                .isEqualTo(Duration.ofSeconds(1));
        assertThat(rateLimiter.tryAcquire("another-client").block().allowed())
                .as("у другого клиента свой счётчик")
                .isTrue();

        clock.advance(Duration.ofSeconds(1));

        RateLimitDecision nextWindow = rateLimiter.tryAcquire(CLIENT).block();
        assertThat(nextWindow.allowed()).isTrue();
        assertThat(nextWindow.remaining()).isEqualTo(properties.limit() - 1);
    }

    @Test
    void windowBoundaryLetsThroughTwiceTheLimit() {
        /* Конец окна: до границы 100 мс. */
        clock.set(WINDOW_START.plusMillis(900));
        long allowedBeforeBoundary = sendSequentially(properties.limit());

        /* Граница: началось новое окно, счётчик новый. */
        clock.advance(Duration.ofMillis(100));
        long allowedAfterBoundary = sendSequentially(properties.limit());

        assertThat(allowedBeforeBoundary + allowedAfterBoundary)
                .as("за 100 мс вокруг границы окон пропущено 20 запросов — вдвое больше лимита")
                .isEqualTo(2L * properties.limit());
        assertThat(rateLimiter.tryAcquire(CLIENT).block().allowed()).isFalse();
    }

    @Test
    void counterKeyExpiresWithItsWindow() {
        rateLimiter.tryAcquire(CLIENT).block();

        String key = RedisFixedWindowRateLimiter.KEY_PREFIX + CLIENT + ":" + WINDOW_START.toEpochMilli();
        Duration timeToLive = redis.getExpire(key).block();

        assertThat(timeToLive)
                .as("срок жизни встал вместе с первым INCR и не длиннее окна: старые счётчики Redis удаляет сам")
                .isPositive()
                .isLessThanOrEqualTo(properties.window());
    }

    @Test
    void filterAnswersTooManyRequestsWithRetryAfter() {
        WebTestClient client = WebTestClient.bindToApplicationContext(applicationContext).build();

        for (int request = 1; request <= properties.limit(); request++) {
            client.get().uri("/products/1").header(RateLimitWebFilter.CLIENT_ID_HEADER, CLIENT)
                    .exchange()
                    .expectStatus().isOk()
                    .expectHeader().valueEquals(RateLimitWebFilter.REMAINING_HEADER,
                            String.valueOf(properties.limit() - request));
        }

        client.get().uri("/products/1").header(RateLimitWebFilter.CLIENT_ID_HEADER, CLIENT)
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.TOO_MANY_REQUESTS)
                .expectHeader().valueEquals("Retry-After", "1");
        client.get().uri("/products/1").header(RateLimitWebFilter.CLIENT_ID_HEADER, "another-client")
                .exchange()
                .expectStatus().isOk();
    }

    /* Запросы одного клиента по очереди через реплики: первый — на A, второй — на B и так далее. */
    private static long sendRoundRobin(List<RateLimiter> replicas, int requests) {
        long allowed = 0;
        for (int request = 0; request < requests; request++) {
            if (replicas.get(request % replicas.size()).tryAcquire(CLIENT).block().allowed()) {
                allowed++;
            }
        }
        return allowed;
    }

    private long sendSequentially(int requests) {
        long allowed = 0;
        for (int request = 0; request < requests; request++) {
            if (rateLimiter.tryAcquire(CLIENT).block().allowed()) {
                allowed++;
            }
        }
        return allowed;
    }
}
