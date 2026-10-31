package io.github.olaaa.redispatternslab.scenario;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;

import io.github.olaaa.redispatternslab.ratelimit.RateLimitDecision;
import io.github.olaaa.redispatternslab.ratelimit.RateLimitProperties;
import io.github.olaaa.redispatternslab.ratelimit.RateLimiter;
import io.github.olaaa.redispatternslab.ratelimit.RedisFixedWindowRateLimiter;
import io.github.olaaa.redispatternslab.ratelimit.RedisSlidingLogRateLimiter;
import io.github.olaaa.redispatternslab.ratelimit.RedisTokenBucketRateLimiter;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import reactor.core.publisher.Flux;

/* Сценарий 2. Ограничение частоты без всплеска на границе окон: скользящее окно по журналу и ведро токенов. */
class Scenario2SlidingLogAndTokenBucketTest extends ScenarioTestSupport {

    private static final String CLIENT = "mobile-app";

    @Autowired
    RedisFixedWindowRateLimiter fixedWindow;

    @Autowired
    RedisSlidingLogRateLimiter slidingLog;

    @Autowired
    RedisTokenBucketRateLimiter tokenBucket;

    @Autowired
    RateLimitProperties properties;

    @Test
    void aroundWindowBoundaryFixedWindowPassesTwentySlidingLogTenTokenBucketEleven() {
        assertThat(burstAroundBoundary(fixedWindow)).as("фиксированное окно").isEqualTo(20);
        assertThat(burstAroundBoundary(slidingLog)).as("скользящее окно").isEqualTo(10);
        assertThat(burstAroundBoundary(tokenBucket)).as("ведро токенов: 10 из полного ведра и 1 набежавший").isEqualTo(11);
    }

    @Test
    void slidingLogLetsRequestsInWhenOldestLeaveWindow() {
        clock.set(WINDOW_START.plusMillis(900));
        assertThat(send(slidingLog, properties.limit())).isEqualTo(properties.limit());

        clock.set(WINDOW_START.plusMillis(1000));
        RateLimitDecision rejected = slidingLog.tryAcquire(CLIENT).block();
        assertThat(rejected.allowed()).isFalse();
        assertThat(rejected.retryAfter())
                .as("самый старый запрос в 0,900 покинет окно в 1,900 — через 900 мс")
                .isEqualTo(Duration.ofMillis(900));

        clock.set(WINDOW_START.plusMillis(1900));
        assertThat(send(slidingLog, properties.limit()))
                .as("через секунду после пачки окно освободилось целиком")
                .isEqualTo(properties.limit());
    }

    @Test
    void slidingLogKeepsOnlyAllowedRequestsAndExpires() {
        send(slidingLog, 15);

        String key = RedisSlidingLogRateLimiter.KEY_PREFIX + CLIENT;
        assertThat(redis.opsForZSet().size(key).block())
                .as("в журнале только пропущенные запросы: память — не больше лимита на клиента")
                .isEqualTo(properties.limit());
        assertThat(redis.getExpire(key).block()).isPositive().isLessThanOrEqualTo(properties.window());
    }

    @Test
    void tokenBucketAllowsBurstUpToCapacityThenSteadyRate() {
        assertThat(send(tokenBucket, properties.limit())).as("полное ведро — пачка из 10").isEqualTo(properties.limit());

        RateLimitDecision rejected = tokenBucket.tryAcquire(CLIENT).block();
        assertThat(rejected.allowed()).isFalse();
        assertThat(rejected.retryAfter())
                .as("10 токенов в секунду — новый токен через 100 мс")
                .isEqualTo(Duration.ofMillis(100));

        clock.advance(Duration.ofMillis(100));
        assertThat(send(tokenBucket, 2)).as("за 100 мс набежал один токен").isEqualTo(1);

        clock.advance(Duration.ofSeconds(5));
        assertThat(send(tokenBucket, 15))
                .as("за 5 секунд набежало бы 50 токенов, но в ведро помещается только 10")
                .isEqualTo(properties.limit());
    }

    @Test
    void tokenBucketKeyIsHashAndExpiresWhenFull() {
        tokenBucket.tryAcquire(CLIENT).block();

        String key = RedisTokenBucketRateLimiter.KEY_PREFIX + CLIENT;
        assertThat(redis.opsForHash().get(key, "tokens").block()).isEqualTo("9");
        assertThat(redis.opsForHash().get(key, "updated_at").block())
                .isEqualTo(String.valueOf(WINDOW_START.toEpochMilli()));
        assertThat(redis.getExpire(key).block())
                .as("через секунду ведро полное: хранить его дальше незачем")
                .isPositive()
                .isLessThanOrEqualTo(properties.window());
    }

    @Test
    void concurrentRequestsNeverExceedLimit() {
        for (RateLimiter rateLimiter : List.of(slidingLog, tokenBucket)) {
            List<RateLimitDecision> decisions = Flux.range(0, 100)
                    .flatMap(requestNumber -> rateLimiter.tryAcquire(rateLimiter.algorithm().name()), 100)
                    .collectList()
                    .block();

            assertThat(decisions.stream().filter(RateLimitDecision::allowed).count())
                    .as("%s: скрипт выполняется атомарно, из 100 одновременных пропущено 10", rateLimiter.algorithm())
                    .isEqualTo(properties.limit());
        }
    }

    /* Пачка в 0,900 и пачка в 1,000: сколько пропущено за эти 100 мс. */
    private long burstAroundBoundary(RateLimiter rateLimiter) {
        redis.execute(connection -> connection.serverCommands().flushDb()).blockLast();
        clock.set(WINDOW_START.plusMillis(900));
        long beforeBoundary = send(rateLimiter, properties.limit());
        clock.set(WINDOW_START.plusMillis(1000));
        long afterBoundary = send(rateLimiter, properties.limit());
        return beforeBoundary + afterBoundary;
    }

    private static long send(RateLimiter rateLimiter, int requests) {
        long allowed = 0;
        for (int request = 0; request < requests; request++) {
            if (rateLimiter.tryAcquire(CLIENT).block().allowed()) {
                allowed++;
            }
        }
        return allowed;
    }
}
