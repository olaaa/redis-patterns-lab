package io.github.olaaa.redispatternslab.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.github.olaaa.redispatternslab.lock.CatalogExportJob;
import io.github.olaaa.redispatternslab.lock.CatalogExportProperties;
import io.github.olaaa.redispatternslab.lock.LockHandle;
import io.github.olaaa.redispatternslab.lock.RedisLock;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/* Сценарий 3. Распределённая блокировка с арендой: выгрузка каталога партнёру с нескольких реплик. */
class Scenario3LeaseLockTest extends ScenarioTestSupport {

    private static final String LOCK_KEY = RedisLock.KEY_PREFIX + CatalogExportJob.LOCK_NAME;
    private static final Duration SHORT_EXPORT = Duration.ofMillis(300);
    private static final Duration LONG_EXPORT = Duration.ofSeconds(1);
    private static final Duration SHORT_LEASE = Duration.ofMillis(300);

    @Autowired
    RedisLock lock;

    @Test
    void withoutLockEveryReplicaExports() {
        CatalogExportJob job = job(Duration.ofSeconds(5), SHORT_EXPORT, null);

        Flux.just("A", "B", "C").flatMap(job::runWithoutLock).blockLast();

        assertThat(partnerInbox.exportedBy()).as("каждая реплика выгрузила каталог").hasSize(3);
        assertThat(partnerInbox.maxConcurrentExports()).as("и все три — одновременно").isEqualTo(3);
    }

    @Test
    void onlyOneReplicaExportsWhileLockIsHeld() {
        CatalogExportJob job = job(Duration.ofSeconds(5), SHORT_EXPORT, null);

        List<Boolean> results = Flux.just("A", "B", "C").flatMap(job::runOnce).collectList().block();

        assertThat(results).containsOnlyOnce(true).hasSize(3);
        assertThat(partnerInbox.exportedBy()).hasSize(1);
        assertThat(partnerInbox.maxConcurrentExports()).isEqualTo(1);
        assertThat(hasKey(LOCK_KEY)).as("после выгрузки блокировка снята").isFalse();
    }

    @Test
    void lockIsReleasedWhenExportFails() {
        CatalogExportJob job = job(Duration.ofSeconds(5), SHORT_EXPORT, null);
        partnerInbox.setFailing(true);

        assertThatThrownBy(() -> job.runOnce("A").block()).hasMessageContaining("партнёр недоступен");
        assertThat(hasKey(LOCK_KEY)).as("ошибка не оставила блокировку висеть до конца аренды").isFalse();

        partnerInbox.setFailing(false);
        assertThat(job.runOnce("B").block()).isTrue();
    }

    @Test
    void leaseFreesLockWhenHolderDies() {
        /* Реплика взяла блокировку и упала — снять её некому. */
        lock.tryAcquire(CatalogExportJob.LOCK_NAME, SHORT_LEASE).block();
        long startNanos = System.nanoTime();

        assertThat(lock.tryAcquire(CatalogExportJob.LOCK_NAME, SHORT_LEASE).blockOptional())
                .as("пока аренда не истекла, взять нельзя")
                .isEmpty();
        await().atMost(Duration.ofSeconds(5))
                .until(() -> lock.tryAcquire(CatalogExportJob.LOCK_NAME, SHORT_LEASE).blockOptional().isPresent());

        assertThat(Duration.ofNanos(System.nanoTime() - startNanos))
                .as("блокировка освободилась сама, когда истекла аренда")
                .isGreaterThanOrEqualTo(Duration.ofMillis(250));
    }

    @Test
    void releaseRemovesOnlyOwnLock() {
        LockHandle replicaA = lock.tryAcquire(CatalogExportJob.LOCK_NAME, Duration.ofMillis(200)).block();
        await().atMost(Duration.ofSeconds(5)).until(() -> !hasKey(LOCK_KEY));
        LockHandle replicaB = lock.tryAcquire(CatalogExportJob.LOCK_NAME, Duration.ofSeconds(5)).block();

        assertThat(lock.release(replicaA).block())
                .as("аренда A истекла, блокировку держит B: скрипт видит чужой маркер и не удаляет")
                .isFalse();
        assertThat(redis.opsForValue().get(LOCK_KEY).block()).isEqualTo(replicaB.token());

        assertThat(lock.releaseWithoutTokenCheck(replicaA).block())
                .as("DEL без проверки маркера удаляет блокировку B")
                .isTrue();
        assertThat(lock.tryAcquire(CatalogExportJob.LOCK_NAME, Duration.ofSeconds(5)).blockOptional())
                .as("и её берёт C, хотя B ещё работает: блокировку держат двое")
                .isPresent();
    }

    @Test
    void renewalKeepsLockDuringLongExport() {
        CatalogExportJob replicaA = job(SHORT_LEASE, LONG_EXPORT, Duration.ofMillis(100));
        CatalogExportJob replicaB = job(SHORT_LEASE, LONG_EXPORT, Duration.ofMillis(100));

        CompletableFuture<Boolean> exportA = replicaA.runOnce("A").toFuture();
        /* Через 600 мс — вдвое дольше аренды — пробует B. */
        Boolean exportedByB = Mono.delay(Duration.ofMillis(600)).then(replicaB.runOnce("B")).block();

        assertThat(exportedByB).as("A продлевает аренду каждые 100 мс — B блокировку не получила").isFalse();
        assertThat(awaitResult(exportA)).isTrue();
        assertThat(partnerInbox.exportedBy()).containsExactly("A");
        assertThat(partnerInbox.maxConcurrentExports()).isEqualTo(1);
    }

    @Test
    void leaseExpiringDuringLongExportLetsSecondReplicaIn() {
        CatalogExportJob replicaA = job(SHORT_LEASE, LONG_EXPORT, null);
        CatalogExportJob replicaB = job(SHORT_LEASE, LONG_EXPORT, Duration.ofMillis(100));

        CompletableFuture<Boolean> exportA = replicaA.runOnce("A").toFuture();
        CompletableFuture<Boolean> exportB = Mono.delay(Duration.ofMillis(500)).then(replicaB.runOnce("B")).toFuture();

        assertThat(awaitResult(exportA)).isTrue();
        assertThat(hasKey(LOCK_KEY))
                .as("A закончила и сняла «свою» блокировку — но маркер уже чужой, и блокировка B на месте")
                .isTrue();
        assertThat(awaitResult(exportB))
                .as("аренда A истекла посреди выгрузки, и B взяла блокировку")
                .isTrue();
        assertThat(partnerInbox.maxConcurrentExports())
                .as("A ещё выгружала, когда начала B: блокировка с арендой не защитила")
                .isEqualTo(2);
    }

    private CatalogExportJob job(Duration lease, Duration workDuration, Duration renewEvery) {
        return new CatalogExportJob(lock, partnerInbox, new CatalogExportProperties(lease, workDuration, renewEvery));
    }

    private static Boolean awaitResult(CompletableFuture<Boolean> export) {
        try {
            return export.get(10, TimeUnit.SECONDS);
        } catch (Exception exception) {
            throw new IllegalStateException("выгрузка не завершилась", exception);
        }
    }

    private boolean hasKey(String key) {
        return Boolean.TRUE.equals(redis.hasKey(key).block());
    }
}
