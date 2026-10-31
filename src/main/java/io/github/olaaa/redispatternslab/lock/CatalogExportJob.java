package io.github.olaaa.redispatternslab.lock;

import java.time.Duration;

import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/*
 * Сценарий 3. Выгрузка каталога партнёру — задача, которую запускает каждая реплика,
 * а выполнить должна одна. Блокировка catalog-export решает, кто выгружает.
 *
 * Блокировка снимается в любом исходе — успех, ошибка, отмена: Mono.usingWhen.
 * Если задан renewEvery, пока выгрузка идёт, аренда продлевается: долгая выгрузка
 * не теряет блокировку, пока процесс жив и успевает продлевать.
 */
@Component
public class CatalogExportJob {

    public static final String LOCK_NAME = "catalog-export";

    private final RedisLock lock;
    private final PartnerInbox inbox;
    private final CatalogExportProperties properties;

    public CatalogExportJob(RedisLock lock, PartnerInbox inbox, CatalogExportProperties properties) {
        this.lock = lock;
        this.inbox = inbox;
        this.properties = properties;
    }

    /*
     * true — блокировку взяла эта реплика и выгрузила; false — выгружает другая.
     * thenReturn(true) стоит внутри flatMap, на выгрузке: снаружи он превратил бы и пустой tryAcquire
     * (блокировку не дали) в true — then не различает «завершился пустым» и «завершился после работы».
     */
    public Mono<Boolean> runOnce(String replica) {
        return lock.tryAcquire(LOCK_NAME, properties.lease())
                .flatMap(handle -> Mono.usingWhen(
                                Mono.just(handle),
                                held -> exportRenewingLease(replica, held),
                                lock::release)
                        .thenReturn(true))
                .defaultIfEmpty(false);
    }

    /* Для сравнения в тесте: та же выгрузка без блокировки. */
    public Mono<Void> runWithoutLock(String replica) {
        return export(replica);
    }

    private Mono<Void> exportRenewingLease(String replica, LockHandle handle) {
        Duration renewEvery = properties.renewEvery();
        if (renewEvery == null) {
            return export(replica);
        }
        return Mono.defer(() -> {
            Disposable renewal = Flux.interval(renewEvery)
                    .concatMap(tick -> lock.extend(handle, properties.lease()))
                    .subscribe();
            return export(replica).doFinally(signal -> renewal.dispose());
        });
    }

    private Mono<Void> export(String replica) {
        return Mono.fromRunnable(inbox::exportStarted)
                .then(Mono.delay(properties.workDuration()))
                .then(Mono.fromRunnable(() -> inbox.exportFinished(replica)));
    }
}
