package io.github.olaaa.redispatternslab.lock;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.stereotype.Component;

/*
 * Сценарий 3. Приёмник выгрузок каталога на стороне партнёра — в памяти, чтобы считать.
 * Помнит, кто выгружал, и сколько выгрузок шло одновременно: две одновременные выгрузки
 * у партнёра перемешают файлы каталога.
 */
@Component
public class PartnerInbox {

    private final List<String> exportedBy = new CopyOnWriteArrayList<>();
    private final AtomicInteger activeExports = new AtomicInteger();
    private final AtomicInteger maxConcurrentExports = new AtomicInteger();
    private volatile boolean failing;

    void exportStarted() {
        if (failing) {
            throw new IllegalStateException("партнёр недоступен");
        }
        int active = activeExports.incrementAndGet();
        maxConcurrentExports.accumulateAndGet(active, Math::max);
    }

    void exportFinished(String replica) {
        activeExports.decrementAndGet();
        exportedBy.add(replica);
    }

    public List<String> exportedBy() {
        return List.copyOf(exportedBy);
    }

    public int maxConcurrentExports() {
        return maxConcurrentExports.get();
    }

    public void setFailing(boolean failing) {
        this.failing = failing;
    }

    public void reset() {
        exportedBy.clear();
        activeExports.set(0);
        maxConcurrentExports.set(0);
        failing = false;
    }
}
