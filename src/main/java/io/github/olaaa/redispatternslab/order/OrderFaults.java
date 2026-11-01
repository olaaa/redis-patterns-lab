package io.github.olaaa.redispatternslab.order;

import org.springframework.stereotype.Component;

/*
 * Сценарий 4. Сбой для опыта: реплика «падает» сразу после того, как заказ записан в базу,
 * но до того, как ответ записан в Redis.
 */
@Component
public class OrderFaults {

    private volatile boolean failAfterSave;

    public boolean failAfterSave() {
        return failAfterSave;
    }

    public void setFailAfterSave(boolean failAfterSave) {
        this.failAfterSave = failAfterSave;
    }

    public void reset() {
        failAfterSave = false;
    }
}
