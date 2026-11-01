package io.github.olaaa.redispatternslab.payment;

import org.springframework.stereotype.Component;

/*
 * Сценарий 4. Сбой для опыта: реплика «падает» сразу после того, как провайдер списал деньги,
 * но до того, как ответ записан в Redis.
 */
@Component
public class PaymentFaults {

    private volatile boolean failAfterCharge;

    public boolean failAfterCharge() {
        return failAfterCharge;
    }

    public void setFailAfterCharge(boolean failAfterCharge) {
        this.failAfterCharge = failAfterCharge;
    }

    public void reset() {
        failAfterCharge = false;
    }
}
