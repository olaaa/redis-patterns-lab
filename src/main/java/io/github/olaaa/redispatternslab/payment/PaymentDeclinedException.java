package io.github.olaaa.redispatternslab.payment;

/* Провайдер отказал в списании: например, сумма больше лимита карты. */
public class PaymentDeclinedException extends RuntimeException {

    public PaymentDeclinedException(String message) {
        super(message);
    }
}
