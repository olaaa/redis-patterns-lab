package io.github.olaaa.redispatternslab.lock;

/* Взятая блокировка: её имя и маркер — случайная строка, которой «подписана» именно эта аренда. */
public record LockHandle(String name, String token) {
}
