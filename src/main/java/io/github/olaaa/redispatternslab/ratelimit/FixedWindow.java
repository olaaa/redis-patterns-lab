package io.github.olaaa.redispatternslab.ratelimit;

import java.time.Clock;
import java.time.Duration;

/*
 * Фиксированное окно: время режется на отрезки длиной window, начиная с эпохи Unix.
 * Запрос в момент now попадает в окно, которое началось в start и закончится в start + window.
 */
record FixedWindow(long startMillis, Duration untilEnd) {

    static FixedWindow containing(Clock clock, Duration window) {
        long now = clock.millis();
        long windowMillis = window.toMillis();
        long start = now - Math.floorMod(now, windowMillis);
        return new FixedWindow(start, Duration.ofMillis(start + windowMillis - now));
    }
}
