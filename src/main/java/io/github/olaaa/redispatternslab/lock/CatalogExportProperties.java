package io.github.olaaa.redispatternslab.lock;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/*
 * Выгрузка каталога партнёру: срок аренды блокировки, сколько длится выгрузка и как часто продлевать
 * аренду во время выгрузки (не задано — не продлевать).
 */
@ConfigurationProperties("catalog-export")
public record CatalogExportProperties(Duration lease, Duration workDuration, Duration renewEvery) {
}
