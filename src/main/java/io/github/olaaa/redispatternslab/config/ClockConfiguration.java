package io.github.olaaa.redispatternslab.config;

import java.time.Clock;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/* Часы приложения. В тестах их подменяют часы, которые двигает сам тест, — без sleep. */
@Configuration(proxyBeanMethods = false)
public class ClockConfiguration {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
