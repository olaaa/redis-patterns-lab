package io.github.olaaa.redispatternslab;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class RedisPatternsLabApplication {

    public static void main(String[] args) {
        SpringApplication.run(RedisPatternsLabApplication.class, args);
    }
}
