package io.github.olaaa.redispatternslab.web;

import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/* Служебные ручки для ручных экспериментов. */
@RestController
@RequestMapping("/lab")
public class LabController {

    private final ReactiveStringRedisTemplate redis;

    public LabController(ReactiveStringRedisTemplate redis) {
        this.redis = redis;
    }

    /* Очищает текущую базу Redis целиком: все счётчики и ключи лаборатории живут в ней. */
    @PostMapping("/reset")
    public Mono<Void> reset() {
        return redis.execute(connection -> connection.serverCommands().flushDb()).then();
    }
}
