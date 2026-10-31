# redis-patterns-lab

Каталог товаров на WebFlux, R2DBC и Spring Data Redis: Redis не как кэш — ограничение частоты запросов, распределённая блокировка, идемпотентность запросов. Каждый приём — отдельный компонент и тест.

Связанные репозитории:

- [caching-lab](https://github.com/olaaa/caching-lab) — локальный кэш на Caffeine;
- [distributed-cache-lab](https://github.com/olaaa/distributed-cache-lab) — Redis как кэш.

## Как запустить

Нужны Java 25 и Docker.

```bash
mvn test
```

Тесты поднимают PostgreSQL и Redis в контейнерах Testcontainers, по одному на все сценарии.

```bash
mvn spring-boot:run
```

Spring Boot сам поднимет PostgreSQL и Redis из `compose.yaml`. Запросы для IntelliJ HTTP Client — в папке `http`, по файлу на сценарий; очистка Redis — `POST /lab/reset`. Посмотреть содержимое Redis: `docker compose exec redis redis-cli`.

## Устройство

- `catalog` — таблица `product` и `ProductRepository` на `DatabaseClient`.
- `ratelimit` — ограничение частоты запросов.
- `web` — `GET /products/{id}` и служебные ручки `/lab`.
- `config/ClockConfiguration` — часы приложения; в тестах их подменяет `MutableClock`.

Клиент Redis — Lettuce, его приносит `spring-boot-starter-data-redis-reactive`.

## Сценарии

Подробное описание каждого — [docs/scenarios](docs/scenarios/README.md). Ниже — коротко.

### 1. Ограничение частоты запросов: фиксированное окно

Тест: `Scenario1FixedWindowTest`. Компоненты: `RedisFixedWindowRateLimiter`, `LocalFixedWindowRateLimiter`, `RateLimitWebFilter`. Подробно: [docs/scenarios/01-fixed-window.md](docs/scenarios/01-fixed-window.md), вручную: `http/01-fixed-window.http`.

Не больше 10 запросов к каталогу от одного клиента в секунду, на всех репликах вместе. Счётчик в памяти реплики умножает лимит на число реплик: три реплики пропускают 30. Счётчик в Redis общий: ключ «клиент плюс окно», `INCR` и срок жизни одним Lua-скриптом; сверх лимита — 429 и `Retry-After`. Слабое место — граница окон: за 100 мс вокруг неё проходит 20 запросов.

### 2. Ограничение частоты без всплеска на границе окон: скользящее окно и ведро токенов

Тест: `Scenario2SlidingLogAndTokenBucketTest`. Компоненты: `RedisSlidingLogRateLimiter`, `RedisTokenBucketRateLimiter`; в фильтре — по `rate-limit.algorithm`. Подробно: [docs/scenarios/02-sliding-log-and-token-bucket.md](docs/scenarios/02-sliding-log-and-token-bucket.md), вручную: `http/02-sliding-log-and-token-bucket.http`.

Скользящее окно хранит в ZSET время каждого пропущенного запроса и в любую секунду пропускает не больше 10 — но держит до 10 записей на клиента. Ведро токенов хранит два числа на клиента, пересчитывает пополнение при каждом запросе и разрешает пачку до ёмкости ведра. Опыт у границы окон, 100 мс: фиксированное окно — 20 запросов, скользящее — 10, ведро — 11.
