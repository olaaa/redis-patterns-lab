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
- `lock` — распределённая блокировка и выгрузка каталога партнёру под ней.
- `idempotency` — ключи идемпотентности в Redis; `order` — заказы, таблица `customer_order`.
- `web` — `GET /products/{id}`, `POST /orders` и служебные ручки `/lab`.
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

### 3. Распределённая блокировка с арендой

Тест: `Scenario3LeaseLockTest`. Компоненты: `RedisLock`, `CatalogExportJob`, `PartnerInbox`. Подробно: [docs/scenarios/03-lease-lock.md](docs/scenarios/03-lease-lock.md), вручную: `http/03-lease-lock.http` на двух репликах.

Выгрузку каталога партнёру запускает каждая реплика, выполняет одна. Блокировка — `SET lock:catalog-export <маркер> NX PX <срок>`; снять и продлить можно только свою — скриптом со сравнением маркера (в Redis 8.4+ ещё `DELEX ... IFEQ`); снимается при любом исходе выгрузки. Простой `DEL` после истечения аренды удаляет чужую блокировку. Аренда освобождает блокировку упавшего держателя, но может истечь и у живого: без продления долгая выгрузка теряет блокировку, и выгружают двое одновременно — это задача маркера ограждения.

### 4. Идемпотентность запросов

Тест: `Scenario4IdempotencyTest`. Компоненты: `IdempotentRequests`, `OrderController`, `OrderService`, `OrderRepository`. Подробно: [docs/scenarios/04-idempotency.md](docs/scenarios/04-idempotency.md), вручную: `http/04-idempotency.http`.

Повтор `POST /orders` без ключа создаёт второй заказ. С заголовком `Idempotency-Key` запрос выполняется один раз: в Redis атомарно создаётся запись «в работе» с отпечатком тела, после обработки в ней сохраняется ответ на сутки. Повтор получает сохранённый ответ с `Idempotent-Replayed: true`, пока первый в работе — 409, тот же ключ с другим телом — 422. Если реплика упала между записью заказа и записью ответа в Redis, повтор выполнится заново — второй заказ не даёт создать `UNIQUE (client_id, idempotency_key)` в таблице заказов и `INSERT … ON CONFLICT DO NOTHING`.
