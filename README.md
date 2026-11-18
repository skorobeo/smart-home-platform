<div align="center">

# Smart Home Platform

**Телеметрия умного дома: Collector → Aggregator → Analyzer**

</div>

---

## О проекте

Платформа для умного дома, состоящая из трёх сервисов:

- **Collector** принимает события от хабов по HTTP (JSON) и gRPC (Protobuf), кодирует их в Avro и отправляет в Kafka.
- **Aggregator** читает события датчиков и поддерживает актуальные снапшоты состояния каждого хаба.
- **Analyzer** проверяет снапшоты по пользовательским сценариям и отправляет команды устройствам через Hub Router.

## Архитектура

```
Хабы / hub-router
        │  HTTP JSON (8080) или gRPC (59091)
        ▼
   ┌───────────┐   telemetry.sensors.v1   ┌────────────┐   telemetry.snapshots.v1   ┌──────────┐
   │ Collector │ ───────────────────────▶ │ Aggregator │ ─────────────────────────▶ │ Analyzer │
   └───────────┘                          └────────────┘                            └────┬─────┘
        │                                                                                │ gRPC
        │ telemetry.hubs.v1                                                              ▼
        └──────────────────────────────────────────────────────────────────────────▶ Hub Router
                                                                                     (59090)
```

### Модули

```
telemetry/
├── collector/          — приём событий и отправка в Kafka
├── aggregator/         — снапшоты состояния хабов
├── analyzer/           — сценарии, PostgreSQL, gRPC-команды устройств
└── serialization/
    ├── avro-schemas/   — Avro-схемы событий и снапшотов, (де)сериализаторы
    └── proto-schemas/  — Protobuf-схемы Collector и Hub Router
commerce/               — каркас сервисов продаж (вне рамок задания)
infra/                  — каркас инфраструктурных компонентов
```

## Топики Kafka

| Топик | Назначение |
|---|---|
| `telemetry.sensors.v1` | события датчиков |
| `telemetry.hubs.v1` | события хабов: устройства и сценарии |
| `telemetry.snapshots.v1` | снапшоты состояния хабов, ключ — `hubId` |

## Collector

Поддерживает два способа приёма данных:

- HTTP/JSON:
  - `POST /events/sensors` — события датчиков;
  - `POST /events/hubs` — события хабов и сценариев.
- gRPC:
  - `CollectorController.CollectSensorEvent`;
  - `CollectorController.CollectHubEvent`.

Порт HTTP — `8080`, порт gRPC — `59091`.

Пример JSON-события датчика освещённости:

```json
{
  "id": "sensor.light.3",
  "hubId": "hub-2",
  "timestamp": "2024-08-06T15:11:24.157Z",
  "linkQuality": 75,
  "luminosity": 59,
  "type": "LIGHT_SENSOR_EVENT"
}
```

## Aggregator

- Читает `telemetry.sensors.v1` в группе консьюмеров `aggregator`.
- Хранит состояние датчиков в памяти по каждому `hubId`.
- Обновляет снапшот только при изменении данных датчика и при условии, что событие не старше уже сохранённого.
- Пишет новый снапшот в `telemetry.snapshots.v1` только при реальном обновлении.
- Использует ручную фиксацию смещений (`enable.auto.commit=false`).

Схема снапшота:

```
SensorsSnapshotAvro {
  hubId: string,
  timestamp: timestamp-millis,
  sensorsState: map<string, SensorStateAvro>
}

SensorStateAvro {
  timestamp: timestamp-millis,
  data: union {
    ClimateSensorAvro, LightSensorAvro, MotionSensorAvro,
    SwitchSensorAvro, TemperatureSensorAvro
  }
}
```

## Analyzer

- Читает снапшоты из `telemetry.snapshots.v1` в отдельной группе консьюмеров.
- Читает события устройств и сценариев из `telemetry.hubs.v1` в другой группе.
- Хранит хабы, датчики, сценарии, условия и действия в PostgreSQL.
- При каждом снапшоте загружает сценарии хаба и проверяет все условия.
- При выполнении условий отправляет `DeviceActionRequest` в Hub Router по gRPC на `localhost:59090`.
- Для снапшотов использует ручную фиксацию смещений и `max.poll.records=1`, чтобы минимизировать повторную обработку.
- Для событий хабов допускает повторную обработку: операции добавления и удаления идемпотентны.

Таблицы создаются при старте сервиса из `telemetry/analyzer/src/main/resources/schema.sql`:
`scenarios`, `sensors`, `conditions`, `actions`, `scenario_conditions`, `scenario_actions`.
Триггеры БД проверяют, что сценарий и датчик относятся к одному и тому же хабу.

## Запуск

Требования: Java 21, Maven 3.9+, Docker.

```bash
# Kafka + PostgreSQL + создание топиков
docker compose up -d
```

В отдельных терминалах:

```bash
# Collector: HTTP 8080, gRPC 59091
mvn -pl telemetry/collector -am spring-boot:run

# Aggregator
mvn -pl telemetry/aggregator -am spring-boot:run

# Analyzer
mvn -pl telemetry/analyzer -am spring-boot:run
```

> Если на порту `5432` уже запущен локальный PostgreSQL, освободите порт
> (остановите локальную службу) или измените публикуемый порт в `docker-compose.yml`
> и `spring.datasource.url` в `telemetry/analyzer/src/main/resources/application.yml`.

После запуска всех трёх сервисов можно запустить эмулятор Hub Router:

```bash
cd hub-router
./run-tests.sh          # Linux/macOS
run-tests.bat           # Windows
```

Скрипт выбирает сценарий по текущей ветке git. Для проверки Analyzer используйте ветку `4-analyzer`.

## Конфигурация

Основные настройки находятся в `application.yml` каждого сервиса:

- `spring.kafka.*` — bootstrap-серверы, группы консьюмеров, сериализаторы;
- `telemetry.*` / `aggregator.kafka.*` / `analyzer.kafka.*` — топики и параметры опроса;
- `spring.datasource.*` — подключение к PostgreSQL Analyzer;
- `grpc.server.port` — порт gRPC Collector;
- `grpc.client.hub-router.address` — адрес Hub Router для Analyzer.

Параметры можно переопределять внешней конфигурацией Spring Boot.

## Стек

| Слой | Технологии |
|---|---|
| Язык | Java 21 |
| Backend | Spring Boot 3.3.2, Spring Web, Spring Kafka, Spring Data JPA |
| Сериализация | Apache Avro 1.11.3, Protobuf/gRPC |
| Брокер | Apache Kafka |
| База данных | PostgreSQL 16 |
| Сборка | Maven (multi-module) |
| Инфраструктура | Docker Compose |

## Тесты

```bash
# unit-тесты Aggregator и Analyzer
mvn -pl telemetry/aggregator,telemetry/analyzer -am test
```

Интеграционные сценарии проверяются эмулятором `hub-router`:
`COLLECTION` (Collector), `AGGREGATION` (Collector + Aggregator), `ANALYZE` (вся цепочка).
