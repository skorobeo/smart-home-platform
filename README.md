<div align="center">

# Smart Home Platform

**Телеметрия и коммерция для умного дома**  
Сервис Collector принимает события от хабов и сохраняет их в Kafka

</div>

---

## О проекте

Платформа для умного дома: сервис **Collector** принимает телеметрию от хабов по HTTP (JSON), валидирует её, кодирует в бинарный формат **Avro** и сохраняет в топики **Kafka**. Каждый вид данных идёт в отдельный топик, чтобы обработку можно было масштабировать и развивать независимо.

В проекте также есть модули `commerce` (сервисы продаж устройств) и `infra` — каркас из шаблона курса.

## Статус и границы

- **Реализовано по ТЗ (спринт 19):** сервис Collector, Avro/Protobuf-схемы, Kafka-продюсер, Docker Compose.
- `commerce` и `infra` — заготовки, их реализация вне рамок задания.
- Интеграционные сценарии проверялись эмулятором хабов `hub-router` (в репозиторий не включён).

## Возможности

- Приём событий датчиков и событий хаба через два REST-эндпоинта.
- Полиморфная десериализация JSON: тип события определяется полем `type` (Jackson `@JsonTypeInfo` / `@JsonSubTypes`).
- Avro-схемы для событий датчиков и хабов: освещённость, температура, движение, климат, переключатель; добавление и удаление устройства или сценария.
- Отправка событий в Kafka в формате Avro через `GeneralAvroSerializer`.
- Разделение потоков по топикам: `telemetry.sensors.v1` и `telemetry.hubs.v1`.
- Docker Compose для Kafka и PostgreSQL с автосозданием топиков.

## Стек

| Слой | Технологии |
|---|---|
| Язык | Java 21 |
| Backend | Spring Boot 3.3.2, Spring Web, Validation, Spring Kafka |
| Сериализация | Apache Avro 1.11.3, Protobuf/gRPC |
| Брокер | Apache Kafka |
| База данных (каркас) | PostgreSQL 16 |
| Сборка | Maven (multi-module) |
| Инфраструктура | Docker Compose |

## Архитектура

```
smart-home-platform/
├── telemetry/
│   ├── collector/          — приём событий по HTTP и отправка в Kafka
│   └── serialization/
│       ├── avro-schemas/   — Avro-схемы событий
│       └── proto-schemas/  — Protobuf-схемы для следующих этапов
├── commerce/               — каркас сервисов продаж (product/order/inventory)
├── infra/                  — каркас инфраструктурных компонентов
└── docker-compose.yml      — Kafka, PostgreSQL, инициализация топиков
```

Поток данных:

```
Хабы → hub-router (эмулятор) → Collector (HTTP JSON) → Avro → Kafka
                                                              ├─ telemetry.sensors.v1
                                                              └─ telemetry.hubs.v1
```

## API

- `POST /events/sensors` — события датчиков.
- `POST /events/hubs` — события хабов и сценариев.

Пример события датчика освещённости:

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

## Запуск

Требования: Java 21, Maven 3.9+, Docker.

```bash
# Kafka + PostgreSQL + создание топиков
docker compose up -d

# сервис Collector
mvn -pl telemetry/collector -am spring-boot:run
```

Collector слушает порт `8080`. Bootstrap-сервер Kafka: `localhost:9092`.

## Роль

Автор реализации Collector по ТЗ: приём и валидация событий, Avro-схемы, Kafka-продюсер, конфигурация Docker Compose.
