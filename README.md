# Практическая работа: Apache Kafka

Приложение отправляет заказы `Order` в топик `orders` в формате JSON Schema с использованием Schema Registry. Продюсер, одиночный и пакетный консьюмеры работают отдельными воркерами и масштабируются независимо.

## Развёртывание Kafka-кластера

Нужны Docker с Compose и `make`. Команды выполняются из корня проекта.

1. Создайте `.env` из `.env.example`, если файл ещё не существует:

   ```bash
   cp .env.example .env
   ```

2. Запустите инфраструктуру: ZooKeeper, два брокера Kafka, Schema Registry и Kafka UI:

   ```bash
   docker compose --env-file .env.example --env-file .env up -d zookeeper kafka1 kafka2 schema-registry kafka-ui
   ```

3. Дождитесь готовности обоих брокеров и Schema Registry. Команды должны завершаться успешно:

   ```bash
   docker compose exec -T kafka1 kafka-topics --bootstrap-server kafka1:9092 --list
   docker compose exec -T kafka2 kafka-topics --bootstrap-server kafka2:9092 --list
   curl -f http://localhost:8081/subjects
   ```

4. Создайте топик с тремя партициями и двумя репликами, затем запустите воркеры:

   ```bash
   COMPOSE_ENV_FILES=.env.example,.env bash scripts/kafka_init.sh
   make up
   ```

`make up` собирает образы и запускает Compose с профилем `workers`. Количество экземпляров задаётся в `.env` переменными `PRODUCER_REPLICAS`, `SINGLE_CONSUMER_REPLICAS` и `BATCH_CONSUMER_REPLICAS`; в примере указано по два. Значения `.env` имеют приоритет над `.env.example`. Kafka доступна внутри Docker-сети по адресам `kafka1:9092` и `kafka2:9092`.

## Проверка работы кластера и приложения

Проверьте состояние контейнеров и описание топика:

```bash
docker compose --env-file .env.example --env-file .env --profile workers ps
docker compose exec -T kafka1 kafka-topics --describe --topic orders --bootstrap-server kafka1:9092
```

Ожидаются `PartitionCount: 3`, `ReplicationFactor: 2`, назначенный `Leader` и по два брокера в `Replicas` и `Isr` у каждой партиции.

Проверьте получение сообщений и сохранённые смещения обеих групп:

```bash
docker compose logs -f produce-order-worker single-consume-order-worker batch-consume-order-worker
docker compose exec -T kafka1 kafka-consumer-groups --bootstrap-server kafka1:9092 --describe --group orders-single
docker compose exec -T kafka1 kafka-consumer-groups --bootstrap-server kafka1:9092 --describe --group orders-batch
```

В логах видны отправляемые и полученные заказы, подтверждения доставки и `Batch processed and committed: 10 messages`. Single-консьюмер обрабатывает сообщения по одному с автокоммитом. Batch-консьюмер накапливает минимум 10 сообщений, обрабатывает пачку и выполняет один синхронный коммит. Разные группы позволяют обоим типам консьюмеров читать одни и те же сообщения. Ошибки выводятся в логи; некорректные сообщения пропускаются, чтение продолжается.

Команды проверки кода:

| Команда | Назначение |
| --- | --- |
| `make test` | Локальные unit-тесты; нужны Python 3.13+ и `uv` |
| `make test-docker` | Сборка тестового образа и запуск unit-тестов в Docker |
| `make lint` | Проверка кода линтером Ruff |
| `make lint-fix` | Автоматическое исправление замечаний Ruff, для которых доступен fix |
| `uv run mypy` | Строгая проверка типов |

Unit-тесты используют моки и не требуют работающей Kafka.

## Параметры конфигурации

| Параметр | Значение в проекте | Назначение |
| --- | --- | --- |
| `partitions` / `replication-factor` | `3` / `2` | Число партиций и копий каждой партиции топика `orders` |
| `min.insync.replicas` | `2` | Для записи нужны две синхронные реплики |
| `acks` | `all` | Продюсер ожидает подтверждения от всех текущих ISR |
| `retries` | `10` | Повторы отправки при временных ошибках |
| `enable.idempotence` | `true` | Защита от дубликатов при сетевых повторах в рамках сессии продюсера |
| `delivery.timeout.ms` | `120000` | Предельное время доставки сообщения |
| `group.id` | `orders-single` / `orders-batch` | Независимое чтение сообщений двумя группами |
| `auto.offset.reset` | `earliest` | Чтение с начала при отсутствии сохранённого смещения |
| `enable.auto.commit` | `true` для single, `false` для batch | Автоматический или ручной коммит |
| `enable.auto.offset.store` | `false` | Локальное сохранение смещения после обработки |
| `fetch.min.bytes` | `1` для single, `1024` для batch | Минимальный желаемый объём ответа брокера |
| `fetch.wait.max.ms` | `500` | Максимальное ожидание fetch в Python-клиенте на librdkafka |

Параметры приложения и количества реплик описаны в `.env.example`. `fetch.min.bytes` задаёт объём в байтах, поэтому размер пачки обеспечивается накоплением результатов `consume()`, который может вернуть меньше запрошенного.

Продюсер настроен на At Least Once: подтверждения и повторы уменьшают риск потери при временных сбоях. Итоговая ошибка доставки логируется при исчерпании повторов или таймаута. При аварийном завершении процесса очередь отправки на диске не сохраняется. С `min.insync.replicas=2` отказ одного из двух брокеров временно блокирует запись.

## Проверка через Kafka UI

Откройте <http://localhost:8080> и выберите кластер `local`:

1. В `Brokers` проверьте наличие двух брокеров.
2. В `Topics → orders` проверьте три партиции, по две реплики и наличие сообщений.
3. В `Consumer Groups` проверьте группы `orders-single` и `orders-batch`, их участников, смещения и lag. При настройках из `.env.example` у каждой группы два участника.

Сообщения содержат префикс Confluent. Для просмотра значений в формате JSON Schema нужно подключение Kafka UI к Schema Registry (`http://schema-registry:8081` внутри Docker-сети); в текущем Compose оно не задано.
