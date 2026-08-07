# Платёж прошёл? Докажи

Сервис проведения платёжной операции через внешнего провайдера, устойчивый к
повторам, гонкам, потерянным HTTP-ответам и перезапускам процесса.

**Главный инвариант:** одной операции соответствует **не более одного платежа
провайдера**, а финальный статус определяется **только callback-квитанцией**.
Успешный HTTP-ответ провайдера ничего не подтверждает; отсутствие ответа ничего
не опровергает.

- Python 3.14, FastAPI, SQLite (WAL, `synchronous=FULL`)
- Развёртывание: `candidate-service` (:8080) + `provider-simulator` (:8081)

## Запуск

### 1. Клонирование репозитория

```bash
git clone https://github.com/mrMoish/payment-gateway-service.git
cd payment-gateway-service
```

### 2. Запуск сервисов

Запустите `candidate-service` и `provider-simulator` через Docker Compose:

```bash
docker compose up --build
```

Готовность сервиса можно проверить:

```bash
curl localhost:8080/health
```

Ожидаемый ответ — HTTP `200`.

### 3. Остановка с сохранением данных

Остановить контейнеры без удаления Docker volume:

```bash
docker compose down
```

Данные остаются в томе `payment-gateway_payments-data`.

Повторный запуск:

```bash
docker compose up
```

Сохранённые операции, намерения отправки, `providerPaymentId` и история событий будут доступны после запуска.

### 4. Полная очистка

Чтобы остановить сервисы и удалить volume вместе со всеми сохранёнными данными:

```bash
docker compose down -v
```

## Тесты

```bash
python3.14 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest
```

79 тестов, ~5 с. Провайдер в тестах — управляемый фейк на реальном
`httpx`-транспорте: проверяются фактические заголовки, байты тела, retry и
backoff.

## Сквозной сценарий

### 1. Создание операции

```bash
curl -X POST localhost:8080/operations \
  -H 'Content-Type: application/json' \
  -d '{"operationId":"operation-123","amount":"1000.00","currency":"RUB","description":"Оплата заказа"}'
```

```
HTTP 201
{"operationId":"operation-123","amount":"1000.00","currency":"RUB",
 "description":"Оплата заказа","status":"CREATED","providerPaymentId":null, ...}
```

Повторное создание того же `operationId` → `409`.

### 2. Submit — надёжная постановка отправки

```bash
curl -X POST localhost:8080/operations/operation-123/submit
```

```
HTTP 202
{... "status":"PROCESSING","providerPaymentId":null ...}
```

Намерение отправки зафиксировано в БД, операция в `PROCESSING`. Повторный submit
для `PROCESSING`/`COMPLETED`/`REJECTED` → `200` с текущим состоянием, нового
намерения не создаётся.

### 3. Callback-квитанция

Симулятор сам присылает её на `POST /receipts` (адрес задан ему через
`CALLBACK_URL`). Вручную это выглядит так:

```bash
curl -X POST localhost:8080/receipts \
  -H 'Content-Type: application/json' \
  -d '{"providerPaymentId":"6996f637-859f-4558-aba9-c1e4c8609243",
       "operationId":"operation-123","result":"COMPLETED",
       "message":"Payment completed","occurredAt":"2026-07-15T12:00:00Z"}'
```

```
HTTP 204
```

### 4. Состояние и история

```bash
curl localhost:8080/operations/operation-123
curl localhost:8080/operations/operation-123/events
```

```json
{
  "operationId": "operation-123",
  "events": [
    {"eventId": 1, "type": "CREATED",                "fromStatus": null,         "toStatus": "CREATED",    "message": "Operation created",                                            "occurredAt": "..."},
    {"eventId": 2, "type": "SUBMIT_INTENT_RECORDED", "fromStatus": "CREATED",    "toStatus": "PROCESSING", "message": "Submission intent durably recorded; awaiting provider receipt", "occurredAt": "..."},
    {"eventId": 3, "type": "PROVIDER_ACCEPTED",      "fromStatus": "PROCESSING", "toStatus": "PROCESSING", "message": "Provider accepted the payment (provider_response); ...",        "occurredAt": "..."},
    {"eventId": 4, "type": "COMPLETED",              "fromStatus": "PROCESSING", "toStatus": "COMPLETED",  "message": "Payment completed",                                            "occurredAt": "..."}
  ]
}
```

Обратите внимание на событие 3: ответ провайдера `202` сохранил
`providerPaymentId`, но **оставил статус `PROCESSING`**. Финальный статус
появился только на событии 4 — из квитанции.

### 5. Проверка: создан ровно один платёж

Симулятор логирует каждый приём платежа с полем `replay`: `false` — создан новый
платёж, `true` — отдан уже существующий по тому же `Idempotency-Key`. Значит
«ровно один платёж на операцию» = ровно одна запись с `replay:false`.

```bash
docker compose logs provider-simulator \
  | grep '"msg":"payment accepted"' \
  | grep -c '"replay":false'
```

Ожидаемо `1` для одной операции. Полная сводка по всем операциям:

```bash
./scripts/count_payments.sh
```

Фактический вывод после сценариев ниже (обычный, 30 конкурентных submit,
рестарт):

```
operationId               payments   calls
operation-123                    1       1
operation-race                   1       1
operation-restart                1       1

VIOLATIONS: none — exactly one payment per operation
```

## Проверка конкурентности

30 параллельных submit одной операции:

```bash
curl -X POST localhost:8080/operations -H 'Content-Type: application/json' \
  -d '{"operationId":"operation-race","amount":"77.77","currency":"RUB"}'

for i in $(seq 1 30); do
  curl -s -o /dev/null -w '%{http_code}\n' \
    -X POST localhost:8080/operations/operation-race/submit &
done | sort | uniq -c
```

```
  29 200
   1 202
```

Ровно один запрос создаёт намерение (`202`), остальные получают уже сохранённое
состояние (`200`). У провайдера — один платёж.

## Проверка восстановления после рестарта

```bash
# провайдер недоступен — отправка не может завершиться
docker compose stop provider-simulator
curl -X POST localhost:8080/operations -H 'Content-Type: application/json' \
  -d '{"operationId":"operation-restart","amount":"500.00","currency":"RUB"}'
curl -X POST localhost:8080/operations/operation-restart/submit   # 202, PROCESSING

# пересоздаём контейнер сервиса, том не трогаем
docker compose rm -sf candidate-service
docker compose up -d

curl localhost:8080/operations/operation-restart
curl localhost:8080/operations/operation-restart/events
```

Операция дойдёт до `COMPLETED`, а в истории появится
`RECOVERY_INTENT_RESCHEDULED`:

```
1 CREATED                     None       -> CREATED
2 SUBMIT_INTENT_RECORDED      CREATED    -> PROCESSING
3 PROVIDER_ATTEMPT_FAILED     PROCESSING -> PROCESSING
4 RECOVERY_INTENT_RESCHEDULED PROCESSING -> PROCESSING
5 PROVIDER_ACCEPTED           PROCESSING -> PROCESSING
6 COMPLETED                   PROCESSING -> COMPLETED
```

Повтор ушёл с тем же `Idempotency-Key`, поэтому второго платежа нет.

Готовый скрипт живых проверок (краевые случаи квитанций + переживание
`down`/`up`): `./scripts/verify_live.sh`.

## API

| Метод | Маршрут | Статус | Назначение |
|---|---|---|---|
| GET | `/health` | 200 | проверка готовности |
| POST | `/operations` | 201 | создание операции (409 — повтор `operationId`) |
| POST | `/operations/{id}/submit` | 202 / 200 | 202 — намерение создано, 200 — уже существовало |
| POST | `/receipts` | 204 | приём callback-квитанции (409 — несовпадающий `providerPaymentId`) |
| GET | `/operations/{id}` | 200 | текущее состояние |
| GET | `/operations/{id}/events` | 200 | история переходов |
| GET | `/metrics` | 200 | метрики (JSON) |
| GET | `/metrics/prometheus` | 200 | метрики (Prometheus) |
| GET | `/docs` | 200 | OpenAPI UI |

`amount` — положительная десятичная строка, ≤ 2 знаков после точки; валюта
`RUB`. Нарушения → `422`.

### Состояния

| Состояние | Значение |
|---|---|
| `CREATED` | операция создана, отправка не запрошена |
| `PROCESSING` | намерение отправки надёжно сохранено, ждём результат провайдера |
| `COMPLETED` | провайдер подтвердил успех квитанцией |
| `REJECTED` | провайдер подтвердил отказ квитанцией |

Переход в `COMPLETED`/`REJECTED` выполняет **только** обработчик квитанции.

### Правила обработки квитанций

| Ситуация | Ответ | Эффект |
|---|---|---|
| первая квитанция | 204 | финальный статус зафиксирован |
| повтор той же квитанции | 204 | нового перехода нет |
| поздняя квитанция с противоположным результатом | 204 | статус не меняется, пишется `RECEIPT_IGNORED_ALREADY_FINAL` |
| квитанция раньше HTTP-ответа провайдера | 204 | именно она устанавливает `providerPaymentId` |
| несовпадающий `providerPaymentId` после установленной связи | 409 | ничего не меняется |
| неизвестная операция | 404 | — |

## Архитектура

```
POST /operations          -> operations (CREATED)
POST /operations/:id/submit -> ОДНА транзакция:
                               CAS status CREATED->PROCESSING
                               + INSERT submission_intents (outbox)
                             HTTP-вызова здесь нет
                                    |
              (фоновый worker, вне транзакций)
                                    v
     POST {PROVIDER_URL}/payments  Idempotency-Key: <operationId>
                                    |
              202 -> сохранить providerPaymentId, статус НЕ менять
                                    |
POST /receipts -> ОДНА транзакция: дедупликация + связывание + финальный статус
```

### Схема хранилища

| Таблица | Роль | Ключевые ограничения |
|---|---|---|
| `operations` | состояние операции | `operation_id` PK, `CHECK` на статус |
| `submission_intents` | outbox намерений | `operation_id` **PK** → не более одного намерения; `idempotency_key` UNIQUE, пишется один раз; `request_body` фиксируется при создании |
| `events` | история переходов | `UNIQUE (operation_id, seq)`, `seq` выделяется в той же транзакции |
| `receipts` | дедупликация квитанций | `UNIQUE (operation_id, provider_payment_id, result)` |
| `provider_stats` | счётчики вызовов и повторов | — |

### Как обеспечивается инвариант

**Не более одного платежа на операцию.**
`operation_id` как PRIMARY KEY в `submission_intents` физически запрещает второе
намерение. `idempotency_key` и `request_body` пишутся один раз и никогда не
обновляются — любой повтор (гонка, retry, рестарт) уходит с тем же ключом и
байт-идентичным телом, поэтому провайдер возвращает существующий платёж. Новый
ключ не генерируется нигде в коде.

**Ровно один победитель гонки.**
`UPDATE operations SET status='PROCESSING' WHERE operation_id=? AND status='CREATED'` —
`rowcount=1` только у одного конкурента, остальные читают сохранённое состояние.
Проверено на 30 параллельных submit и на нескольких независимых подключениях к
БД.

**Финальный статус только от квитанции.**
`COMPLETED`/`REJECTED` пишет единственная функция — `Storage.apply_receipt`.
Обработка ответа `202` вызывает
`UPDATE operations SET provider_payment_id=? WHERE ... AND provider_payment_id IS NULL`
и колонку `status` не трогает вообще.

**Нет откатов из финального статуса.**
Все переходы защищены `WHERE status NOT IN ('COMPLETED','REJECTED')`, поэтому
поздний `202` не возвращает операцию в `PROCESSING`.

**Блокировка не держится на время внешнего вызова.**
Транзакция закрывается до HTTP-запроса. Worker берёт lease на намерение, но это
лишь защита от параллельных попыток: истёкший lease переигрывается с тем же
ключом, поэтому дедупликацию гарантирует провайдер, а не наша блокировка. Именно
поэтому квитанция может прийти во время вызова — это покрыто тестом.

**Переживание рестарта.**
При старте `recover_intents()` сбрасывает lease мёртвого процесса, делает
намерения снова готовыми к выполнению (с исходным ключом), достраивает намерение
для `PROCESSING`-операции, у которой строка не записалась, и закрывает намерения
операций, уже закрытых квитанцией. Остановка между «провайдер принял платёж» и
«мы сохранили это локально» второго платежа не даёт — покрыто тестом
`test_a_crash_between_acceptance_and_persistence_creates_no_second_payment`.

## Наблюдаемость

JSON-логи в stdout с полями `operationId`, `correlationId`, `providerPaymentId`,
`attempt`, `idempotencyKey`:

```json
{"timestamp":"...","level":"INFO","service":"candidate-service","logger":"app.provider",
 "message":"Provider accepted the payment","operationId":"operation-123",
 "correlationId":"operation-123","idempotencyKey":"operation-123","attempt":1,
 "providerPaymentId":"6996f637-...","providerStatus":"ACCEPTED","providerStatusCode":202}
```

`GET /metrics`:

```json
{
  "operationsProcessing": 0,
  "operationsByStatus": {"CREATED": 0, "PROCESSING": 0, "COMPLETED": 3, "REJECTED": 0},
  "submissionIntentsByState": {"PENDING": 0, "IN_FLIGHT": 0, "ACCEPTED": 0, "SETTLED": 3},
  "providerCallsTotal": 8,
  "providerRetriesTotal": 4
}
```

## Конфигурация

| Переменная | Дефолт | Назначение |
|---|---|---|
| `PROVIDER_URL` | `http://provider-simulator:8081` | адрес провайдера |
| `DATABASE_PATH` | `/data/payments.db` | файл SQLite (на томе) |
| `PROVIDER_TIMEOUT_SECONDS` | `5.0` | таймаут одной попытки |
| `PROVIDER_MAX_ATTEMPTS` | `4` | предел попыток в одном цикле |
| `PROVIDER_BACKOFF_BASE_SECONDS` | `0.2` | первая задержка (далее ×2) |
| `PROVIDER_BACKOFF_MAX_SECONDS` | `5.0` | верхний предел задержки |
| `PROVIDER_BACKOFF_JITTER` | `1.0` | доля jitter (1.0 — full jitter) |
| `WORKER_LEASE_SECONDS` | `60.0` | срок lease на намерение |
| `WORKER_POLL_INTERVAL_SECONDS` | `0.5` | период опроса |
| `WORKER_BATCH_SIZE` | `16` | намерений за цикл |
| `SHUTDOWN_GRACE_SECONDS` | `10.0` | ожидание текущих попыток при остановке |
| `LOG_LEVEL` | `INFO` | уровень логов |

Симулятору обязательна переменная `CALLBACK_URL` (задана в `compose.yaml`) — без
неё он не стартует.

## Ограничения

- **Один процесс-писатель.** `Dockerfile` запускает uvicorn с `--workers 1`:
  SQLite плохо переносит конкурентную запись. Схема к горизонтальному
  масштабированию готова (lease + неизменный ключ + CAS), но для нескольких
  реплик нужна общая БД (Postgres) вместо файла.
- **Retry ограничен по попыткам, не по общему времени.** Исчерпав бюджет,
  worker оставляет операцию в `PROCESSING` и вернётся к ней позже — решать за
  провайдера мы не вправе.
- **Полный список значений `MODE` симулятора не установлен.** Сценарии отказов
  (503, потеря ответа, ранняя/повторная/конфликтующая квитанция) покрыты тестами
  на управляемом фейке, где отказы задаются явно.
