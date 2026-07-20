# Event-Driven User Activity Processor

A production-style, event-driven microservice that consumes `user_activity`
events from RabbitMQ, processes them asynchronously and idempotently, and
persists aggregated insights into PostgreSQL `user_profiles` in real time.

Built with **Python, FastAPI, Pika (RabbitMQ), SQLAlchemy, PostgreSQL, and
Docker Compose**.

---

## 1. Architecture

```
                 ┌───────────────┐
                 │  producer.py  │  (publishes user_activity events)
                 └───────┬───────┘
                         │ basic_publish
                         ▼
              ┌─────────────────────┐
              │  user_activity_queue│  (durable, dead-letter configured)
              └─────────┬───────────┘
                         │ basic_consume (prefetch=1)
                         ▼
        ┌───────────────────────────────────┐
        │      app_service (consumer)       │
        │  1. parse JSON                    │
        │  2. validate schema               │
        │  3. idempotency check (event_id)  │
        │  4. upsert user_profiles          │
        │  5. ack (success) / nack (failure)│
        └───────┬────────────────────┬──────┘
                 │ success                │ failure (nack, requeue=False)
                 ▼                         ▼
      ┌────────────────────┐   ┌───────────────────┐
      │  PostgreSQL        │   │  dlx_exchange     │
      │  - user_profiles   │   │  (direct)         │
      │  - processed_events│   └─────────┬─────────┘
      └────────────────────┘              │ routing_key='dlq'
                                           ▼
                                ┌───────────────────────┐
                                │  user_activity_dlq    │
                                │ (manual inspection/   │
                                │  replay)              │
                                └───────────────────────┘

  app_service also exposes:  GET /health  → checks RabbitMQ + PostgreSQL
```

The FastAPI process and the RabbitMQ consumer run **in the same container**,
in separate threads: the consumer loop runs on a background daemon thread
started on app startup, while the main thread serves `/health` via uvicorn.
This keeps the service to a single container/process group while still
satisfying the "expose an HTTP health endpoint" requirement.

---

## 2. Project layout

```
.
├── app/
│   ├── main.py             FastAPI app: /health, starts consumer thread
│   ├── consumer.py         RabbitMQ topology + consume loop + DLQ routing
│   ├── schema.py            user_activity event validation
│   ├── profile_service.py   idempotent user_profiles update logic
│   ├── models.py             SQLAlchemy models (UserProfile, ProcessedEvent)
│   ├── db.py                  engine/session/Base + init_db()
│   ├── types.py                cross-DB GUID column type
│   ├── logging_config.py        structured JSON logging
│   └── config.py                  all configuration via env vars
├── producer.py               standalone event producer / simulator
├── init.sql                  auto-run schema migration for Postgres
├── tests/
│   ├── test_schema_validation.py   unit tests
│   ├── test_profile_service.py      unit tests (idempotency, aggregation)
│   └── integration_test.py           end-to-end test against live stack
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── pytest.ini
```

---

## 3. Running the whole stack

Requires Docker and Docker Compose.

```bash
git clone <https://github.com/JAHNAVISINDHU/event-driven-user-activity-processor>
cd event-driven-user-activity-processor
cp .env.example .env        # optional — defaults already work out of the box
docker compose up --build
```

This starts three services:

| Service     | Purpose                                   | Port(s)            |
|-------------|--------------------------------------------|---------------------|
| `rabbitmq`  | Message broker + management UI             | 5672, 15672          |
| `postgres`  | Data store for `user_profiles`              | 5432                  |
| `app_service` | FastAPI + RabbitMQ consumer               | 8000                   |

All three have Docker healthchecks; `app_service` waits for `rabbitmq` and
`postgres` to report healthy before starting.

Check it's up:

```bash
curl http://localhost:8000/health
# {"status":"ok","database":"connected","rabbitmq":"connected"}
```

---

## 4. Generating events (producer)

With the stack running:

```bash
docker compose exec app_service python producer.py
```

This publishes a realistic simulated stream: **3 users × 6 events each**,
with randomized `event_type`s (`view`, `click`, `add_to_cart`, `purchase`,
`login`, `logout`) and payloads, plus one intentionally malformed event to
demonstrate DLQ routing.

You can also import and call `producer.publish_event()` /
`producer.build_event()` directly from your own scripts to publish specific
events.

---

## 5. Inspecting results

**RabbitMQ management UI:** http://localhost:15672 (user/pass from `.env`,
default `guest`/`guest`). Check the `user_activity_queue` and
`user_activity_dlq` queues under the "Queues" tab.

**PostgreSQL:**

```bash
docker compose exec postgres psql -U activity_user -d activity_db \
  -c "SELECT user_id, total_events, last_event_type, event_counts, last_activity_at FROM user_profiles;"
```

---

## 6. Running tests

**Unit tests** (fast, no external services — uses an in-memory SQLite DB):

```bash
docker compose exec app_service pytest tests/ -v
# or locally: pip install -r requirements.txt && pytest tests/ -v
```

Covers:
- Event schema validation (`tests/test_schema_validation.py`) — missing
  fields, invalid UUIDs, bad timestamps, non-object payloads, etc.
- Idempotent profile-update logic (`tests/test_profile_service.py`) — new
  profile creation, aggregate updates, duplicate `event_id` handling,
  out-of-order timestamp handling, multi-user isolation.

**Integration test** (talks to the real running stack):

```bash
docker compose exec app_service python -m tests.integration_test
```

This publishes events via the producer, polls `user_profiles` until the
consumer has processed them, verifies the aggregates are correct,
re-publishes a duplicate `event_id` to confirm idempotency, and finally
publishes a malformed event to confirm it's routed to the DLQ.

---

## 7. Event schema

```json
{
  "event_id": "3fb1e2c0-...-uuid",
  "user_id": "9a2d4e10-...-uuid",
  "event_type": "purchase",
  "timestamp": "2026-07-19T10:15:30+00:00",
  "payload": { "item_id": "...", "value": 42.50 }
}
```

`payload` is intentionally unconstrained (any JSON object) so new
`event_type`s can be introduced without a schema migration — the `payload`
contents are stored implicitly via `event_counts`, but not individually
persisted per-field, keeping the aggregate table stable as new event types
are added.

## 8. `user_profiles` schema

| Column             | Type                      | Notes                                   |
|--------------------|---------------------------|-------------------------------------------|
| `user_id`          | UUID, PK                  |                                             |
| `last_activity_at` | TIMESTAMPTZ                | only moves forward (see idempotency below) |
| `total_events`     | INTEGER, default 0          |                                             |
| `last_event_type`  | VARCHAR                      |                                             |
| `event_counts`     | JSONB                          | e.g. `{"view": 3, "purchase": 1}`           |

A second table, `processed_events (event_id PK, processed_at)`, backs
idempotency (see below) and is not part of the public data model.

---

## 9. Key design decisions

**Idempotency.** Every successfully processed `event_id` is recorded in
`processed_events`. Before applying an event's effects, the consumer checks
whether that `event_id` already exists; if so, the message is acknowledged
(so it's removed from the queue) but its effects are *not* re-applied. This
guarantees `total_events` / `event_counts` are never double-counted under
at-least-once delivery. Separately, `last_activity_at` / `last_event_type`
only move forward in time — an event with an older `timestamp` than what's
already stored still increments the counters (it happened), but won't
regress the "most recent activity" fields, guarding against out-of-order
delivery.

**Dead Letter Queue.** `user_activity_queue` is declared with
`x-dead-letter-exchange` / `x-dead-letter-routing-key` pointing at a
dedicated `dlx_exchange` → `user_activity_dlq`. Any message that fails JSON
parsing, schema validation, or is `nack`'d for any other reason
(`requeue=False`) is routed there automatically by RabbitMQ rather than
being lost or endlessly retried against the main queue. Operators can
inspect/replay from the DLQ via the management UI.

**Manual ack, prefetch=1.** Messages are only acknowledged after the
database transaction commits successfully. `basic_qos(prefetch_count=1)`
ensures a worker isn't handed a new message until it's finished with the
current one, trading some throughput for stronger delivery guarantees.

**ORM (SQLAlchemy) over raw SQL.** Chosen for readability, testability
(the same models run against SQLite in-memory for fast unit tests and
against PostgreSQL in production), and to avoid hand-rolled SQL string
building for the JSONB increment logic.

**Schema evolution.** `event_type` is a free-form string and `payload` an
unconstrained JSON object, so new event types require no migration. For a
real system, the next step would be an explicit `event_version` field in
the payload plus a schema registry / compatibility policy — noted here as
a bonus consideration per the task FAQ, not implemented in this scope.

**Structured logging.** All consumer log lines are emitted as single-line
JSON (`{"timestamp", "level", "event", ...}`) via a custom
`logging.Formatter`, making them easy to ingest into a log aggregator.

**Stateless service.** All state lives in PostgreSQL; `app_service` itself
holds no persistent state and can be scaled horizontally (multiple
consumers on the same queue, coordinated by `prefetch_count` + RabbitMQ's
round-robin dispatch) without code changes.

---

## 10. Common failure scenarios & how they're handled

| Scenario                                   | Behavior                                                        |
|---------------------------------------------|--------------------------------------------------------------------|
| Malformed JSON body                          | `nack(requeue=False)` → routed to DLQ                               |
| Missing/invalid schema fields                | `nack(requeue=False)` → routed to DLQ                               |
| DB error during processing                    | rollback, `nack(requeue=False)` → routed to DLQ (avoids hot-loop retry) |
| Duplicate `event_id` (redelivery)               | acknowledged, effects skipped (idempotent)                          |
| Out-of-order `timestamp`                          | counters still increment; `last_activity_at` doesn't regress        |
| RabbitMQ connection drops                           | consumer reconnects with a backoff delay and re-declares topology     |

---

## 11. Environment variables

See [`.env.example`](./.env.example) for the full list with defaults
(RabbitMQ host/port/credentials, queue/exchange names, PostgreSQL
host/port/credentials, app port). No credentials are hardcoded anywhere in
the codebase.
