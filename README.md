# High-Concurrency Ticket Reservation & Inventory System

How do you safely reserve scarce inventory when thousands of requests arrive at once?

**Users → FastAPI → PostgreSQL** — a small system that makes concurrency failure modes tangible: race conditions, locking strategies, atomic updates, idempotency, reservation TTL, deadlock retries, and load-test comparisons.

## Architecture

```
Users
  ↓
FastAPI (async)
  ↓
PostgreSQL 16
  ├── ticket_inventory  (available, version)
  └── reservations      (status, idempotency_key, expires_at)
```

## Core ideas

| Concept | How it's implemented |
|---|---|
| **Race conditions** | `naive` strategy: read available → compute → absolute `SET` (oversells) |
| **Atomic inventory updates** | `atomic`: `UPDATE ... WHERE available >= qty RETURNING` |
| **Optimistic locking** | `version` column + compare-and-swap, app retries on conflict |
| **Pessimistic locking** | `SELECT ... FOR UPDATE` then update |
| **Isolation** | `READ COMMITTED` (Postgres default; enough with atomic / `FOR UPDATE`) |
| **Idempotency** | required `Idempotency-Key` header + unique constraint |
| **Reservation TTL** | `expires_at`; background sweeper releases stock (`FOR UPDATE SKIP LOCKED`) |
| **Deadlocks** | retry with jittered backoff on `40P01` / `40001` |
| **Consistency check** | `available + held(pending\|confirmed) == total` |

## Quick start

**Requirements:** Python 3.11+ and PostgreSQL (Docker or local)

```bash
# Windows
scripts\dev.bat

# macOS / Linux
chmod +x scripts/dev.sh && ./scripts/dev.sh
```

Or manually:

```bash
docker compose up -d
python -m venv .venv
# Windows: .venv\Scripts\activate
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8001
```

- Docs: http://127.0.0.1:8001/docs
- Seed inventory: `22222222-2222-2222-2222-222222222222` (100 GA tickets)

### Local PostgreSQL (without Docker)

```bash
psql -U postgres -f scripts/setup_local_pg.sql
psql -U postgres -d tickets -f migrations/001_init.sql
# grant table rights to ticket if tables were created as postgres
```

Copy `.env.example` → `.env` and set:

`DATABASE_URL=postgresql://ticket:ticket@127.0.0.1:5432/tickets`

## API

### Reserve (requires Idempotency-Key)

```bash
curl -s -X POST http://127.0.0.1:8001/reservations \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: demo-key-1" \
  -d '{
    "inventory_id": "22222222-2222-2222-2222-222222222222",
    "user_id": "alice",
    "quantity": 2,
    "strategy": "atomic"
  }'
```

Strategies: `naive` | `atomic` | `optimistic` | `pessimistic`

### Confirm / cancel / consistency

```bash
curl -s -X POST http://127.0.0.1:8001/reservations/{id}/confirm \
  -H "Content-Type: application/json" \
  -d '{"user_id":"alice"}'

curl -s http://127.0.0.1:8001/inventory/22222222-2222-2222-2222-222222222222/consistency
```

### Reset stock (benchmarks)

```bash
curl -s -X POST "http://127.0.0.1:8001/inventory/22222222-2222-2222-2222-222222222222/reset?available=100"
```

Passing `available=N` sets both `total` and `available` to `N` so scarce-stock tests keep `available + held == total`.

## Correctness demo (race)

50 buyers, 10 tickets:

```bash
python loadtest/demo_race.py --strategy naive --stock 10 --buyers 50
python loadtest/demo_race.py --strategy atomic --stock 10 --buyers 50
```

| Strategy | Result |
|---|---|
| `naive` | **OVERSOLD** (e.g. held=50 against stock=10) |
| `atomic` / `optimistic` / `pessimistic` | **SAFE** — exactly 10 reserved |

## Benchmark results

Measure RPS + p50 / p95 / p99:

```bash
# Safe strategies only
python loadtest/bench.py --safe --requests 1000 --concurrency 50 --stock 200

# All strategies (includes naive oversell)
python loadtest/bench.py --compare --requests 500 --concurrency 50 --stock 100
```

### Scarce stock (hot row)

`1000` requests · concurrency `50` · stock `200`

| Strategy | RPS | p50 | p95 | p99 | ok / sold_out |
|---|---:|---:|---:|---:|---|
| **atomic** | **122.3** | **250.2 ms** | **1164.8 ms** | **1555.0 ms** | 200 / 800 |
| optimistic | 109.0 | 308.6 ms | 1254.2 ms | 1930.7 ms | 200 / 800 |
| pessimistic | 110.8 | 292.4 ms | 1263.0 ms | 1765.7 ms | 200 / 800 |

Winner under contention: **atomic**.

### Full sell-through (no sold-out noise)

`500` requests · concurrency `50` · stock `500`

| Strategy | RPS | p50 | p95 | p99 | ok / sold_out |
|---|---:|---:|---:|---:|---|
| atomic | 198.5 | 158.5 ms | 749.0 ms | 1029.3 ms | 500 / 0 |
| optimistic | 179.3 | 183.3 ms | 789.9 ms | 1214.6 ms | 500 / 0 |
| **pessimistic** | **222.6** | **147.2 ms** | **641.7 ms** | **925.4 ms** | 500 / 0 |

Winner on this run: **pessimistic**. All safe strategies stayed consistent (no oversell, 0 errors).

## Strategy notes

### `atomic` (default — prefer this)

```sql
UPDATE ticket_inventory
SET available = available - $qty, version = version + 1
WHERE id = $id AND available >= $qty
RETURNING available;
```

One statement, no lost updates, strong under hot-row contention.

### `optimistic`

Read `(available, version)` → update only if `version` unchanged. App retries on conflict. Best when conflicts are rare; more retries than atomic on a single hot row.

### `pessimistic`

`SELECT ... FOR UPDATE` serializes writers on the inventory row. Correct; lock wait grows with concurrency on one hot key.

### `naive` (educational only)

Classic TOCTOU. Two transactions can both observe `available = 1` and both create a reservation — use it to prove why inventory needs atomicity or locks.

## Reservation lifecycle

```
pending ──TTL expiry──► expired   (stock released)
   │
   ├── confirm ──► confirmed  (stock stays consumed)
   └── cancel  ──► cancelled  (stock released)
```

TTL default: **120s** (`RESERVATION_TTL_SECONDS`). A background sweeper runs every 2s; or call `POST /admin/expire`.

## Project layout

```
app/
  main.py                  # HTTP API + TTL sweeper
  strategies/              # naive / atomic / optimistic / pessimistic
  services/reservations.py
  errors.py                # deadlock retry helper
migrations/001_init.sql
loadtest/bench.py          # RPS + p50/p95/p99 (--safe / --compare)
loadtest/demo_race.py
docker-compose.yml
scripts/setup_local_pg.sql
```

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | `postgresql://ticket:ticket@localhost:5432/tickets` | Postgres DSN |
| `RESERVATION_TTL_SECONDS` | `120` | Hold time before auto-release |
| `DB_POOL_MAX` | `40` | asyncpg pool size |
| `DEFAULT_STRATEGY` | `atomic` | Used when body omits `strategy` |
| `PORT` | `8001` | API port |
