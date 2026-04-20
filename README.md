# postgres proxy  

> This is a repository for learning purposes. This is NOT ready for production.

A Postgres connection proxy written in Go that implements:

- **Connection pooling** (transaction-mode) — multiplex many client connections onto a smaller pool of persistent backend connections
- **Read/write splitting** — automatically route `SELECT` queries to replicas, writes to the primary
- **Health checking** — background TCP probes remove unhealthy backends from rotation
- **Observability** — HTTP metrics endpoint at `/metrics` and `/healthz`

Speaks the PostgreSQL wire protocol v3

```
psql / app
    │
    ▼
 proxy :5433          ← clients connect here
    │
    ├─ WRITE → primary :5432
    ├─ READ  → replica-0 :5434   (round-robin)
    └─ READ  → replica-1 :5435
```

---

## Quick Start

```bash
go build -o proxy ./cmd/proxy
./proxy                          # defaults: primary on localhost:5432
./proxy cmd/proxy/config.json    # with config file
psql -h localhost -p 5433 -U postgres mydb
```

---

## Configuration

```json
{
  "listen_addr": "0.0.0.0:5433",
  "metrics_addr": ":9090",
  "pool_size": 10,
  "pool_min_size": 0,
  "pool_max_idle_seconds": 300,
  "pool_max_life_seconds": 3600,
  "pool_acquire_timeout_seconds": 3,
  "pool_idle_check_seconds": 30,
  "health_interval_seconds": 5,
  "primary": {
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password": "postgres",
    "database": "postgres"
  },
  "replicas": [
    {
      "host": "localhost",
      "port": 5434,
      "user": "postgres",
      "password": "postgres",
      "database": "postgres"
    }
  ]
}
```

| Field | Default | Description |
|---|---|---|
| `listen_addr` | `0.0.0.0:5433` | Address clients connect to |
| `metrics_addr` | `:9090` | HTTP metrics/health endpoint |
| `pool_size` | `10` | Max backend connections per pool |
| `pool_min_size` | `0` | Target minimum connections kept open (best-effort warmup + reaper floor) |
| `pool_max_idle_seconds` | `300` | Idle longer than this may be closed if total exceeds `pool_min_size` |
| `pool_max_life_seconds` | `3600` | Recycle connections older than this |
| `pool_acquire_timeout_seconds` | `3` | Max wait for a free backend conn (`0` = no client-side deadline; pool still obeys internal settings) |
| `pool_idle_check_seconds` | `30` | Reaper interval (`0` disables periodic idle reaping) |
| `health_interval_seconds` | `5` | How often to probe backends |

---

## Metrics

```bash
curl localhost:9090/metrics
```

```json
{
  "uptime_seconds": 42,
  "queries_total": 1000,
  "queries_read": 820,
  "queries_write": 180,
  "clients_active": 3,
  "pool_exhausted": 0,
  "backend_errors": 0,
  "pool_wait_total": 12,
  "pool_wait_seconds": 0.04,
  "pool_acquire_timeouts": 0,
  "pools": [
    { "label": "primary",   "total": 4, "in_use": 1, "idle": 3, "waiting": 0 },
    { "label": "replica-0", "total": 6, "in_use": 2, "idle": 4, "waiting": 1 }
  ]
}
```

```bash
curl localhost:9090/healthz   # → "ok"
```

---

## Architecture

```
cmd/proxy/main.go          Entry point, accept loop, per-client goroutine
internal/
  protocol/                PostgreSQL wire protocol v3
    protocol.go            Message framing, StartupMessage, query classification
  pool/
    pool.go                Transaction-mode connection pool
    pool_md5.go            MD5 password auth (authType 5)
    pool_scram.go          SCRAM-SHA-256 auth (authType 10, RFC 5802)
  router/
    router.go              Route queries to primary or replica
  health/
    health.go              Background TCP health checker
  config/
    config.go              JSON config loader
  metrics/
    metrics.go             Atomic counters + HTTP /metrics
```

### How a query flows

```
1.  Client connects on :5433
2.  Proxy reads StartupMessage  (user, database, protocol version)
3.  Proxy sends AuthOK + ReadyForQuery
4.  Client sends Query 'Q'
5.  Router inspects SQL:
      SELECT              → replica pool (round-robin, healthy only)
      INSERT/UPDATE/...   → primary pool
      SELECT FOR UPDATE   → primary pool
      Inside BEGIN        → primary pool (pinned for transaction)
6.  Pool checks out idle backend conn, waits when at capacity (up to `pool_acquire_timeout_seconds`), or dials up to `pool_size`
7.  Query forwarded verbatim to backend
8.  All response messages streamed back until ReadyForQuery
9.  ReadyForQuery status byte updates transaction tracking
10. Backend connection returned to pool
```

### Transaction-mode pooling

A backend connection is held only for the duration of one request/response cycle:

```
100 app connections  →  proxy  →  10 Postgres connections
```

> ⚠️  Session-level features (`SET LOCAL`, advisory locks, `LISTEN/NOTIFY`, prepared statements across requests) are not safe with transaction-mode pooling.

---

## Load Testing

```bash
# Baseline — direct to Postgres
pgbench -h localhost -p 5432 -U postgres -c 50 -T 30 postgres

# Through the proxy
pgbench -h localhost -p 5433 -U postgres -c 50 -T 30 postgres
```

---

## Running Tests

```bash
go test ./...
```

---

## Limitations

| Feature | Notes |
|---|---|
| Client authentication | Proxy accepts all inbound connections without verifying credentials |
| TLS | No TLS support on either side |
| Prepared statements | Extended query protocol always routed to primary |
| `COPY` streaming | Not handled |
| Cancel requests | `CancelRequest` messages use a separate backend connection |
| Prometheus format | `/metrics` returns JSON, not Prometheus exposition format |
