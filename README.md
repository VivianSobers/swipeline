# swipeline

swipeline is the data platform of a card payment network that you run on one machine. It generates repeatable synthetic card traffic, returns durable approve/decline decisions, and exposes replay and authorization metrics in Prometheus. A transaction ledger, settlement and analytics dashboards are planned for later milestones.

The gateway stores admissions and final decisions in PostgreSQL. Flink scores requests through Kafka. If scoring misses its deadline, the gateway stores a fail-closed stand-in decline. A late proposal cannot change that outcome. File previews remain available with `--preview`.

## Install and run

You need Docker (no sudo) and Docker Compose. Everything else, including the Go toolchain, runs in containers. No datasets are downloaded.

```sh
docker compose up -d --wait
docker compose exec replayer /swipeline replay --preview --seed 42 --tps 50 --events 6000 --business-date 2026-10-08 --run-id demo-42 --output first
docker compose exec replayer /swipeline status
```

The replay command prints a JSON summary when it finishes: offered, admitted and completed counts, the achieved rate, scheduler lag and peak outstanding work. At 50 events per second, 6,000 events take about two minutes, so one business day plays back 720 times faster than real time. "Admitted" here means accepted into the replay queue, not approved by a payment network.

Each completed replay is available over HTTP:

- `http://localhost:8080/v1/previews/first` returns the events as JSON Lines.
- `http://localhost:8080/v1/previews/first/manifest` returns the run's configuration and progress.

Open Prometheus at `http://localhost:9090` and try:

```promql
rate(swipeline_replayer_completed_total[1m])
swipeline_replayer_achieved_tps
swipeline_replayer_in_flight
swipeline_replayer_scheduler_lag_seconds
```

Counters add up across replays and reset when the service restarts. The rate gauges describe the current or most recent run, and `swipeline_replayer_active` tells you whether one is running. No transaction, card or merchant identifier is ever used as a metric label.

Both services listen on localhost only. Set `SWIPELINE_HTTP_PORT` or `SWIPELINE_PROM_PORT` before `docker compose up` to use other host ports. Each service is limited to 256 MiB of memory.

## Repeat a business day

```sh
docker compose exec replayer /swipeline replay --preview --seed 42 --tps 50 --events 6000 --business-date 2026-10-08 --run-id demo-42 --output second --workers 1
```

`first` and `second` contain the same transactions in the same order, with identical transaction IDs, event IDs and payload hashes. Only the production timestamps and measured rates differ. The rate, worker count and queue size never change what is generated.

A few rules keep replays safe:

- An output name can't be reused, so a finished replay is never overwritten.
- A run ID is tied to its seed, day and event count. To generate different transactions, pick a new run ID.
- `--tps` must be positive, `--events` is limited to 100,000 per replay, and `--workers` can't exceed `--queue`. The queue bounds all outstanding work.
- If a replay is cancelled or a write fails, nothing is published. A crash can leave a hidden temporary directory behind; completed replays survive restarts.

Cards are synthetic tokens, never usable card numbers. Each block of 1,000 events contains a short velocity burst on one card and a card-testing burst of 80 distinct cards at one merchant, which the authorization rules can detect.

`docker compose down` keeps your data. Nothing in this repository deletes the normal volumes.

## Authorize transactions

The latency and stand-in budgets are enforced on the four-core reference machine; CI runners with two cores check behavior and report the measurements.

Start the core stack and replay through the gateway:

```sh
docker compose up -d --build --wait
docker compose exec replayer /swipeline replay --seed 42 --tps 50 --events 1000 --business-date 2026-10-08 --run-id auth-demo-42 --deadline-mode latency --workers 1 --lanes 128
curl http://localhost:8081/v1/runs/auth-demo-42
```

Latency mode allows 35 ms for scoring and 50 ms for the response. Throughput mode uses 80 ms and 100 ms. These are budgets, not guarantees during storage or operating-system stalls. A final approval or decline returns HTTP 200. An uncertain response requires retrying or querying the same transaction ID.

Read a stored final from the demo run:

```sh
id=$(docker compose exec -T postgres psql -U gateway -d gateway -Atc "SELECT transaction_id FROM authorizations WHERE run_id='auth-demo-42' ORDER BY transaction_id LIMIT 1")
curl "http://localhost:8081/v1/authorizations/$id"
```

Repeat the replay command unchanged to resume after an interruption. The same run preserves transaction IDs and stored outcomes. Use a new run ID to change its configuration. After a gateway restart, pending admissions recover to durable finals. A Flink restart restores its latest complete checkpoint.

The gateway exposes `http://localhost:8081/metrics`, which Prometheus scrapes. In Prometheus, inspect response p99 and the stand-in share:

```promql
histogram_quantile(0.99, sum by (le, deadline_mode) (rate(swipeline_gateway_budget_duration_seconds_bucket[2m])))
sum(rate(swipeline_gateway_standin_total[2m])) / clamp_min(sum(rate(swipeline_gateway_final_decisions_total[2m])), 0.001)
```

The latency target is 50 ms and the stand-in share must stay below 1%. Also inspect `swipeline_gateway_pending_authorizations`, `swipeline_gateway_outbox_unsent`, and `swipeline_gateway_database_up`. Alert rules and notifications are not provisioned in this milestone; these queries provide the observations for latency and stand-in alerts.

Run the installed checks:

```sh
make m2-demo
make m2-latency
```

Each target creates a disposable project, uses random host ports, and removes its own volumes on exit. The demo checks approvals, declines, card testing, late proposals and gateway crash recovery. The latency target measures 1,000 requests at 20 TPS after warm-up. It prints the effective buffer timeout from the running job and both gate verdicts. The default is 0 ms. Its 100 ms negative control must fail the gate. `SWIPELINE_AUTH_BUFFER_TIMEOUT_MS` selects the explicit buffer override; leave it unset for normal operation.

Measured on 2026-10-10 on this laptop:

| Target | Result |
| --- | --- |
| `make m2-demo`, 50 TPS | 1,000 completed; 967 approvals; 33 declines; 4 stand-ins (0.400%) |
| Sparse run, effective 0 ms buffer | Passed; p99 19.958592 ms; stand-ins 0.200% |
| Negative control, effective 100 ms buffer | Rejected; p99 49.844827 ms; stand-ins 85.400% |

These are observations from those runs. The stack has a 4 GiB steady memory budget. The acceptance container adds a 768 MiB limit. Keep one installed acceptance target running at a time.

## Watch the network

The full profile adds Druid, ZooKeeper, Superset and an idempotent bootstrap service to ingest canonical decisions and display them on an operational dashboard.

### Capacity and preflight

The full profile requires sufficient host memory before starting. Run the preflight check:

```sh
tests/preflight.sh full
```

The preflight script sums the memory limits of all rendered services plus a 1,536 MiB buffer and compares the total against `MemAvailable` in `/proc/meminfo`. If available memory is insufficient, it refuses startup and reports required versus available memory. The full profile is sized for the four-core reference server with 15 GiB of RAM; standard laptops and 2-core CI runners are not expected to fit.

### Start the full profile

```sh
COMPOSE_PROFILES=full docker compose up -d --wait
```

The bootstrap container registers the Kafka ingestion supervisor with Druid and waits until ingestion state reaches `RUNNING`. Superset initializes its metadata database, creates an administrative user with a generated password stored in the named `superset-secrets` volume, and imports the payments operations dashboard.

Print the generated Superset password:

```sh
make superset-password
```

Open Superset at `http://localhost:8088` (or the port set by `SWIPELINE_SUPERSET_PORT`) and log in with username `admin` and the printed password. Navigate to Dashboards and select `payments-operations`. The Druid router console is published at `http://localhost:8888` (or `SWIPELINE_DRUID_PORT`).

### Dashboard panels and timing boundaries

The Payments Operations dashboard provides real-time visibility into transaction outcomes:

- **Throughput (TPS)**: Total final decisions bucketed by 10-second intervals (`TIME_FLOOR(__time, 'PT10S') / 10.0`). Visibility lags the transaction by up to one Flink checkpoint interval (10 s) plus processing.
- **Approval Rate**: Proportion of approved final decisions over total final decisions, including stand-ins. If total decisions is zero, the panel reports null.
- **Decline Reasons**: Distribution of decline counts grouped by decline reason and decision source (`FLINK` vs `STAND_IN`).
- **Fraud Flags by MCC**: Transactions flagged for `CARD_TESTING`, `CARD_VELOCITY`, or `SPEND_ANOMALY` grouped by merchant category code (`mcc`). Because multiple flags may apply to a single transaction, flag counts can overlap.
- **Decision Latency**: Latency histogram buckets (0-5, 5-10, 10-20, 20-35, 35-50, 50+ ms) with mean and maximum. This panel measures the duration from admission to final decision choice, excluding the database commit; Prometheus remains the authority for the 50 ms and 100 ms response targets.
- **Freshness and Integrity**: Tracks `MAX(__time)`, ingestion lag in seconds (`EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) - EXTRACT(EPOCH FROM MAX(__time))`), total ingested row count, and the stand-in share.

### Reconcile decisions

To verify that every gateway final decision reached Druid without duplication or loss, run the reconciliation command:

```sh
docker compose exec replayer /swipeline reconcile --run-id demo-42 --gateway http://gateway:8081 --druid http://druid-router:8888 --wait 2m
```

The tool compares the gateway's final decision count from PostgreSQL against Druid's total row count and distinct transaction count. It polls until the gateway has zero pending decisions and Druid row count equals both distinct transactions and gateway finals. If row count exceeds distinct transactions, it reports duplicate rows and exits non-zero.

### Measured results

Full acceptance runs via `make m3-demo` on the four-core reference server:

| Metric | Result |
| --- | --- |
| Completed decisions | (to be recorded after server run) |
| Approvals | (to be recorded after server run) |
| Declines | (to be recorded after server run) |
| Stand-in share | (to be recorded after server run) |
| Ingestion freshness lag | (to be recorded after server run) |
| Reconciliation status | (to be recorded after server run) |

### Analytics limits

Operational limits for network analytics:

- Canonical decision visibility lags transactions by up to one checkpoint interval (10 s) plus processing.
- Druid exactly-once relies on intact supervisor offsets in Kafka and metadata storage.
- Never reset offsets on a populated datasource (rebuild into a fresh datasource instead). You must never reset offsets on an active datasource.
- The full profile does not fit the 14 GB laptop or standard two-core CI runners; run full acceptance on the four-core reference server.

## Develop and check

```sh
make fmt           # format Go code
make check         # gofmt, go vet and race-enabled tests
make build         # compile every package
make deploy-check  # validate the Compose file and Prometheus config
make demo          # start a throwaway stack and prove two replays match
```

Checks run in the pinned `golang:1.27.2` container. `make demo` uses its own Compose project on random ports and removes only that project's volumes when it exits.

| Path | Contents |
| --- | --- |
| `cmd/replayer` | The `swipeline` service and command-line client |
| `internal/contract` | Event envelope, deterministic IDs and payload hashes |
| `internal/generator` | Seeded synthetic card-day generator |
| `internal/replay` | Rate limiting and the bounded, ordered replay pipeline |
| `internal/preview` | HTTP service, atomic file publication and metrics |
| `schemas` | JSON Schemas for the event format |
| `deploy` | Container image and Prometheus configuration |
| `tests` | Deployment checks and the installed end-to-end demo |

## License

Apache License 2.0. See [LICENSE](LICENSE).
