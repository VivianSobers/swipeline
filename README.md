# swipeline

swipeline is the data platform of a card payment network that you run on one machine. This first release generates a realistic, repeatable day of card transactions at a rate you choose and shows the replay in Prometheus. Later releases add real-time approve/decline decisions, a transaction ledger, nightly settlement between banks and live dashboards.

This release writes synthetic transaction requests to local JSON Lines files. It does not approve or decline payments yet.

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

Cards are synthetic tokens, never usable card numbers. Each block of 1,000 events contains a short velocity burst on one card and a card-testing burst of 80 distinct cards at one merchant, so later releases have realistic fraud patterns to detect.

`docker compose down` keeps your data. Nothing in this repository deletes the normal volumes.

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
