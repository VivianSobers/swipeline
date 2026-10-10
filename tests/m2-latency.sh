#!/bin/sh
set -eu
project=swipeline-m2-latency-$(date +%s)-$$
export SWIPELINE_HTTP_PORT=0 SWIPELINE_PROM_PORT=0 SWIPELINE_GATEWAY_PORT=0 SWIPELINE_FLINK_PORT=0
. "$(dirname "$0")/dump-logs.sh"
cleanup() {
 rc=$?
 trap - EXIT
 if [ "$rc" -ne 0 ]; then dump_logs "$project"; fi
 docker compose -p "$project" down -v --remove-orphans
 exit "$rc"
}
trap cleanup EXIT
measure() {
 docker run --rm --memory=768m --cpus=2 --network "${project}_default" --user "$(id -u):$(id -g)"   -e SWIPELINE_ENFORCE_BUDGETS="${SWIPELINE_ENFORCE_BUDGETS:-1}" -e GOMEMLIMIT=512MiB -e GOCACHE=/tmp/go-cache -e GOMODCACHE=/tmp/go-mod -e GOFLAGS=-buildvcs=false   -e SWIPELINE_TEST_GATEWAY=http://gateway:8081 -e SWIPELINE_TEST_FLINK=http://flink-jobmanager:8081   -e SWIPELINE_EXPECT_BUFFER_MS="$SWIPELINE_AUTH_BUFFER_TIMEOUT_MS"   -v "$PWD:/src:ro,z" -w /src golang:1.27.2   go test ./tests -run '^TestInstalledSparseLatency$' -count=1 -v
}
export SWIPELINE_AUTH_BUFFER_TIMEOUT_MS=0 SWIPELINE_FLINK_PROPERTIES=
docker compose -p "$project" up -d --build --wait
measure
docker compose -p "$project" stop flink-taskmanager flink-jobmanager
export SWIPELINE_AUTH_BUFFER_TIMEOUT_MS=100
export SWIPELINE_FLINK_PROPERTIES='execution.buffer-timeout.interval: 100 ms'
docker compose -p "$project" up -d --wait --wait-timeout 180
measure
