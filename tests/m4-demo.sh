#!/bin/sh
set -eu
export COMPOSE_PROFILES=core
project=swipeline-m4-demo-$(date +%s)-$$
export SWIPELINE_HTTP_PORT=0 SWIPELINE_PROM_PORT=0 SWIPELINE_GATEWAY_PORT=0 SWIPELINE_FLINK_PORT=0 SWIPELINE_SPARK_BATCH_PORT=0 SWIPELINE_SEAWEED_PORT=0
work=$(mktemp -d)
mkdir -p "$work/go-cache" "$work/go-mod"

cleanup() {
	rc=$?
	trap - EXIT INT TERM
	if [ "$rc" -ne 0 ]; then
		echo "=== GATEWAY LOGS ==="
		COMPOSE_PROFILES=core docker compose -p "$project" logs --no-color gateway || true
		echo "=== SPARK-BATCH LOGS ==="
		COMPOSE_PROFILES=core docker compose -p "$project" logs --no-color spark-batch || true
		echo "=== SEAWEEDFS LOGS ==="
		COMPOSE_PROFILES=core docker compose -p "$project" logs --no-color seaweedfs || true
	fi
	COMPOSE_PROFILES=core docker compose -p "$project" down -v --remove-orphans
	rm -rf "$work"
	exit "$rc"
}
trap cleanup EXIT INT TERM

if ! docker image inspect swipeline-spark:m4 >/dev/null 2>&1; then
	docker build -f deploy/spark.Dockerfile -t swipeline-spark:m4 .
fi
if ! docker image inspect swipeline-streaming:m2 >/dev/null 2>&1; then
	docker build -f deploy/streaming.Dockerfile -t swipeline-streaming:m2 .
fi

COMPOSE_PROFILES=core docker compose -p "$project" up -d --build --wait --wait-timeout 240 postgres kafka gateway replayer prometheus flink-jobmanager flink-taskmanager seaweedfs spark-batch

run_tests() {
 docker run --rm --memory=1024m --cpus=2 --network "${project}_default" --user "$(id -u):$(id -g)" \
  -e GOCACHE=/tmp/go-cache -e GOMODCACHE=/tmp/go-mod -e GOFLAGS=-buildvcs=false \
  -e SWIPELINE_TEST_DB="postgres://gateway:gateway-local@postgres:5432/gateway?sslmode=disable" \
  -e SWIPELINE_TEST_GATEWAY="http://gateway:8081" \
  -e SWIPELINE_KAFKA="kafka:9092" -e SWIPELINE_SPARK="http://spark-batch:8080" \
  -e SWIPELINE_FIXTURES=/fixtures \
  -v "$work/go-cache:/tmp/go-cache:z" -v "$work/go-mod:/tmp/go-mod:z" -v "$work:/fixtures:z" -v "$PWD:/src:ro,z" -w /src golang:1.27.2 \
  sh -ec 'if [ -f /fixtures/ids.env ]; then set -a; . /fixtures/ids.env; set +a; fi; go test -p 1 ./tests -run "$1" -count=1 -v' sh "$1"
}
run_tests '^TestInstalledLifecyclePrepare$'
# Restart the actual installed worker and assert complete snapshots retain IDs,
# hashes, versions, posting dates and batch assignments.
docker compose -p "$project" restart spark-batch
docker compose -p "$project" up -d --wait --wait-timeout 240 spark-batch
run_tests '^TestInstalledLifecycle(Clearing|OutOfOrder|ReversalsAndChargebacks|Stability|InspectionLock)$'
