#!/bin/sh
set -eu
export COMPOSE_PROFILES=core
project=swipeline-m4-demo-$(date +%s)-$$
export SWIPELINE_HTTP_PORT=0 SWIPELINE_PROM_PORT=0 SWIPELINE_GATEWAY_PORT=0 SWIPELINE_FLINK_PORT=0 SWIPELINE_SPARK_BATCH_PORT=0 SWIPELINE_SEAWEEDFS_PORT=0 SWIPELINE_SEAWEEDFS_S3_PORT=0
work=$(mktemp -d)

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

COMPOSE_PROFILES=core docker compose -p "$project" up -d --build --wait

# Run 100-event replay with lifecycle events
run_id="m4-demo-$(date +%s)"
docker compose -p "$project" exec -T replayer /swipeline replay --events 100 --run-id "$run_id" --gateway http://gateway:8081 --tps 25

# Execute batch worker to ingest and seal batch
docker compose -p "$project" exec -T spark-batch python3 -m batch.worker --once

docker run --rm --network "${project}_default" --user "$(id -u):$(id -g)" \
	-e GOCACHE=/tmp/go-cache -e GOMODCACHE=/tmp/go-mod -e GOFLAGS=-buildvcs=false \
	-e SWIPELINE_TEST_DB="postgres://gateway:gateway-local@postgres:5432/gateway?sslmode=disable" \
	-e SWIPELINE_TEST_GATEWAY="http://gateway:8081" \
	-e SWIPELINE_KAFKA="kafka:9092" \
	-e SWIPELINE_SPARK="http://spark-batch:8080" \
	-e SWIPELINE_RUN_ID="$run_id" \
	-v "$PWD:/src:ro,z" -w /src golang:1.27.2 \
	sh -ec '
		go test ./tests -run '\''^TestInstalledLifecycle'\'' -count=1 -v
	'
