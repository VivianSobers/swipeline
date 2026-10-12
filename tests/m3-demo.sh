#!/bin/sh
set -eu
export COMPOSE_PROFILES=full
project=swipeline-m3-demo-$(date +%s)-$$
export SWIPELINE_HTTP_PORT=0 SWIPELINE_PROM_PORT=0 SWIPELINE_GATEWAY_PORT=0 SWIPELINE_FLINK_PORT=0 SWIPELINE_DRUID_PORT=0 SWIPELINE_SUPERSET_PORT=0
work=$(mktemp -d)
worker=
mkdir -p "$work/go-cache" "$work/go-mod"

cleanup() {
	rc=$?
	trap - EXIT INT TERM
	if [ -n "$worker" ]; then kill "$worker" 2>/dev/null || true; wait "$worker" 2>/dev/null || true; fi
	if [ "$rc" -ne 0 ]; then
		echo "=== GATEWAY LOGS ==="
		COMPOSE_PROFILES=full docker compose -p "$project" logs --no-color gateway || true
		echo "=== DRUID LOGS ==="
		COMPOSE_PROFILES=full docker compose -p "$project" logs --no-color druid-coordinator-overlord druid-broker druid-historical druid-middlemanager druid-router || true
		echo "=== SUPERSET LOGS ==="
		COMPOSE_PROFILES=full docker compose -p "$project" logs --no-color superset || true
	fi
	COMPOSE_PROFILES=full docker compose -p "$project" down -v --remove-orphans
	chmod -R u+w "$work/go-mod"
	rm -rf "$work"
	exit "$rc"
}
trap cleanup EXIT INT TERM

docker build -f deploy/superset.Dockerfile -t swipeline-superset:m3 .
if ! docker image inspect swipeline-streaming:m2 >/dev/null 2>&1; then
	docker build -f deploy/streaming.Dockerfile -t swipeline-streaming:m2 .
fi

COMPOSE_PROFILES=full docker compose -p "$project" up -d --build --wait postgres kafka gateway replayer prometheus flink-jobmanager flink-taskmanager druid-zk druid-coordinator-overlord druid-broker druid-historical druid-middlemanager druid-router superset

# Background fault worker for fault injection during tests
(
	while :; do
		if [ ! -f "$work/request" ]; then sleep 0.1; continue; fi
		read -r id action < "$work/request"
		rm "$work/request"
		rc=0
		case "$action" in
			restart-middlemanager)
				COMPOSE_PROFILES=full docker compose -p "$project" restart druid-middlemanager && COMPOSE_PROFILES=full docker compose -p "$project" up -d --wait --wait-timeout 180 postgres kafka gateway replayer prometheus flink-jobmanager flink-taskmanager druid-zk druid-coordinator-overlord druid-broker druid-historical druid-middlemanager druid-router superset || rc=$?
				;;
			*) rc=2 ;;
		esac
		printf '%s %s\n' "$id" "$rc" > "$work/result"
		mv "$work/result" "$work/done"
	done
) &
worker=$!

# Compile before replay so toolchain setup cannot consume the freshness budget.
docker run --rm --memory=1024m --cpus=2 --user "$(id -u):$(id -g)" \
 -e GOCACHE=/tmp/go-cache -e GOMODCACHE=/tmp/go-mod -e GOFLAGS=-buildvcs=false \
 -v "$work/go-cache:/tmp/go-cache:z" -v "$work/go-mod:/tmp/go-mod:z" \
 -v "$PWD:/src:ro,z" -v "$work:/faults:z" -w /src golang:1.27.2 \
 go test -c -o /faults/analytics.test ./tests

# Run 300-event warm-up
docker compose -p "$project" exec -T replayer /swipeline replay --events 300 --run-id "warm-$(date +%s)" --tps 50

# Run 1000-event run
docker compose -p "$project" exec -T replayer /swipeline replay --events 1000 --run-id installed-42 --tps 50

SUPERSET_PASSWORD=$(docker compose -p "$project" exec -T superset cat /secrets/admin-password 2>/dev/null || true)

docker run --rm --network "${project}_default" --user "$(id -u):$(id -g)" \
	-e GOCACHE=/tmp/go-cache -e GOMODCACHE=/tmp/go-mod -e GOFLAGS=-buildvcs=false \
	-e SWIPELINE_TEST_DB="postgres://gateway:gateway-local@postgres:5432/gateway?sslmode=disable" \
	-e SWIPELINE_TEST_GATEWAY="http://gateway:8081" \
	-e SWIPELINE_KAFKA="kafka:9092" \
	-e SWIPELINE_FLINK="http://flink-jobmanager:8081" \
	-e SWIPELINE_DRUID="http://druid-router:8888" \
	-e SWIPELINE_DRUID_BROKER="http://druid-broker:8082" \
	-e SWIPELINE_DRUID_COORDINATOR="http://druid-coordinator-overlord:8081" \
	-e SWIPELINE_SUPERSET="http://superset:8088" \
	-e SWIPELINE_SUPERSET_PASSWORD="$SUPERSET_PASSWORD" \
	-e SWIPELINE_RUN_ID="installed-42" \
	-e SWIPELINE_FAULT_DIR="/faults" \
	-v "$PWD:/src:ro,z" -v "$work:/faults:z" -w /src golang:1.27.2 \
	sh -ec '
		/faults/analytics.test -test.run '\''^TestInstalledAnalytics'\'' -test.count=1 -test.v
	'
