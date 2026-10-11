#!/bin/sh
set -eu
project=swipeline-m2-demo-$(date +%s)-$$
export SWIPELINE_HTTP_PORT=0 SWIPELINE_PROM_PORT=0 SWIPELINE_GATEWAY_PORT=0 SWIPELINE_FLINK_PORT=0
. "$(dirname "$0")/dump-logs.sh"
work=$(mktemp -d)
worker=
cleanup() {
 rc=$?
 trap - EXIT
 if [ -n "$worker" ]; then kill "$worker" 2>/dev/null || true; wait "$worker" 2>/dev/null || true; fi
 if [ "$rc" -ne 0 ]; then dump_logs "$project"; fi
 docker compose -p "$project" down -v --remove-orphans
 rm -rf "$work"
 exit "$rc"
}
trap cleanup EXIT
ready() {
 for attempt in $(seq 1 60); do
  if docker compose -p "$project" exec -T flink-jobmanager curl -fsS --max-time 2 http://gateway:8081/readyz; then return 0; fi
  sleep 1
 done
 return 1
}
docker compose -p "$project" build gateway replayer
docker compose -p "$project" up -d --wait
ready
(
 while :; do
  if [ ! -f "$work/request" ]; then sleep 0.1; continue; fi
  read -r id action < "$work/request"
  rm "$work/request"
  rc=0
  case "$action" in
   stop-taskmanager) docker compose -p "$project" stop flink-taskmanager || rc=$? ;;
   start-taskmanager) docker compose -p "$project" up -d --wait --wait-timeout 180 || rc=$? ;;
   kill-gateway) docker compose -p "$project" kill -s SIGKILL gateway || rc=$? ;;
   start-gateway) docker compose -p "$project" up -d --wait --wait-timeout 180 && ready || rc=$? ;;
   restart-flink) docker compose -p "$project" restart flink-jobmanager flink-taskmanager && docker compose -p "$project" up -d --wait --wait-timeout 180 || rc=$? ;;
   *) rc=2 ;;
  esac
  printf '%s %s\n' "$id" "$rc" > "$work/result"
  mv "$work/result" "$work/done"
 done
) &
worker=$!
docker run --rm --memory=768m --cpus=2 --network "${project}_default" --user "$(id -u):$(id -g)" \
 -e SWIPELINE_ENFORCE_BUDGETS="${SWIPELINE_ENFORCE_BUDGETS:-1}" -e GOMEMLIMIT=512MiB -e GOCACHE=/tmp/go-cache -e GOMODCACHE=/tmp/go-mod -e GOFLAGS=-buildvcs=false \
 -e SWIPELINE_TEST_DB=postgres://gateway:gateway-local@postgres:5432/gateway?sslmode=disable -e SWIPELINE_TEST_GATEWAY=http://gateway:8081 -e SWIPELINE_KAFKA=kafka:9092 -e SWIPELINE_FLINK=http://flink-jobmanager:8081 -e SWIPELINE_FAULT_DIR=/faults \
 -v "$PWD:/src:ro,z" -v "$work:/faults:z" -w /src golang:1.27.2 \
 sh -ec '
  go test ./tests -run '\''^(TestInstalledAuthorization|TestLateProposalAfterStandIn|TestGatewayRestartRecovers)$'\'' -count=1 -v
  go test ./tests -run '\''^TestCanonical'\'' -count=1 -v
 '
