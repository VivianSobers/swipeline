#!/bin/sh
set -eu

dump_logs() {
	if [ -n "${1:-}" ]; then
		set -- -p "$1"
	else
		set --
	fi
	docker compose "$@" logs --no-color gateway || true
	docker compose "$@" logs --no-color postgres || true
	services=$(docker compose "$@" config --services 2>/dev/null || true)
	if [ -z "$services" ]; then
		services="flink-jobmanager flink-taskmanager kafka prometheus replayer"
	fi
	for s in $services; do
		if [ "$s" != "gateway" ] && [ "$s" != "postgres" ]; then
			docker compose "$@" logs --no-color --tail 100 "$s" || true
		fi
	done
}

if [ "${1:-}" != "--source-only" ] && [ $# -gt 0 ]; then
	dump_logs "$@"
fi
