#!/bin/sh
set -eu

profile="${1:-full}"
meminfo="${SWIPELINE_MEMINFO:-/proc/meminfo}"

if [ ! -f "$meminfo" ]; then
    echo "meminfo not found: $meminfo" >&2
    exit 1
fi

avail_kb=$(awk '/^MemAvailable:/ {print $2}' "$meminfo")
if [ -z "$avail_kb" ]; then
    echo "MemAvailable not found in $meminfo" >&2
    exit 1
fi
avail_mib=$(( avail_kb / 1024 ))

config_json=$(COMPOSE_PROFILES="$profile" docker compose config --format json)

total_bytes=$(echo "$config_json" | awk '
{
    while (match($0, /"mem_limit"[ \t]*:[ \t]*"?[0-9]+/)) {
        s = substr($0, RSTART, RLENGTH)
        gsub(/[^0-9]/, "", s)
        sum += s
        $0 = substr($0, RSTART + RLENGTH)
    }
}
END {
    printf "%.0f", sum
}
')

total_mib=$(( total_bytes / 1048576 ))
required_mib=$(( total_mib + 1536 ))

echo "required=${required_mib} available=${avail_mib}"

if [ "$avail_mib" -lt "$required_mib" ]; then
    exit 1
fi
