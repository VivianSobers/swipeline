#!/bin/bash
# Creates the six application topics and fails, naming the topic, when an
# existing one has drifted from the fixed topology. Safe to rerun.
set -euo pipefail
# CLI tools inherit the broker's heap settings unless given their own.
export KAFKA_HEAP_OPTS=-Xmx128m
bin=/opt/kafka/bin
server=kafka:9092
names=()
declare -A retention
topic() { names+=("swipeline.$1.v1"); retention["swipeline.$1.v1"]=$2; }
topic auth.requests 86400000
topic auth.proposals 86400000
topic decisions.raw 86400000
topic decisions 86400000
topic lifecycle 86400000
topic dlq 259200000
for name in "${names[@]}"; do
  "$bin/kafka-topics.sh" --bootstrap-server "$server" --create --if-not-exists --topic "$name" \
    --partitions 4 --replication-factor 1 --config "retention.ms=${retention[$name]}" \
    --config retention.bytes=128000000 --config cleanup.policy=delete >/dev/null
done
summaries=$("$bin/kafka-topics.sh" --bootstrap-server "$server" --describe)
configs=$("$bin/kafka-configs.sh" --bootstrap-server "$server" --entity-type topics --describe)
drift=0
for name in "${names[@]}"; do
  summary=$(grep -F "Topic: $name	" <<<"$summaries" | head -1 || true)
  if [[ $summary != *"PartitionCount: 4"* || $summary != *"ReplicationFactor: 1"* ]]; then
    echo "topic $name drifted: ${summary:-missing}" >&2
    drift=1
  fi
  block=$(awk -v n="$name" '/^Dynamic configs for topic /{on=($5==n)} on' <<<"$configs")
  for want in "retention.ms=${retention[$name]} " "retention.bytes=128000000 " "cleanup.policy=delete "; do
    if [[ $block != *"$want"* ]]; then
      echo "topic $name drifted: missing ${want% }" >&2
      drift=1
    fi
  done
done
[[ $drift == 0 ]]
echo "six topics verified"
