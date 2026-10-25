#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose exec -T kafka1 kafka-topics --create --if-not-exists \
  --topic orders --partitions 3 --replication-factor 2 \
  --config min.insync.replicas=2 --bootstrap-server kafka1:9092

# Apply the durability setting to an existing topic as well.
docker compose exec -T kafka1 kafka-configs --alter \
  --entity-type topics --entity-name orders --add-config min.insync.replicas=2 \
  --bootstrap-server kafka1:9092

docker compose exec -T kafka1 kafka-topics --describe \
  --topic orders --bootstrap-server kafka1:9092
