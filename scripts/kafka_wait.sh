#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Retry readiness checks before creating topics and starting workers.
for attempt in {1..60}; do
  if docker compose exec -T kafka1 kafka-topics --bootstrap-server kafka1:9092 --list >/dev/null 2>&1 &&
     docker compose exec -T kafka2 kafka-topics --bootstrap-server kafka2:9092 --list >/dev/null 2>&1 &&
     docker compose exec -T schema-registry curl --max-time 3 -fsS http://localhost:8081/subjects >/dev/null 2>&1 &&
     docker compose exec -T schema-registry curl --max-time 3 -fsS http://kafka-ui:8080/actuator/health >/dev/null 2>&1; then
    echo "Kafka brokers, Schema Registry and Kafka UI are ready"
    exit 0
  fi
  echo "Waiting for infrastructure: attempt $attempt/60"
  sleep 2
done

echo "Infrastructure did not become ready; check docker compose logs" >&2
exit 1
