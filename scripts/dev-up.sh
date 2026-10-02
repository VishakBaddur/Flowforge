#!/usr/bin/env bash
# Starts orchestrator, worker and api in the background (logs in ./logs) and waits until all are ready.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p logs
pkill -f 'flowforge-.*-SNAPSHOT.jar' || true
sleep 1
for svc in orchestrator worker api; do
  nohup java -jar "flowforge-$svc/target/flowforge-$svc-0.1.0-SNAPSHOT.jar" > "logs/$svc.log" 2>&1 &
  echo "started $svc (pid $!)"
done
for _ in $(seq 1 45); do
  if grep -q "Application run failed" logs/*.log 2>/dev/null; then
    echo "FAILED to start:"; grep -l "Application run failed" logs/*.log; exit 1
  fi
  if grep -q "Recovered" logs/orchestrator.log && grep -q "partitions assigned" logs/worker.log \
     && grep -q "Started ApiApplication" logs/api.log; then
    echo "all services ready"; exit 0
  fi
  sleep 1
done
echo "timed out waiting for services; check logs/"; exit 1
