#!/usr/bin/env bash
# Kubernetes resilience + autoscaling demo against the local kind cluster (API on localhost:8085).
#   A) rolling restart of orchestrator + api AND a deleted worker pod, during live traffic
#   B) sustained load -> HorizontalPodAutoscaler scales workers
set -uo pipefail
cd "$(dirname "$0")/.."
API=http://localhost:8085/api/v1
K="kubectl -n flowforge"

sql() { docker exec ff-postgres psql -U flowforge -d flowforge -tAc "$1"; }

report() {  # $1 = workflow name
  local n="$1"
  for _ in $(seq 1 120); do
    [ "$(sql "SELECT count(*) FROM workflow_runs WHERE name='$n' AND status='RUNNING'")" = "0" ] && break
    sleep 1
  done
  local ev="workflow_events e JOIN workflow_runs r USING (workflow_id) WHERE r.name='$n'"
  echo "statuses            : $(sql "SELECT string_agg(status||'='||c,' ') FROM (SELECT status,count(*) c FROM workflow_runs WHERE name='$n' GROUP BY status) s")"
  echo "tasks succeeded     : $(sql "SELECT count(*) FROM $ev AND e.event_type='TaskSucceeded'")"
  echo "succeeded twice     : $(sql "SELECT count(*) FROM (SELECT e.workflow_id, e.payload->>'taskId' FROM $ev AND e.event_type='TaskSucceeded' GROUP BY 1,2 HAVING count(*)>1) d")"
  echo "retried attempts    : $(sql "SELECT count(*) FROM $ev AND e.event_type='TaskFailed'")"
  echo "retry reasons       : $(sql "SELECT coalesce(string_agg(r2||' x'||c, '; '),'none') FROM (SELECT left(e.payload->>'error',45) r2, count(*) c FROM $ev AND e.event_type='TaskFailed' GROUP BY 1) x")"
}

echo "################ A) rolling restart + pod failure under load ################"
NAME="k8s-rolling-$(date +%s)"
python3 loadtest/submit.py --api "$API" --file examples/chain.json --count 300 --name "$NAME"
sleep 2
echo "-> rolling restart: orchestrator + api"; $K rollout restart deployment/orchestrator deployment/api
VICTIM=$($K get pod -l app=worker -o name | head -1)
echo "-> deleting $VICTIM (simulated pod failure)"; $K delete "$VICTIM" --wait=false
$K rollout status deployment/orchestrator --timeout=180s
$K rollout status deployment/api --timeout=180s
$K rollout status deployment/worker --timeout=180s
report "$NAME"
$K get pods
echo "pods that restarted: $($K get pods --no-headers | awk '$4 != "0"' | wc -l | tr -d ' ')"
echo "Postgres connections in use: $(sql "SELECT count(*) FROM pg_stat_activity") of $(sql "SHOW max_connections")"

echo; echo "################ B) autoscaling under sustained load ################"
hpa() { $K get hpa worker -o jsonpath='{.status.currentReplicas} {.status.currentMetrics[0].resource.current.averageUtilization}'; }
echo "waiting for workers to scale back down to the minimum (2) so the scale-up is visible..."
for _ in $(seq 1 36); do set -- $(hpa); [ "${1:-0}" = "2" ] && break; sleep 5; done
echo "start: replicas=$(hpa | cut -d' ' -f1) cpu=$(hpa | cut -d' ' -f2)%"
python3 loadtest/load_test.py --api "$API" --workflows 2000 --width 20 --label k8s-hpa > /tmp/k8s-hpa.txt 2>&1 &
LOAD=$!
for i in $(seq 1 18); do
  sleep 10
  set -- $(hpa 2>/dev/null); echo "t+$((i*10))s  replicas=${1:-?}  cpu=${2:-?}% of request (target 60%)"
  kill -0 $LOAD 2>/dev/null || break
done
wait $LOAD
grep -E "statuses|succeeded|duplicate|THROUGHPUT" /tmp/k8s-hpa.txt
echo "--- autoscaler decisions ---"
$K describe hpa worker | sed -n '/Events:/,$p' | tail -6
