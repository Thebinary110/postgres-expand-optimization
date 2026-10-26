#!/bin/bash
# Failure-scenario validation of the Expand job on the final code.
# Every scenario starts from the same dataset snapshot, injects one failure, finishes the way a user would
# (reconnect, or run the same expansion again) and must end with destinations AND version registry identical
# to the untouched original code's result (bench/fingerprint.sql, bench/control_fingerprint.sql).
#
#   recovery_suite.sh <snapshot tag> <results dir>
# Needs the local helpers in $R/bin (ah3, ah3-orig, ah3-db, ah3-killjob) and the server container $N.
set -u
R=${R:-/home/shhaurya/Desktop/kca-data_eng/agentic-harness-tissues-ML-part-3}; N=${N:-agentic-harness-tissues-ML-part-3}
TAG=${1:-1m}; OUT=$(realpath -m "${2:-$R/results/recovery}"); mkdir -p "$OUT"
JOBS=/home/harness/postgres_data_manager_storage/expand_jobs
SUMMARY=$OUT/summary.txt; : > "$SUMMARY"
PASS=0; FAIL=0

log() { echo "[$(date +%T)] $*" | tee -a "$SUMMARY"; }
sqlc() { docker exec -u postgres $N psql -X -qtA -d "${2:-kaggle_challenge}" -c "$1"; }
load() { "$R/bin/ah3-db" load "$TAG" >/dev/null; }
fast() { "$R/bin/ah3" bench/headless_expand.py "$@"; }
fp_data() { "$R/bin/ah3-db" fp; }
fp_ctrl() { docker exec -i -u postgres $N psql -X -qtA -d postgres_data_manager < "$R/repo/bench/control_fingerprint.sql"; }
dest_rows() { sqlc "SELECT (SELECT count(*) FROM public.table1)+(SELECT count(*) FROM public.table2)+(SELECT count(*) FROM public.table3)
                     +(SELECT count(*) FROM public.table4)+(SELECT count(*) FROM public.table5)+(SELECT count(*) FROM public.table6)"; }
runners() { docker exec $N bash -c "pgrep -f 'expand_jobs/.*/[r]un.sh' | wc -l"; }
has_jobs() { [ "$(sqlc "SELECT to_regclass('pgdm_expansion.units') IS NOT NULL")" = t ]; }
units_done() { has_jobs && sqlc "SELECT count(*) FILTER (WHERE state = 'done') || '/' || count(*) FROM pgdm_expansion.units" || echo 0/0; }
jobs_state() { echo "registry=[$(sqlc "SELECT string_agg(state, ',' ORDER BY created_at) FROM app_control.expansion_jobs" postgres_data_manager)]" \
                    "data=[$(has_jobs && sqlc "SELECT string_agg(state, ',' ORDER BY created_at) FROM pgdm_expansion.jobs")]"; }
staging() { sqlc "SELECT count(*) FROM pg_namespace WHERE nspname LIKE 'pgdm_xs_%'"; }

# wait until a job's runner is alive, then sleep $1 more seconds
wait_job() {
  for _ in $(seq 1 600); do [ "$(runners)" -gt 0 ] && break; sleep 0.1; done
  sleep "$1"
}
wait_no_runner() { for _ in $(seq 1 1200); do [ "$(runners)" -eq 0 ] && return 0; sleep 0.5; done; return 1; }
wait_pg() { for _ in $(seq 1 120); do docker exec -u postgres $N pg_isready -q && sqlc "SELECT 1" >/dev/null 2>&1 && return 0; sleep 1; done; return 1; }
crash_pg() {  # SIGKILL one backend: the postmaster resets every session (crash recovery, unlogged tables emptied)
  local pid; pid=$(sqlc "SELECT pid FROM pg_stat_activity WHERE datname = 'kaggle_challenge' AND backend_type = 'client backend'
                        AND pid <> pg_backend_pid() ORDER BY application_name LIKE 'pgdm_worker%' DESC LIMIT 1")
  [ -z "$pid" ] && { docker exec -d -u postgres $N psql -X -q -d kaggle_challenge -c "SELECT pg_sleep(60)"; sleep 1
    pid=$(sqlc "SELECT pid FROM pg_stat_activity WHERE query LIKE 'SELECT pg_sleep(60)%' AND pid <> pg_backend_pid() LIMIT 1"); }
  docker exec $N kill -KILL "$pid"; sleep 2; wait_pg
}

# compare against the reference; $1 scenario, $2 data reference file, $3 control reference file
check() {
  local name=$1 ok=1 got_d got_c
  got_d=$(fp_data); got_c=$(fp_ctrl)
  echo "$got_d" > "$OUT/$name.fp"; echo "$got_c" > "$OUT/$name.cfp"
  grep -q '^table1 [1-9]' <<< "$got_d" || { ok=0; log "  $name: destinations are empty (the expansion did not run)"; }
  [ "$got_d" == "$(cat "$2")" ] || { ok=0; log "  $name: DATA DIFFERS"; diff <(echo "$got_d") "$2" | tee -a "$SUMMARY"; }
  [ "$got_c" == "$(cat "$3")" ] || { ok=0; log "  $name: VERSION REGISTRY DIFFERS"; diff <(echo "$got_c") "$3" | tee -a "$SUMMARY"; }
  [ "$(runners)" -eq 0 ] || { ok=0; log "  $name: runner still alive"; }
  [ "$(staging)" -eq 0 ] || { ok=0; log "  $name: staging schema left behind"; }
  local js; js=$(jobs_state)
  if [ $ok = 1 ]; then PASS=$((PASS + 1)); log "PASS $name ($js)"; else FAIL=$((FAIL + 1)); log "FAIL $name ($js)"; fi
}

cd "$R/repo"

log "== references: untouched original code on snapshot $TAG"
load; (cd "$R/orig" && "$R/bin/ah3-orig" bench/headless_expand.py > "$OUT/ref_orig.log" 2>&1)
fp_data > "$OUT/ref.fp"; fp_ctrl > "$OUT/ref.cfp"; log "  $(grep MEASURED "$OUT/ref_orig.log")"
grep -q '^table1 [1-9]' "$OUT/ref.fp" || { log "ABORT: the reference run produced no rows (see ref_orig.log)"; exit 1; }
# reference for S7: the same source after one more raw row is ingested (raw tables are append-only)
MUTATE="INSERT INTO public.raw_data SELECT (SELECT max(raw_id) + 1 FROM public.raw_data), raw_hash, raw_ingested_at, raw_schema,
        raw_tab, raw FROM public.raw_data WHERE raw_id = (SELECT min(raw_id) FROM public.raw_data)"
load; sqlc "$MUTATE" >/dev/null; (cd "$R/orig" && "$R/bin/ah3-orig" bench/headless_expand.py > "$OUT/ref_mut_orig.log" 2>&1)
fp_data > "$OUT/ref_mut.fp"; fp_ctrl > "$OUT/ref_mut.cfp"

log "== S0 clean run"
load; fast > "$OUT/S0.log" 2>&1; log "  $(grep MEASURED "$OUT/S0.log")"; check S0 "$OUT/ref.fp" "$OUT/ref.cfp"

log "== S1 client killed mid-execution (laptop/SSH lost); reconnect registers"
load; fast > "$OUT/S1_attempt.log" 2>&1 & c=$!; wait_job 2; kill -KILL $c; wait $c 2>/dev/null
log "  client killed at units $(units_done); waiting for the detached job"; wait_no_runner
log "  job finished on its own: $(jobs_state)"
fast --recover-only > "$OUT/S1_recover.log" 2>&1; grep -h recovered "$OUT/S1_recover.log" | tee -a "$SUMMARY"
check S1 "$OUT/ref.fp" "$OUT/ref.cfp"

log "== S2 client lost after the data commit, before registration; two clients reconnect at the same time"
load; fast --skip-register > "$OUT/S2_attempt.log" 2>&1; log "  after commit: $(jobs_state)"
fast --recover-only > "$OUT/S2_recover_a.log" 2>&1 & a=$!
fast --recover-only > "$OUT/S2_recover_b.log" 2>&1 & b=$!; wait $a $b
log "  recoveries: $(cat "$OUT"/S2_recover_?.log | grep -c recovered) (must be 1)"
fast --recover-only > "$OUT/S2_recover_c.log" 2>&1
check S2 "$OUT/ref.fp" "$OUT/ref.cfp"

for t in 1 3 5; do
  log "== S3_${t}s server-side job killed ${t}s into execution; same expansion run again resumes"
  load; fast > "$OUT/S3_${t}_attempt.log" 2>&1 & c=$!; wait_job $t; "$R/bin/ah3-killjob" | tee -a "$SUMMARY"; wait $c 2>/dev/null
  log "  after kill: units $(units_done), destination rows $(dest_rows) (must be 0), $(jobs_state)"
  fast > "$OUT/S3_${t}_resume.log" 2>&1; log "  $(grep MEASURED "$OUT/S3_${t}_resume.log")"
  check "S3_${t}s" "$OUT/ref.fp" "$OUT/ref.cfp"
done

log "== S4 user presses Cancel 3s into execution; same expansion run again"
load; fast --cancel-after 3 > "$OUT/S4_attempt.log" 2>&1; log "  attempt exit: $(tail -1 "$OUT/S4_attempt.log" | cut -c1-120)"
wait_no_runner; log "  after cancel: units $(units_done), destination rows $(dest_rows) (must be 0), $(jobs_state)"
fast > "$OUT/S4_resume.log" 2>&1; check S4 "$OUT/ref.fp" "$OUT/ref.cfp"

log "== S5 PostgreSQL crash (backend SIGKILL -> postmaster reset) 3s into execution"
load; fast > "$OUT/S5_attempt.log" 2>&1 & c=$!; wait_job 3; crash_pg; wait $c 2>/dev/null; wait_no_runner
log "  after crash: units $(units_done), destination rows $(dest_rows) (must be 0), $(jobs_state)"
fast > "$OUT/S5_resume.log" 2>&1; check S5 "$OUT/ref.fp" "$OUT/ref.cfp"

log "== S6 job killed 4s in, then PostgreSQL crash (empties checkpointed UNLOGGED staging)"
load; fast > "$OUT/S6_attempt.log" 2>&1 & c=$!; wait_job 4; "$R/bin/ah3-killjob" | tee -a "$SUMMARY"; wait $c 2>/dev/null
log "  checkpoints before crash: $(units_done)"; crash_pg
log "  after crash: units $(units_done), destination rows $(dest_rows) (must be 0)"
fast > "$OUT/S6_resume.log" 2>&1; check S6 "$OUT/ref.fp" "$OUT/ref.cfp"

log "== S7 job killed 3s in, then a raw row is ingested; the rerun must not reuse checkpoints"
load; fast > "$OUT/S7_attempt.log" 2>&1 & c=$!; wait_job 3; "$R/bin/ah3-killjob" | tee -a "$SUMMARY"; wait $c 2>/dev/null
sqlc "$MUTATE" >/dev/null; log "  source changed; units $(units_done)"
fast > "$OUT/S7_resume.log" 2>&1; check S7 "$OUT/ref_mut.fp" "$OUT/ref_mut.cfp"

log "== S8 a second client starts the same expansion while the first is running"
load; fast > "$OUT/S8_first.log" 2>&1 & c=$!; wait_job 2
fast > "$OUT/S8_second.log" 2>&1; log "  second client: $(grep -h -m1 -o 'still running[^.]*' "$OUT/S8_second.log" || tail -1 "$OUT/S8_second.log")"
wait $c; check S8 "$OUT/ref.fp" "$OUT/ref.cfp"

log "== S9 the expansion is run again on already expanded destinations (original first, then this code)"
(cd "$R/orig" && "$R/bin/ah3-orig" bench/headless_expand.py > "$OUT/S9_orig.log" 2>&1); log "  original: $(grep -h -m1 -oE 'ERROR: .{0,90}' "$OUT/S9_orig.log")"
fast > "$OUT/S9_fast.log" 2>&1; log "  this code: $(grep -h -m1 -oE 'ERROR: .{0,90}' "$OUT/S9_fast.log")"
if [ "$(fp_data)" == "$(cat "$OUT/ref.fp")" ] && [ "$(fp_ctrl)" == "$(cat "$OUT/ref.cfp")" ]; then
  PASS=$((PASS + 1)); log "PASS S9 (both refused, destinations and versions unchanged; $(jobs_state); staging schemas $(staging))"
else FAIL=$((FAIL + 1)); log "FAIL S9 (destinations or versions changed)"; fi

log "== RESULT: $PASS passed, $FAIL failed"
