#!/bin/bash
# Resource sampler for benchmark runs (run inside the database server as root, stop with SIGTERM):
# peak container memory, peak staging size (pgdm_xs_* schemas), peak pgsql_tmp spill files, WAL size.
# usage: monitor.sh <output-file> [database]
out=$1; db=${2:-kaggle_challenge}; pgdata=${PGDATA:-/var/lib/postgresql/data}
peak_mem=0; peak_anon=0; peak_stage=0; peak_tmp=0; peak_wal=0
trap 'printf "peak_memory_incl_page_cache_bytes=%s\npeak_anon_memory_bytes=%s\npeak_staging_bytes=%s\npeak_pgsql_tmp_bytes=%s\npeak_wal_bytes=%s\n" "$peak_mem" "$peak_anon" "$peak_stage" "$peak_tmp" "$peak_wal" > "$out"; exit 0' TERM INT
while true; do
  m=$(cat /sys/fs/cgroup/memory.current 2>/dev/null || echo 0)
  # anon = process memory (backends' private memory, psql, ...); shared buffers are shmem, counted separately
  a=$(awk '$1=="anon"{print $2}' /sys/fs/cgroup/memory.stat 2>/dev/null); a=${a:-0}
  [ "$a" -gt "$peak_anon" ] && peak_anon=$a
  s=$(su postgres -c "psql -X -qAt -d $db -c \"select coalesce(sum(pg_total_relation_size(c.oid)),0) from pg_class c join pg_namespace n on n.oid=c.relnamespace where n.nspname like 'pgdm\\_xs\\_%' and c.relkind in ('r','m')\"" 2>/dev/null || echo 0)
  t=$(du -sb $pgdata/base/pgsql_tmp 2>/dev/null | cut -f1); t=${t:-0}
  w=$(du -sb $pgdata/pg_wal 2>/dev/null | cut -f1); w=${w:-0}
  [ "$m" -gt "$peak_mem" ] && peak_mem=$m; [ "${s:-0}" -gt "$peak_stage" ] && peak_stage=$s
  [ "$t" -gt "$peak_tmp" ] && peak_tmp=$t; [ "$w" -gt "$peak_wal" ] && peak_wal=$w
  sleep 0.5
done
