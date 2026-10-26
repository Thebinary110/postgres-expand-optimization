#!/bin/bash
# psql wrapper: moves -h/-p/-U/-d into PG* env vars so host-side `ps` shows no database/user names.
args=()
while [ $# -gt 0 ]; do
  case "$1" in
    -h) export PGHOST="$2"; shift 2;; -p) export PGPORT="$2"; shift 2;;
    -U) export PGUSER="$2"; shift 2;; -d) export PGDATABASE="$2"; shift 2;;
    *) args+=("$1"); shift;;
  esac
done
exec -a psql psql "${args[@]}"
