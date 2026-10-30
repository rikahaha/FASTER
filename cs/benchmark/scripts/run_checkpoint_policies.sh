#!/usr/bin/env bash
# Compares checkpoint scheduling policies on the YCSB benchmark.
#
# Usage: scripts/run_checkpoint_policies.sh [path/to/FASTER.benchmark] [output dir]
# Build first: dotnet build -c Release (from cs/benchmark)
#
# Every run writes <prefix>_timeline.csv and <prefix>_cycles.csv, and appends one row to <outdir>/ckpt_summary.csv.
# The best fixed interval in hindsight (max effective_ops_per_sec over the fixed-* rows of a workload) is the
# oracle the adaptive policies should approach.

set -euo pipefail

BENCH=${1:-./bin/x64/Release/net7.0/FASTER.benchmark}
OUT=${2:-ckpt_results}
THREADS=${THREADS:-8}
RUNSEC=${RUNSEC:-60}
MTBF=${MTBF:-60}
DIST=${DIST:-zipf}
FIXED_INTERVALS=${FIXED_INTERVALS:-"250 500 1000 2000 4000 8000"}

mkdir -p "$OUT"

run() {
    local name=$1; shift
    echo "=== $name"
    "$BENCH" -b 0 -t "$THREADS" -d "$DIST" --synth --sd --runsec "$RUNSEC" --mtbfsec "$MTBF" \
        --resultprefix "$OUT/$name" "$@" | tee "$OUT/$name.log"
    # Collect all summary rows in one file.
    tail -n +2 "$OUT/${name}_summary.csv" >> "$OUT/ckpt_summary.csv"
    rm "$OUT/${name}_summary.csv"
}

for type in foldover snapshot; do
    snap=""
    [ "$type" = snapshot ] && snap="--chkptsnap"

    # Static workloads: does each policy find the right interval?
    for rumd in "90,10,0,0" "50,50,0,0" "0,100,0,0" "50,0,50,0"; do
        tag="${type}_${rumd//,/-}"
        for ms in $FIXED_INTERVALS; do
            run "${tag}_fixed${ms}" $snap --rumd "$rumd" --chkptpolicy fixed --chkptms "$ms"
        done
        run "${tag}_youngdaly" $snap --rumd "$rumd" --chkptpolicy youngdaly
        run "${tag}_ucb" $snap --rumd "$rumd" --chkptpolicy ucb
    done

    # Workload shift halfway through: read-heavy -> write-heavy. Adaptive policies should re-converge.
    shift_args=(--rumd 90,10,0,0 --rumd2 0,100,0,0 --shiftsec $((RUNSEC / 2)))
    tag="${type}_shift"
    for ms in $FIXED_INTERVALS; do
        run "${tag}_fixed${ms}" $snap "${shift_args[@]}" --chkptpolicy fixed --chkptms "$ms"
    done
    run "${tag}_youngdaly" $snap "${shift_args[@]}" --chkptpolicy youngdaly
    run "${tag}_ucb" $snap "${shift_args[@]}" --chkptpolicy ucb
done

# Prepend the header once.
header="timestamp,policy,checkpoint_type,distribution,rumd,rumd2,shift_sec,threads,run_sec,mtbf_sec,checkpoints,mean_interval_ms,mean_duration_ms,ops_per_sec,effective_ops_per_sec,index_policy,full_checkpoints,mean_full_duration_ms,recovery_scan_mb_est,recovery_ms,rto_ms,max_predicted_recovery_ms,rto_violations"
{ echo "$header"; cat "$OUT/ckpt_summary.csv"; } > "$OUT/ckpt_summary.tmp" && mv "$OUT/ckpt_summary.tmp" "$OUT/ckpt_summary.csv"
echo "Summary: $OUT/ckpt_summary.csv"
