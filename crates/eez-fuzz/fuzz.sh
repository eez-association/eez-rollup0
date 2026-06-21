#!/usr/bin/env bash
# Driver for the eez-fuzz targets (compose, program). Run from anywhere.
#
#   ./fuzz.sh run   [compose|program]   — campaign (EEZ_FUZZ_TIME secs, default 30)
#   ./fuzz.sh cmin  [compose|program]   — minimize fuzz/corpus/<target> in place
#   ./fuzz.sh repro <target> <artifact> — replay a crash
#   ./fuzz.sh tmin  <target> <artifact> — shrink a crash input
#   ./fuzz.sh cov   [compose|program]   — coverage report
#
# `run` reads + writes fuzz/corpus/<target> (cargo-fuzz default): the committed
# corpus is both the fuzzer seed AND the CI regression set (replayed by
# tests/corpus_replay.rs). A campaign that finds new coverage adds files there —
# `cmin` + commit to curate/extend. No host paths baked in: the instrumented
# build honors CARGO_TARGET_DIR from the environment.
set -euo pipefail
cd "$(dirname "$0")"
TIME="${EEZ_FUZZ_TIME:-30}"
cmd="${1:-run}"; T="${2:-compose}"
case "$cmd" in
  run)   cargo +nightly fuzz run  "$T" --sanitizer none -- -max_total_time="$TIME" -rss_limit_mb=6144 ;;
  cmin)  cargo +nightly fuzz cmin "$T" --sanitizer none ;;
  repro) cargo +nightly fuzz run  "$T" --sanitizer none "${3:?artifact path}" ;;
  tmin)  cargo +nightly fuzz tmin "$T" --sanitizer none "${3:?artifact path}" ;;
  cov)   cargo +nightly fuzz coverage "$T" --sanitizer none ;;
  *) echo "usage: $0 {run|cmin|repro|tmin|cov} [compose|program] ..."; exit 1 ;;
esac
