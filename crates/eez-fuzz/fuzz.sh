#!/usr/bin/env bash
# Driver for the eez-fuzz targets (`compose`, `program`). Run from anywhere.
#
#   ./fuzz.sh run     [compose|program]   — campaign (EEZ_FUZZ_TIME secs, default 30)
#   ./fuzz.sh promote [compose|program]   — fold campaign findings into the
#                                           tracked corpus (minimized) → commit
#   ./fuzz.sh repro   <target> <artifact> — replay a crash
#   ./fuzz.sh tmin    <target> <artifact> — shrink a crash input
#   ./fuzz.sh cov     [compose|program]   — coverage report
#
# Corpus model: the LIVE campaign corpus lives OFF-TREE under $SCRATCH so a
# campaign never dirties the repo. `fuzz/corpus/<target>` is the curated,
# COMMITTED regression set (CI replays it via tests/corpus_replay.rs). A
# campaign seeds from it and writes new finds to scratch; `promote` merges those
# into the tracked corpus and minimizes — then you `git add` + commit to extend.
set -euo pipefail
cd "$(dirname "$0")"
# No host paths baked in. Honor CARGO_TARGET_DIR from the environment (this host
# points it at /mnt/ssd per its storage policy; CI/others use cargo's default).
# Live campaign corpus goes off-tree: $EEZ_FUZZ_SCRATCH, else under the target
# dir, else a gitignored repo-relative .scratch-corpus/.
SCRATCH="${EEZ_FUZZ_SCRATCH:-${CARGO_TARGET_DIR:+$CARGO_TARGET_DIR/fuzz-corpus}}"
SCRATCH="${SCRATCH:-fuzz/.scratch-corpus}"
TIME="${EEZ_FUZZ_TIME:-30}"

cmd="${1:-run}"
T="${2:-compose}"
case "$cmd" in
  run)
    mkdir -p "$SCRATCH/$T"
    # scratch = primary (grows); tracked corpus = read-only seed.
    cargo +nightly fuzz run "$T" "$SCRATCH/$T" "fuzz/corpus/$T" \
      --sanitizer none -- -max_total_time="$TIME" -rss_limit_mb=6144
    ;;
  promote)
    cp -n "$SCRATCH/$T"/* "fuzz/corpus/$T/" 2>/dev/null || true
    cargo +nightly fuzz cmin "$T" --sanitizer none
    echo "minimized fuzz/corpus/$T — review, then: git add crates/eez-fuzz/fuzz/corpus && git commit"
    ;;
  repro) cargo +nightly fuzz run  "$T" --sanitizer none "${3:?artifact path}" ;;
  tmin)  cargo +nightly fuzz tmin "$T" --sanitizer none "${3:?artifact path}" ;;
  cov)   cargo +nightly fuzz coverage "$T" --sanitizer none ;;
  *) echo "usage: $0 {run|promote|repro|tmin|cov} [compose|program] ..."; exit 1 ;;
esac
