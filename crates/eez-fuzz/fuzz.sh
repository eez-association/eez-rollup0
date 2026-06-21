#!/usr/bin/env bash
# Driver for the `compose` fuzz target. Keeps the heavy instrumented build on
# /mnt/ssd (repo storage policy). Run from anywhere.
set -euo pipefail
cd "$(dirname "$0")"
# Heavy instrumented build → /mnt/ssd on this host (storage policy); CI/other
# hosts override CARGO_TARGET_DIR.
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/mnt/ssd/eez-fuzz-target}"
T=compose
case "${1:-run}" in
  run)   shift; cargo +nightly fuzz run   "$T" --sanitizer none -- "${@:--max_total_time=300}" ;;
  repro) cargo +nightly fuzz run   "$T" --sanitizer none "$2" ;;   # repro <artifact>
  tmin)  cargo +nightly fuzz tmin  "$T" --sanitizer none "$2" ;;   # shrink a crash input
  cmin)  cargo +nightly fuzz cmin  "$T" --sanitizer none ;;        # minimize the corpus
  cov)   cargo +nightly fuzz coverage "$T" --sanitizer none ;;     # llvm-cov report
  *) echo "usage: $0 {run [libfuzzer-args]|repro <artifact>|tmin <artifact>|cmin|cov}"; exit 1 ;;
esac
