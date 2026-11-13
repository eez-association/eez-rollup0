#!/usr/bin/env bash
# Runs cargo on the stateless proof signer (provers/stateless-ecdsa) with its
# eez-rollup0 crates taken from this checkout instead of the commit provers
# pins, so a change to a shared crate is built and tested with the signer here.
#
#   scripts/provers-cargo.sh build -p eez-prover-stateless --bin eez-proof-signer
#   scripts/provers-cargo.sh nextest run --workspace
#
# The patches rewrite the signer's Cargo.lock, so `--locked` cannot be used;
# the lockfile is restored on exit.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
signer="$root/provers/stateless-ecdsa"
url="https://github.com/eez-association/eez-rollup0"

if [[ ! -f "$signer/Cargo.toml" ]]; then
    echo "provers/ is not checked out: git submodule update --init provers" >&2
    exit 1
fi
if [[ $# -lt 1 ]]; then
    echo "usage: $0 <cargo subcommand> [args...]" >&2
    exit 2
fi

# Every eez-rollup0 package in the signer's lockfile.
mapfile -t crates < <(awk -v src="source = \"git+$url?" '
    index($0, "name = ") == 1 { name = $3; gsub(/"/, "", name) }
    index($0, src) == 1 { print name }
' "$signer/Cargo.lock")
if [[ ${#crates[@]} -eq 0 ]]; then
    echo "no eez-rollup0 packages in $signer/Cargo.lock" >&2
    exit 1
fi
patches=()
for crate in "${crates[@]}"; do
    if [[ ! -f "$root/crates/$crate/Cargo.toml" ]]; then
        echo "the signer uses $crate, which is not in crates/" >&2
        exit 1
    fi
    patches+=(--config "patch.\"$url\".$crate.path=\"$root/crates/$crate\"")
done

saved="$(mktemp)"
cp "$signer/Cargo.lock" "$saved"
trap 'cp "$saved" "$signer/Cargo.lock"; rm -f "$saved"' EXIT

# Options go right after the subcommand so arguments after `--` stay last.
subcommand=("$1")
shift
if [[ ${subcommand[0]} == nextest && $# -gt 0 ]]; then
    subcommand+=("$1")
    shift
fi
"${CARGO:-cargo}" "${subcommand[@]}" --manifest-path "$signer/Cargo.toml" "${patches[@]}" "$@"
