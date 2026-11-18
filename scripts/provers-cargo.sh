#!/usr/bin/env bash
# Runs cargo on one ECDSA prover workspace with its shared signing and
# protocol crates taken from this checkout instead of the commit provers
# pins, so a change to a shared crate is built and tested with the signer here.
#
#   scripts/provers-cargo.sh build -p eez-prover-stateless --bin eez-proof-signer
#   scripts/provers-cargo.sh --stateful nextest run --workspace
#
# The patches rewrite the signer's Cargo.lock, so `--locked` cannot be used;
# the lockfile is restored on exit.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
signer="$root/provers/stateless-ecdsa"
if [[ ${1:-} == --stateful ]]; then
    signer="$root/provers/stateful-ecdsa"
    shift
fi
url="https://github.com/eez-association/eez-rollup0"

if [[ ! -f "$signer/Cargo.toml" ]]; then
    echo "provers/ is not checked out: git submodule update --init provers" >&2
    exit 1
fi
if [[ $# -lt 1 ]]; then
    echo "usage: $0 [--stateful] <cargo subcommand> [args...]" >&2
    exit 2
fi

# Each run rewrites the signer's Cargo.lock and restores it, so runs take turns.
# The lock is held until this script and its cargo exit. perl's flock, because
# flock(1) is not on macOS.
mkdir -p "$root/target"
exec 9>"$root/target/.provers-cargo.lock"
perl -MFcntl=:flock -e 'open(my $lock, ">&=", 9) or die "lock: $!\n"; flock($lock, LOCK_EX) or die "lock: $!\n"'

# Only execution-independent crates cross workspace boundaries. Keep each
# prover's follower/EVM closure at its own Git pin, including when root later
# changes reth versions. The signing library is shared by both backends.
crates=(eez-blob eez-control-rpc eez-payload-codec eez-primitives eez-proof-signer eez-protocol eez-prover eez-prover-client)
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
