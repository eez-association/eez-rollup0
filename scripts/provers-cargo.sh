#!/usr/bin/env bash
# Runs cargo on both ECDSA proof signers (provers/ecdsa) with their
# protocol crates taken from this checkout instead of the commit provers
# pins, so a change to a shared crate is built and tested with the signer here.
#
#   scripts/provers-cargo.sh build -p eez-prover-stateless --bin eez-proof-signer
#   scripts/provers-cargo.sh nextest run --workspace
#
# The patches rewrite the signer's Cargo.lock, so `--locked` cannot be used;
# the lockfile is restored on exit.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
signer="$root/provers/ecdsa"
url="https://github.com/eez-association/eez-rollup0"

if [[ ! -f "$signer/Cargo.toml" ]]; then
    echo "provers/ is not checked out: git submodule update --init provers" >&2
    exit 1
fi
if [[ $# -lt 1 ]]; then
    echo "usage: $0 <cargo subcommand> [args...]" >&2
    exit 2
fi

# Each run rewrites the signer's Cargo.lock and restores it, so runs take turns.
# The lock is held until this script and its cargo exit. perl's flock, because
# flock(1) is not on macOS.
mkdir -p "$root/target"
exec 9>"$root/target/.provers-cargo.lock"
perl -MFcntl=:flock -e 'open(my $lock, ">&=", 9) or die "lock: $!\n"; flock($lock, LOCK_EX) or die "lock: $!\n"'

# Only protocol crates cross independently pinned execution stacks. Patching the
# follower closure from an evm2 checkout would also change its owning workspace's
# reth dependencies. Keep that closure at the provers repo's Git pin by default.
crates=(eez-blob eez-control-rpc eez-payload-codec eez-primitives eez-protocol eez-prover eez-prover-client)
patches=()
for crate in "${crates[@]}"; do
    if [[ ! -f "$root/crates/$crate/Cargo.toml" ]]; then
        echo "the signer uses $crate, which is not in crates/" >&2
        exit 1
    fi
    patches+=(--config "patch.\"$url\".$crate.path=\"$root/crates/$crate\"")
done

# For work on shared follower internals, explicitly supply a checkout compatible
# with the prover reth pin. This does not select the EVM; the prover binary does.
if [[ -n "${EEZ_PROVERS_FOLLOWER_SOURCE:-}" ]]; then
    for crate in eez-deriver eez-driver eez-engine eez-follower eez-l1 eez-node-common eez-p2p eez-seq-sig; do
        path="$EEZ_PROVERS_FOLLOWER_SOURCE/crates/$crate"
        [[ -f "$path/Cargo.toml" ]] || { echo "missing $path/Cargo.toml" >&2; exit 1; }
        patches+=(--config "patch.\"$url\".$crate.path=\"$path\"")
    done
fi

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
