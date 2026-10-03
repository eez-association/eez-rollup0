#!/usr/bin/env bash
# Shared by the Dockerfile and CI. reth_gnosis turns on reth's default `jit`
# (revmc → llvm-sys 221 → LLVM 22) and `gmp` (m4) for the whole build graph.
set -euo pipefail

if [[ -x /usr/lib/llvm-22/bin/llvm-config ]] \
    && [[ -f /usr/lib/llvm-22/lib/libPolly.a ]] \
    && [[ -x /usr/lib/llvm-22/bin/clang ]] \
    && command -v m4 >/dev/null; then
    /usr/lib/llvm-22/bin/llvm-config --version
    exit 0
fi

[[ "$EUID" -eq 0 ]] || { echo 'Run with sudo to install LLVM 22 build dependencies' >&2; exit 1; }
source /etc/os-release
case "$ID" in debian|ubuntu) ;; *) echo "Unsupported build distribution: $ID" >&2; exit 1 ;; esac
apt-get update
if ! apt-cache show llvm-22-dev >/dev/null 2>&1; then
    apt-get install -y --no-install-recommends ca-certificates curl gnupg
    key="$(mktemp)"
    trap 'rm -f "$key"' EXIT
    curl --fail --silent --show-error --location https://apt.llvm.org/llvm-snapshot.gpg.key -o "$key"
    gpg --batch --yes --dearmor -o /usr/share/keyrings/eez-llvm.gpg "$key"
    printf 'deb [signed-by=/usr/share/keyrings/eez-llvm.gpg] https://apt.llvm.org/%s/ llvm-toolchain-%s-22 main\n' \
        "$VERSION_CODENAME" "$VERSION_CODENAME" > /etc/apt/sources.list.d/eez-llvm.list
    apt-get update
fi
apt-get install -y --no-install-recommends llvm-22-dev libpolly-22-dev clang-22 m4
/usr/lib/llvm-22/bin/llvm-config --version
