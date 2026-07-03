# syntax=docker/dockerfile:1
#
# eez-node image. Builds the `eez-node` binary (release) with cargo-chef
# dependency-layer caching — reth is a large, rarely-changing tree, so
# the cooked-deps layer is reused across code changes.
#
# The build is self-contained in `crates/` + `Cargo.{toml,lock}`; the
# Solidity protocol submodule and `contracts/` are NOT needed (ABI is
# inline `sol!`). Contract deploys are a separate `forge` step (see
# scripts/devnet-test.sh + README).

# ── chef base: toolchain + system deps reth/mdbx/secp256k1 need ───────
FROM rust:1.94-bookworm AS chef
RUN apt-get update && apt-get install -y --no-install-recommends \
        clang libclang-dev pkg-config cmake libssl-dev git ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && cargo install cargo-chef --locked
WORKDIR /build

# ── planner: compute the dependency recipe from manifests ────────────
FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo chef prepare --recipe-path recipe.json

# ── builder: cook deps (cached), then build eez-node ─────────────────
FROM chef AS builder
# Release-profile knobs, overridable at build time. Defaults MATCH Cargo.toml's
# [profile.release] (lto=thin / codegen-units=1 / debug=1), so a plain
# `docker build` still produces the optimized production binary. The Kurtosis
# devnet build (infra/kurtosis/up.sh) overrides these for a much faster compile
# (parallel codegen, no LTO, no debug info) — fine for a test node. Applied to
# BOTH the cook and build steps so cargo-chef's cache stays consistent.
ARG CARGO_PROFILE_RELEASE_LTO=thin
ARG CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1
ARG CARGO_PROFILE_RELEASE_DEBUG=1
ENV CARGO_PROFILE_RELEASE_LTO=${CARGO_PROFILE_RELEASE_LTO} \
    CARGO_PROFILE_RELEASE_CODEGEN_UNITS=${CARGO_PROFILE_RELEASE_CODEGEN_UNITS} \
    CARGO_PROFILE_RELEASE_DEBUG=${CARGO_PROFILE_RELEASE_DEBUG}
COPY --from=planner /build/recipe.json recipe.json
# Slow, cache-friendly layer: only re-runs when the dep graph changes.
RUN cargo chef cook --release --recipe-path recipe.json
# Workspace sources; only this layer rebuilds on first-party code changes.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release -p eez-node \
    && strip target/release/eez-node

# ── runtime: slim image with just the binary + L2 genesis ────────────
FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/eez-node /usr/local/bin/eez-node
# L2 genesis the node boots from. Fixed (deterministic) — every operator's
# L2 chain shares this genesis; each gets its own EEZ rollup on chiado.
COPY genesis.json /app/genesis.json
ENTRYPOINT ["eez-node"]
CMD ["--help"]
