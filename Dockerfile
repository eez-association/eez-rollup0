# syntax=docker/dockerfile:1
#
# eez-node image. Builds the production node binaries (release) with cargo-chef
# dependency-layer caching — reth is a large, rarely-changing tree, so
# the cooked-deps layer is reused across code changes.
#
# The build is self-contained in `crates/` + `Cargo.{toml,lock}`; the
# Solidity protocol submodule and `contracts/` are NOT needed (ABI is
# inline `sol!`). Contract deploys are a separate `forge` step (see
# scripts/deploy.sh + README).

# ── chef base: toolchain + system deps reth/mdbx/secp256k1 need ───────
FROM rust:1.94-bookworm AS chef
RUN apt-get update && apt-get install -y --no-install-recommends \
        clang libclang-dev pkg-config cmake libssl-dev git ca-certificates protobuf-compiler mold \
    && rm -rf /var/lib/apt/lists/* \
    && cargo install cargo-chef --locked
# mold cuts the final link from tens of seconds (single-threaded bfd on a
# reth-sized symbol table) to a few. Set here so cook and build see the
# SAME flags — RUSTFLAGS is part of cargo's fingerprint, and a mismatch
# between the two RUNs would rebuild deps in the hot source layer.
# Docker-only on purpose: host builds keep the default linker.
ENV RUSTFLAGS="-C link-arg=-fuse-ld=mold"
WORKDIR /build

# ── planner: compute the dependency recipe from manifests ────────────
FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo chef prepare --recipe-path recipe.json

# ── builder: cook deps (cached), then build eez-node ─────────────────
# Keep target/ in Docker layers, not a BuildKit cache mount. The cooked
# dependency artifacts then survive into the source build layer and can be
# exported/restored by the GitHub Actions cache. A target/ cache mount is
# useful on a persistent local builder, but GitHub-hosted runners start cold
# often enough that it makes the hot source layer recompile too much.
# Mount registry/git subdirs only (never all of $CARGO_HOME — that would hide
# cargo-chef in $CARGO_HOME/bin).
FROM chef AS builder
# `release` (workspace profile: cgu=16, stripped, thin LTO) is the
# fast-to-build default; BUILD_PROFILE=maxperf for production binaries.
ARG BUILD_PROFILE=release
COPY --from=planner /build/recipe.json recipe.json
# Slow, cache-friendly layer: only re-runs when the dep graph changes.
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git,sharing=locked \
    cargo chef cook --profile "$BUILD_PROFILE" --recipe-path recipe.json \
        --package eez-node --package eez-follower --package eez-proof-signer
# Workspace sources; only this layer rebuilds on first-party code changes.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN --mount=type=cache,id=cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=cargo-git,target=/usr/local/cargo/git,sharing=locked \
    cargo build --profile "$BUILD_PROFILE" -p eez-node --bin eez-composer --example genesis_state_root \
    && cargo build --profile "$BUILD_PROFILE" -p eez-follower --bin eez-follower \
    && cargo build --locked --profile "$BUILD_PROFILE" -p eez-proof-signer --bin eez-proof-signer \
    && cp "target/$BUILD_PROFILE/eez-proof-signer" /build/eez-proof-signer \
    && cp "target/$BUILD_PROFILE/eez-composer" /build/eez-composer \
    && cp "target/$BUILD_PROFILE/eez-follower" /build/eez-follower \
    && cp "target/$BUILD_PROFILE/examples/genesis_state_root" /build/genesis_state_root

# CI builds this target after the node image, reusing the same builder layers.
FROM debian:bookworm-slim AS proof-signer
RUN apt-get update && apt-get install -y --no-install-recommends netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder --chmod=0555 /build/eez-proof-signer /usr/local/bin/eez-proof-signer
WORKDIR /app
USER 65532:65532
EXPOSE 50061/tcp
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/eez-proof-signer"]

# ── runtime: slim image with just the binaries ──────────────────
FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/eez-composer /usr/local/bin/eez-composer
COPY --from=builder /build/eez-follower /usr/local/bin/eez-follower
COPY --from=builder /build/genesis_state_root /usr/local/bin/eez-genesis-state-root
# Deployment generates the L2 genesis with its own system address. It must be
# mounted explicitly; the image must never ship a privileged test identity.
ENTRYPOINT ["eez-composer"]
CMD ["--help"]
