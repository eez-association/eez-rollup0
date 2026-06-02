# syntax=docker/dockerfile:1

FROM rust:1.93-bookworm AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    clang \
    cmake \
    git \
    libssl-dev \
    pkg-config \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release -p eez-node \
    && cp /app/target/release/eez-node /usr/local/bin/eez-node

FROM ghcr.io/foundry-rs/foundry:v1.6.0-rc1 AS foundry

FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    ca-certificates \
    curl \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/bin/eez-node /usr/local/bin/eez-node

ENTRYPOINT ["eez-node"]

FROM debian:bookworm-slim AS deploy

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash \
    ca-certificates \
    curl \
    git \
    jq \
    libssl3 \
    python3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=foundry /usr/local/bin/anvil /usr/local/bin/anvil
COPY --from=foundry /usr/local/bin/cast /usr/local/bin/cast
COPY --from=foundry /usr/local/bin/forge /usr/local/bin/forge

WORKDIR /app

COPY contracts ./contracts
COPY deployments ./deployments
COPY scripts ./scripts
COPY sync-rollups-protocol ./sync-rollups-protocol

RUN test -f sync-rollups-protocol/src/EEZ.sol \
    && test -f sync-rollups-protocol/lib/forge-std/src/Script.sol \
    || { \
        echo "sync-rollups-protocol submodule is incomplete."; \
        echo "Run: git submodule update --init --recursive"; \
        exit 1; \
    }

ENTRYPOINT ["/bin/bash"]
