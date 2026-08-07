# syntax=docker/dockerfile:1
#
# eez-deploy image — runs scripts/deploy.sh inside the Kurtosis enclave to
# deploy the EEZ contracts and generate the timestamp-aligned L2 genesis, once
# the shared L1 is live. Captured output (/out: deployments.env + l2-genesis.json)
# is consumed by the eez-node service (see main.star).
#
# Built with the REPO ROOT as context (needs contracts/ AND its sibling
# sync-rollups-protocol/ submodule, per contracts/foundry.toml libs+remappings).
# The root .dockerignore excludes sync-rollups-protocol/, so this Dockerfile
# ships its own ignore file (deploy.Dockerfile.dockerignore, used by BuildKit).
#
#   DOCKER_BUILDKIT=1 docker build -f infra/kurtosis/deploy.Dockerfile -t eez-deploy:dev .
#
# Copies eez-genesis-state-root from the eez-node image so deploy.sh can hash
# the rendered L2 genesis without a Rust toolchain in this image.

ARG EEZ_NODE_IMAGE=eez-node:dev
FROM ${EEZ_NODE_IMAGE} AS node-tools

FROM debian:bookworm-slim

ARG FOUNDRY_VERSION=stable

RUN apt-get update && apt-get install -y --no-install-recommends \
        bash git jq python3 curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Foundry (forge + cast) via foundryup — prebuilt glibc binaries for this base.
ENV PATH="/root/.foundry/bin:${PATH}"
RUN curl -L https://foundry.paradigm.xyz | bash \
    && foundryup -i "${FOUNDRY_VERSION}"

COPY --from=node-tools /usr/local/bin/eez-genesis-state-root /usr/local/bin/eez-genesis-state-root

WORKDIR /repo
# deploy.sh resolves REPO from its own location and expects these siblings:
#   /repo/scripts/deploy.sh, /repo/contracts, /repo/sync-rollups-protocol,
#   /repo/genesis.json (base L2 genesis it timestamps per-deploy).
COPY scripts ./scripts
COPY contracts ./contracts
COPY sync-rollups-protocol ./sync-rollups-protocol
COPY genesis.json ./genesis.json
# deploy.sh requires EEZ_ENV_FILE (default /repo/.env) to exist; real values
# arrive via env vars from the Kurtosis run_sh task, so an empty file suffices.
RUN touch /repo/.env

# Pre-fetch solc 0.8.34 (pinned in contracts/foundry.toml) and warm the build
# cache
RUN cd /repo/contracts && forge build

ENTRYPOINT ["/bin/bash", "-c"]
CMD ["bash /repo/scripts/deploy.sh"]
