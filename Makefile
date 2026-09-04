# eez-rollup0 — operational entry points.
#
# Deployment reads .env (gitignored). Runtime node settings live in TOML.

SHELL := /bin/bash

# Surface deployment inputs to deploy recipes.
ifneq (,$(wildcard .env))
    include .env
    export
endif
ifneq (,$(wildcard deployments.env))
    include deployments.env
    export
endif

.PHONY: help check test fmt build deploy-protocol run-node clean-l2 clean-deploy

EEZ_CONFIG ?= eez-composer.toml
L2_GENESIS ?= datadir/genesis.json
L2_DATADIR ?= data/eez-l2

help:
	@echo "Targets:"
	@echo "  make build            - cargo build the workspace"
	@echo "  make check            - cargo fmt --check + clippy + check (workspace)"
	@echo "  make test             - cargo test + forge test"
	@echo "  make fmt              - cargo fmt + forge fmt"
	@echo "  make deploy-protocol  - deploy EEZ + ECDSA PS + Rollup manager + register; writes deployments.env"
	@echo "  make run-node         - run the composer against the configured L2 datadir"
	@echo "  make clean-l2         - rm L2_DATADIR (fresh L2 chain)"
	@echo "  make clean-deploy     - rm deployments.env (fresh contract addresses on next deploy)"

# ─── Rust ─────────────────────────────────────────────────────────────────
build:
	cargo build --workspace

check:
	cargo fmt --check
	cargo clippy --workspace --all-targets -- -D warnings
	cargo check --workspace

test:
	cargo test --workspace
	cd contracts && forge test

fmt:
	cargo fmt
	cd contracts && forge fmt

# ─── Contracts ────────────────────────────────────────────────────────────
#
# Runs the 5-step deploy sequence:
#
#   1. DeployEEZ                 → EEZ_REGISTRY_ADDRESS + deploy block
#   2. DeployECDSAProofSystem     → EEZ_ECDSA_PROOF_SYSTEM_ADDRESS
#                                   (authorizedSigner derived from EEZ_PROOF_SIGNER_KEY)
#   3. DeployRollup              → Rollup manager contract
#   4. RegisterRollup            → EEZ_ROLLUP_ID = 1
#   5. DeployBridgeL1            → L1 cross-chain bridge contracts
#
# Outputs land in deployments.env (gitignored). Copy those public bindings into
# eez-composer.toml before starting the node.
deploy-protocol:
	@./scripts/deploy.sh

# ─── Node ─────────────────────────────────────────────────────────────────
run-node:
	@test -f "$(EEZ_CONFIG)" || (echo "$(EEZ_CONFIG) missing; copy eez-composer.example.toml and fill it in" && exit 1)
	@test -f "$(L2_GENESIS)" || (echo "$(L2_GENESIS) missing; run make deploy-protocol first" && exit 1)
	cargo run -p eez-node -- node \
		--chain "$(L2_GENESIS)" \
		--datadir "$(L2_DATADIR)" \
		--eez.config "$(EEZ_CONFIG)"

clean-l2:
	@test -n "$(L2_DATADIR)" || (echo "L2_DATADIR not set" && exit 1)
	rm -rf "$(L2_DATADIR)"

clean-deploy:
	rm -f deployments.env
