# eez-rollup0 — operational entry points.
#
# Reads configuration from .env (gitignored). Copy .env.example first.
# Deployment outputs land in deployments.env (gitignored, auto-written
# by scripts/deploy.sh, auto-read by eez-node at startup via dotenvy).

SHELL := /bin/bash

# Surface .env values to recipes for any direct-shell consumers.
# (The Rust binary auto-loads .env via dotenvy regardless.)
ifneq (,$(wildcard .env))
    include .env
    export
endif
ifneq (,$(wildcard deployments.env))
    include deployments.env
    export
endif

.PHONY: help check test fmt build deploy-protocol run-node clean-l2 clean-deploy

help:
	@echo "Targets:"
	@echo "  make build            - cargo build the workspace"
	@echo "  make check            - cargo fmt --check + clippy + check (workspace)"
	@echo "  make test             - cargo test + forge test"
	@echo "  make fmt              - cargo fmt + forge fmt"
	@echo "  make deploy-protocol  - deploy EEZ + ECDSA PS + Rollup manager + register; writes deployments.env"
	@echo "  make run-node         - run the composer against the configured L2 datadir"
	@echo "  make clean-l2         - rm EEZ_L2_DATADIR (fresh L2 chain)"
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
# Outputs land in deployments.env (gitignored). eez-node loads it
# alongside .env at startup, so a successful deploy means `make run-node`
# Just Works without any paste-the-address-into-.env step.
deploy-protocol:
	@./scripts/deploy.sh

# ─── Node ─────────────────────────────────────────────────────────────────
run-node:
	@test -n "$(EEZ_L2_DATADIR)" || (echo "EEZ_L2_DATADIR not set; copy .env.example to .env" && exit 1)
	@test -n "$(EEZ_L2_GENESIS_PATH)" -a -f "$(EEZ_L2_GENESIS_PATH)" \
		|| (echo "EEZ_L2_GENESIS_PATH not set or file missing; run make deploy-protocol first" && exit 1)
	cargo run -p eez-node -- node \
		--chain $(EEZ_L2_GENESIS_PATH) \
		--datadir $(EEZ_L2_DATADIR) \

clean-l2:
	@test -n "$(EEZ_L2_DATADIR)" || (echo "EEZ_L2_DATADIR not set" && exit 1)
	rm -rf $(EEZ_L2_DATADIR)

clean-deploy:
	rm -f deployments.env
