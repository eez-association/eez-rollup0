#
# EEZ cross-chain devnet — single self-contained Kurtosis package.
#
# Runs BOTH halves of the harness inside one enclave:
#
#   Pair B  ethpandaops/ethereum-package: reth+lighthouse validators, rbuilder,
#           relay, mev-boost, spamoor, disruptoor. Generates the ONE shared
#           genesis (artifact "el_cl_genesis_data").
#   Pair A  eez-node (embedded L1 reth + composer + sequencer + L2) and a
#           beacon-only follower Lighthouse that drives eez-node's embedded reth
#           over the engine API.
#
# Everything talks over the enclave's internal DNS on fixed ports, so the old
# host-side scrape/bridge scripts (parse-endpoints, extract-genesis,
# get-cl-bootnode, get-el-bootnode, host.docker.internal) are gone: the enclave
# EL's enode, the CL's ENR/multiaddr, and the genesis artifact all come straight
# from ethereum-package's run() output.
#
# Usage:  kurtosis run . --args-file args.yaml     (see args.example.yaml)

ethereum_package = import_module("github.com/ethpandaops/ethereum-package/main.star")

# Fixed in-enclave ports for Pair A (host publishing is handled by Kurtosis).
# EMBEDDED_L1_* are the reth compiled INTO eez-node (its own L1 node); L2_* are
# eez-node's rollup reth. Both live in the one eez-node process/service.
EMBEDDED_L1_RPC_PORT = 18545      # embedded L1 reth JSON-RPC (composer reads in-process)
EMBEDDED_L1_ENGINE_PORT = 18551   # embedded L1 reth authrpc — the follower dials this
L2_RPC_PORT = 18688               # eez-node's L2 rollup reth JSON-RPC
L2_ENGINE_PORT = 18684            # L2 engine (driven in-process)
L2_P2P_PORT = 30640

# rbuilder's bundle endpoint ("rbuilder-rpc") is 8645 inside the enclave — the
# EL range, not the standard 8545 JSON-RPC.
RBUILDER_RPC_PORT = 8645


def run(plan, args):
    eth_args = args["ethereum_package"]
    eez = args.get("eez", {})

    # Fail early with a clear message if the keys weren't filled in (rather than
    # a cryptic KeyError deep in the deploy step).
    poster_key = eez.get("poster_key", "")
    proof_signer_key = eez.get("proof_signer_key", "")
    if poster_key in ["", "0xCHANGE_ME"] or proof_signer_key in ["", "0xCHANGE_ME"]:
        fail("set eez.poster_key and eez.proof_signer_key in the args file " +
             "(copy args.example.yaml → args.yaml; see the 'Keys' section of the README)")

    # ── 1. Pair B: the whole L1 / MEV / load stack ──────────────────────
    eth = ethereum_package.run(plan, eth_args)

    participants = eth.all_participants
    # The reference L1 node = first participant (el-1 / cl-1): a validator-backed
    # reth+lighthouse pair on Pair B's chain. Distinct from eez-node's EMBEDDED
    # L1 reth — this is the external L1 the embedded one syncs against. Its enode
    # seeds the embedded reth's RLPx backfill; its CL ENR/multiaddr seed the
    # follower's discovery — both straight from the output API, no scraping.
    l1_el = participants[0].el_context
    l1_cl = participants[0].cl_context

    # rbuilder lives on the participant whose service name carries "builder".
    builder_el = None
    for p in participants:
        if "builder" in p.el_context.service_name:
            builder_el = p.el_context
            break

    builder_rpc = eez.get("builder_rpc_url", "")
    if builder_rpc == "":
        if builder_el == None:
            fail("no rbuilder participant found (service name containing 'builder'); " +
                 "set eez.builder_rpc_url explicitly")
        builder_rpc = "http://{}:{}".format(builder_el.dns_name, RBUILDER_RPC_PORT)

    chain_id = str(eth_args.get("network_params", {}).get("network_id", "7331"))

    # ── 2. Mint the engine-API JWT shared by embedded reth <-> follower ──
    # Independent of Pair B's JWT: it only guards the Pair-A engine channel.
    jwt = plan.run_sh(
        description = "mint engine-API JWT (embedded reth <-> follower)",
        image = "alpine:3.20",
        # 32 bytes as 64 hex chars, no newline. tr -dc / head -c are busybox-safe.
        run = "mkdir -p /jwt && tr -dc 'a-f0-9' < /dev/urandom | head -c 64 > /jwt/jwtsecret",
        store = [StoreSpec(src = "/jwt/jwtsecret", name = "eez-jwt")],
    )

    # ── 3. Deploy EEZ contracts + build the L2 genesis on the live L1 ────
    # scripts/deploy.sh (baked into the deploy image) deploys the registry +
    # proof system + rollup, then writes a timestamp-aligned L2 genesis. Both
    # outputs land in /out and are captured as the "eez-deployments" artifact.
    # deploy.sh already retries the L1 RPC until it answers, so no extra wait.
    deploy = plan.run_sh(
        description = "deploy EEZ contracts + generate L2 genesis on the shared L1",
        image = eez.get("deploy_image", "eez-deploy:dev"),
        env_vars = {
            "EEZ_L1_RPC_URL": l1_el.rpc_http_url,
            "EEZ_L1_POSTER_KEY": poster_key,
            "EEZ_PROOF_SIGNER_KEY": proof_signer_key,
            "EEZ_DEPLOYMENTS_FILE": "/out/deployments.env",
            "EEZ_GENESIS_OUT": "/out/l2-genesis.json",
        },
        run = "mkdir -p /out && bash /repo/scripts/deploy.sh",
        store = [StoreSpec(src = "/out", name = "eez-deployments")],
        wait = "900s",
    )

    # ── 4. eez-node (embedded L1 + composer + L2) ───────────────────────
    # The deployments artifact is mounted at the SAME /out path used at deploy
    # time, so the absolute paths written into deployments.env (notably
    # EEZ_L2_GENESIS_PATH=/out/l2-genesis.json) resolve unchanged.
    eez_env = {
        "EEZ_L1_EMBEDDED": "1",
        "EEZ_L1_CHAIN": "devnet",
        "EEZ_L1_CHAIN_PATH": "/genesis/genesis.json",
        "EEZ_L1_JWT_SECRET": "/jwt/jwtsecret",
        "EEZ_L1_HTTP_PORT": str(EMBEDDED_L1_RPC_PORT),
        "EEZ_L1_AUTH_PORT": str(EMBEDDED_L1_ENGINE_PORT),
        "EEZ_L1_CHAIN_ID": chain_id,
        # Composer reads L1 state in-process from the embedded reth.
        "EEZ_L1_RPC_URL": "http://127.0.0.1:{}".format(EMBEDDED_L1_RPC_PORT),
        # Submitter targets the CANONICAL EL (where rbuilder builds and blocks
        # land) for bundle targeting + receipts — NOT the lagging embedded reth.
        "EEZ_L1_TARGET_RPC_URL": l1_el.rpc_http_url,
        "EEZ_L1_BUILDER_RPC_URL": builder_rpc,
        # Embedded reth backfills 1..N over RLPx from this enode (the follower
        # feeds it only HEAD payloads). enode carries the real enclave IP.
        "EEZ_L1_TRUSTED_PEERS": l1_el.enode,
        "EEZ_L1_BLOCK_TIME_MS": str(eez.get("l1_block_time_ms", 12000)),
        "EEZ_L2_BLOCK_TIME_MS": str(eez.get("l2_block_time_ms", 2000)),
        "EEZ_PROOF_TIME_MS": str(eez.get("proof_time_ms", 4000)),
        "EEZ_SUBMISSION_SLACK_MS": str(eez.get("submission_slack_ms", 1500)),
        "DEVNET_FEE_RECIPIENT": eez.get("fee_recipient", "0x0000000000000000000000000000000000000000"),
        "EEZ_L1_POSTER_KEY": poster_key,
        "EEZ_PROOF_SIGNER_KEY": proof_signer_key,
        "EEZ_L2_DATADIR": "/data/l2",
        "EEZ_L2_HTTP_PORT": str(L2_RPC_PORT),
        "EEZ_L2_AUTH_PORT": str(L2_ENGINE_PORT),
        "EEZ_L2_P2P_PORT": str(L2_P2P_PORT),
        "EEZ_L2_SYSTEM_KEY": eez.get("l2_system_key", "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"),
        "EEZ_L2_SYSTEM_ADDRESS": eez.get("l2_system_address", "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"),
        "EEZ_CCM_L2_ADDRESS": eez.get("ccm_l2_address", "0x4200000000000000000000000000000000000007"),
        # MUST include the L1 chainId or every op classifies as L2-only and
        # batches settle empty.
        "EEZ_CROSS_CHAIN_SOURCE_CHAIN_IDS": eez.get("cross_chain_source_chain_ids", chain_id),
    }

    node_cmd = " ".join([
        "set -a; . /out/deployments.env; set +a;",
        "exec eez-node node",
        "--chain=/out/l2-genesis.json",
        "--datadir=$EEZ_L2_DATADIR",
        "--http --http.addr=0.0.0.0 --http.port=$EEZ_L2_HTTP_PORT --http.api=eth,net,web3",
        "--authrpc.addr=127.0.0.1 --authrpc.port=$EEZ_L2_AUTH_PORT",
        "--port=$EEZ_L2_P2P_PORT --discovery.port=$EEZ_L2_P2P_PORT",
        "--discovery.v5.port=$((EEZ_L2_P2P_PORT+1))",
        "--ipcdisable --disable-discovery",
    ])

    plan.add_service(
        name = "eez-node",
        config = ServiceConfig(
            image = eez.get("eez_node_image", "eez-node:dev"),
            ports = {
                "l1-rpc": PortSpec(number = EMBEDDED_L1_RPC_PORT, transport_protocol = "TCP", application_protocol = "http"),
                "l1-engine": PortSpec(number = EMBEDDED_L1_ENGINE_PORT, transport_protocol = "TCP"),
                "l2-rpc": PortSpec(number = L2_RPC_PORT, transport_protocol = "TCP", application_protocol = "http"),
            },
            files = {
                "/out": deploy.files_artifacts[0],
                "/genesis": "el_cl_genesis_data",
                "/jwt": jwt.files_artifacts[0],
            },
            env_vars = eez_env,
            entrypoint = ["/bin/sh", "-c"],
            cmd = [node_cmd],
        ),
    )

    # ── 5. Follower beacon (no validators) — drives eez-node's embedded reth ─
    # discv5 still bans private-IP ENRs by default, so --disable-packet-filter
    # + an explicit boot ENR / libp2p multiaddr (both from cl_context) let it
    # peer into the enclave CL net. No scraping — these come from the output API.
    plan.add_service(
        name = "eez-follower",
        config = ServiceConfig(
            image = eez.get("follower_image", "sigp/lighthouse:v8.1.2"),
            ports = {
                "http": PortSpec(number = 5252, transport_protocol = "TCP", application_protocol = "http"),
            },
            files = {
                "/testnet": "el_cl_genesis_data",
                "/jwt": jwt.files_artifacts[0],
            },
            entrypoint = ["lighthouse"],
            cmd = [
                "beacon_node",
                "--testnet-dir=/testnet",
                "--datadir=/data",
                "--execution-endpoint=http://eez-node:{}".format(EMBEDDED_L1_ENGINE_PORT),
                "--jwt-secrets=/jwt/jwtsecret",
                "--boot-nodes=" + l1_cl.enr,
                "--libp2p-addresses=" + l1_cl.multiaddr,
                "--disable-packet-filter",
                "--subscribe-all-subnets",
                "--listen-address=0.0.0.0",
                "--port=9000",
                "--http",
                "--http-address=0.0.0.0",
                "--http-port=5252",
                "--suggested-fee-recipient=" + eez.get("fee_recipient", "0x0000000000000000000000000000000000000000"),
            ],
        ),
    )

    plan.print("EEZ cross-chain devnet up: Pair B (ethereum-package) + Pair A (eez-node + follower), chain_id=" + chain_id)
