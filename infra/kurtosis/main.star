# EEZ cross-chain devnet Kurtosis package.
#
# Runs both halves of the harness in one enclave:
#   Pair B: ethereum-package L1, rbuilder, relay, mev-boost, spamoor,
#           disruptoor, and observability services.
#   Pair A: eez-node with embedded L1, composer, L2, cross-chain fronts,
#           plus a follower Lighthouse for the embedded L1 Engine API.
#
# Usage: kurtosis run . --args-file args.yaml

ethereum_package = import_module("github.com/ethpandaops/ethereum-package/main.star")

# Pair A fixed ports inside the enclave.
EMBEDDED_L1_RPC_PORT = 18545
EMBEDDED_L1_ENGINE_PORT = 18551
L2_RPC_PORT = 18688
L2_ENGINE_PORT = 18684
L2_P2P_PORT = 30640
L1_XCHAIN_PORT = 18999
L2_XCHAIN_PORT = 18998
# rbuilder exposes eth_sendBundle on its dedicated RPC port.
BUILDER_FLASHBOTS_RPC_PORT = 8645
MEV_RELAY_API_PORT = 9062
SPAMOOR_IMAGE = "ethpandaops/spamoor@sha256:24818bf7ab76696b2dccb0c59cb419cce358cf1b4326a545012b031afd11658b"

# L2 genesis state root for genesis.json. Recompute if genesis alloc changes.
L2_GENESIS_STATE_ROOT = "0xd381d828f650845aa890778c74ad2de245f5b3f2a24763f243e19a6bafb4fec5"


def run(plan, args):
    eth_args = args["ethereum_package"]
    eez = args.get("eez", {})

    poster_key = eez.get("poster_key", "")
    proof_signer_key = eez.get("proof_signer_key", "")
    if poster_key in ["", "0xCHANGE_ME"] or proof_signer_key in ["", "0xCHANGE_ME"]:
        fail("set eez.poster_key and eez.proof_signer_key in the args file " +
             "(bash infra/kurtosis/up.sh derives both automatically on first run)")

    # Pair B: canonical L1, validators, MEV stack, and load/reorg services.
    eth = ethereum_package.run(plan, eth_args)

    participants = eth.all_participants
    l1_el = participants[0].el_context

    # Feed the follower all Pair B beacon peers for stable block gossip.
    cl_enrs = [p.cl_context.enr for p in participants]
    cl_multiaddrs = [p.cl_context.multiaddr for p in participants]
    cl_peer_ids = [p.cl_context.peer_id for p in participants]

    # Find the rbuilder participant.
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
        builder_rpc = "http://{}:{}".format(builder_el.dns_name, BUILDER_FLASHBOTS_RPC_PORT)

    relay_rpc = eez.get("relay_url", "")
    if relay_rpc == "":
        relay_rpc = "http://mev-relay-api:{}".format(MEV_RELAY_API_PORT)

    chain_id = str(eth_args.get("network_params", {}).get("network_id", "7331"))

    # Pair A engine API JWT.
    jwt = plan.run_sh(
        description = "mint engine-API JWT (embedded reth <-> follower)",
        image = "alpine:3.20",
        run = "mkdir -p /jwt && tr -dc 'a-f0-9' < /dev/urandom | head -c 64 > /jwt/jwtsecret",
        store = [StoreSpec(src = "/jwt/jwtsecret", name = "eez-jwt")],
    )

    # Deploy protocol contracts and emit /out/deployments.env + /out/l2-genesis.json.
    deploy = plan.run_sh(
        description = "deploy EEZ contracts + generate L2 genesis on the shared L1",
        image = eez.get("deploy_image", "eez-deploy:dev"),
        env_vars = {
            "EEZ_L1_RPC_URL": l1_el.rpc_http_url,
            "EEZ_L1_POSTER_KEY": poster_key,
            "EEZ_PROOF_SIGNER_KEY": proof_signer_key,
            "EEZ_DEPLOYMENTS_FILE": "/out/deployments.env",
            "EEZ_GENESIS_OUT": "/out/l2-genesis.json",
            "EEZ_INITIAL_STATE_ROOT": L2_GENESIS_STATE_ROOT,
        },
        run = "mkdir -p /out && bash /repo/scripts/deploy.sh",
        store = [StoreSpec(src = "/out", name = "eez-deployments")],
        wait = "900s",
    )

    # eez-node: embedded L1, composer, L2, and cross-chain fronts.
    eez_env = {
        "EEZ_L1_EMBEDDED": "1",
        "EEZ_L1_CHAIN": "devnet",
        "EEZ_L1_CHAIN_PATH": "/genesis/genesis.json",
        "EEZ_L1_JWT_SECRET": "/jwt/jwtsecret",
        "EEZ_L1_HTTP_PORT": str(EMBEDDED_L1_RPC_PORT),
        "EEZ_L1_AUTH_PORT": str(EMBEDDED_L1_ENGINE_PORT),
        "EEZ_L1_CHAIN_ID": chain_id,
        "EEZ_L1_RPC_URL": "http://127.0.0.1:{}".format(EMBEDDED_L1_RPC_PORT),
        "EEZ_L1_TARGET_RPC_URL": l1_el.rpc_http_url,
        "EEZ_L1_BUILDER_RPC_URL": builder_rpc,
        "EEZ_L1_RELAY_RPC_URL": relay_rpc,
        "EEZ_L1_TRUSTED_PEERS": l1_el.enode,
        "EEZ_L1_BLOCK_TIME_MS": str(eez.get("l1_block_time_ms", 12000)),
        "EEZ_L2_BLOCK_TIME_MS": str(eez.get("l2_block_time_ms", 2000)),
        "EEZ_PROOF_TIME_MS": str(eez.get("proof_time_ms", 5000)),
        "EEZ_SUBMISSION_SLACK_MS": str(eez.get("submission_slack_ms", 2500)),
        "EEZ_MAX_SPECULATIVE_DEPTH": str(eez.get("max_speculative_depth", 0)),
        "EEZ_MAX_USER_TXS_PER_BUNDLE": str(eez.get("max_user_txs_per_bundle", 10)),
        "DEVNET_FEE_RECIPIENT": eez.get("fee_recipient", "0x0000000000000000000000000000000000000000"),
        "EEZ_L1_POSTER_KEY": poster_key,
        "EEZ_PROOF_SIGNER_KEY": proof_signer_key,
        "EEZ_L2_DATADIR": "/data/l2",
        "EEZ_L2_HTTP_PORT": str(L2_RPC_PORT),
        "EEZ_L2_RPC_URL": "http://127.0.0.1:{}".format(L2_RPC_PORT),
        "EEZ_L1_XCHAIN_PORT": str(L1_XCHAIN_PORT),
        "EEZ_L2_XCHAIN_PORT": str(L2_XCHAIN_PORT),
        "EEZ_L2_AUTH_PORT": str(L2_ENGINE_PORT),
        "EEZ_L2_P2P_PORT": str(L2_P2P_PORT),
        "EEZ_L2_SYSTEM_KEY": eez.get("l2_system_key", "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"),
        "EEZ_L2_SYSTEM_ADDRESS": eez.get("l2_system_address", "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"),
        "EEZ_CCM_L2_ADDRESS": eez.get("ccm_l2_address", "0x4200000000000000000000000000000000000007"),
        "EEZ_CROSS_CHAIN_SOURCE_CHAIN_IDS": eez.get("cross_chain_source_chain_ids", chain_id),
    }

    node_cmd = " ".join([
        "set -eu;",
        "echo 'eez-node: sourcing /out/deployments.env';",
        "test -f /out/deployments.env;",
        "grep -E '^(EEZ_REGISTRY_ADDRESS|EEZ_REGISTRY_DEPLOY_BLOCK|EEZ_ROLLUP_ID|EEZ_INITIAL_STATE_ROOT|EEZ_L1_L2_PROXY|EEZ_L1_BRIDGE_SENDER)=' /out/deployments.env;",
        "set -a; . /out/deployments.env; set +a;",
        "echo \"eez-node: loaded EEZ_REGISTRY_ADDRESS=$EEZ_REGISTRY_ADDRESS EEZ_ROLLUP_ID=$EEZ_ROLLUP_ID EEZ_INITIAL_STATE_ROOT=$EEZ_INITIAL_STATE_ROOT\";",
        "exec eez-node node",
        "--chain=/out/l2-genesis.json",
        "--datadir=$EEZ_L2_DATADIR",
        "--http --http.addr=0.0.0.0 --http.port=$EEZ_L2_HTTP_PORT --http.api=eth,net,web3",
        "--authrpc.addr=127.0.0.1 --authrpc.port=$EEZ_L2_AUTH_PORT",
        "--port=$EEZ_L2_P2P_PORT --discovery.port=$EEZ_L2_P2P_PORT",
        "--discovery.v5.port=$((EEZ_L2_P2P_PORT+1))",
        "--ipcdisable --disable-discovery",
    ])

    # Follower beacon drives eez-node's embedded reth over the engine API.
    plan.add_service(
        name = "eez-node",
        config = ServiceConfig(
            image = eez.get("eez_node_image", "eez-node:dev"),
            ports = {
                "l1-engine": PortSpec(number = EMBEDDED_L1_ENGINE_PORT, transport_protocol = "TCP"),
                "l2-rpc": PortSpec(number = L2_RPC_PORT, transport_protocol = "TCP", application_protocol = "http"),
                "l1-xchain": PortSpec(number = L1_XCHAIN_PORT, transport_protocol = "TCP", application_protocol = "http"),
                "l2-xchain": PortSpec(number = L2_XCHAIN_PORT, transport_protocol = "TCP", application_protocol = "http"),
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

    plan.add_service(
        name = "eez-follower",
        config = ServiceConfig(
            image = eez.get("follower_image", "sigp/lighthouse:v8.1.2"),
            private_ip_address_placeholder = "FOLLOWER_IP",
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
                "--boot-nodes=" + ",".join(cl_enrs),
                "--libp2p-addresses=" + ",".join(cl_multiaddrs),
                "--trusted-peers=" + ",".join(cl_peer_ids),
                "--enable-private-discovery",
                "--disable-packet-filter",
                "--disable-enr-auto-update",
                "--enr-address=FOLLOWER_IP",
                "--enr-udp-port=9000",
                "--enr-tcp-port=9000",
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

    # Keep inbound and outbound wallet pools in separate daemons.
    spamoor_eez = eez.get("spamoor_eez", {})
    enabled = spamoor_eez.get("enabled", True)
    inbound_key = spamoor_eez.get("inbound_private_key", "")
    outbound_key = spamoor_eez.get("outbound_private_key", "")
    inbound_enabled = enabled and inbound_key not in ["", "0xCHANGE_ME"]
    outbound_enabled = enabled and outbound_key not in ["", "0xCHANGE_ME"]
    if enabled and not inbound_enabled:
        plan.print("skipping spamoor-eez-inbound: set eez.spamoor_eez.inbound_private_key (up.sh derives it automatically)")
    if enabled and not outbound_enabled:
        plan.print("skipping spamoor-eez-outbound: set eez.spamoor_eez.outbound_private_key (up.sh derives it automatically)")

    if inbound_enabled or outbound_enabled:
        # Uploaded contents are mounted at /plugins/eez-rollup below — that
        # container path (not this host path) drives the Yaegi import name.
        plugin_files = plan.upload_files(
            src = "./spamoor-plugins",
            name = "eez-xchain-plugin",
        )

    if inbound_enabled:
        inbound_args = [
            "/app/spamoor-daemon",
            "--port=8080",
            "-h {}".format(l1_el.rpc_http_url),
            "-p " + inbound_key,
            "--plugin=/plugins/eez-rollup",
        ]
        inbound_files = {"/plugins/eez-rollup": plugin_files}
        inbound_startup = spamoor_eez.get("inbound_startup_spammer_config", "")
        if inbound_startup != "":
            inbound_files["/config"] = plan.upload_files(
                src = inbound_startup,
                name = "eez-inbound-spammer-config",
            )
            inbound_args.append("--startup-spammer=/config/" + inbound_startup.split("/")[-1])

        plan.add_service(
            name = "spamoor-eez-inbound",
            config = ServiceConfig(
                image = spamoor_eez.get("image", SPAMOOR_IMAGE),
                ports = {
                    "http": PortSpec(number = 8080, transport_protocol = "TCP", application_protocol = "http"),
                },
                files = inbound_files,
                entrypoint = ["/bin/sh", "-c"],
                cmd = ["while ! " + " ".join(inbound_args) + "; do echo 'spamoor inbound startup failed; retrying in 2s'; sleep 2; done"],
            ),
        )

    if outbound_enabled:
        outbound_args = [
            "/app/spamoor-daemon",
            "--port=8080",
            "-h http://eez-node:{}".format(L2_RPC_PORT),
            "-p " + outbound_key,
            "--plugin=/plugins/eez-rollup",
        ]
        outbound_files = {"/plugins/eez-rollup": plugin_files}
        outbound_startup = spamoor_eez.get("outbound_startup_spammer_config", "")
        if outbound_startup != "":
            outbound_files["/config"] = plan.upload_files(
                src = outbound_startup,
                name = "eez-outbound-spammer-config",
            )
            outbound_args.append("--startup-spammer=/config/" + outbound_startup.split("/")[-1])

        plan.add_service(
            name = "spamoor-eez-outbound",
            config = ServiceConfig(
                image = spamoor_eez.get("image", SPAMOOR_IMAGE),
                ports = {
                    "http": PortSpec(number = 8080, transport_protocol = "TCP", application_protocol = "http"),
                },
                files = outbound_files,
                entrypoint = ["/bin/sh", "-c"],
                cmd = ["while ! " + " ".join(outbound_args) + "; do echo 'spamoor outbound startup failed; retrying in 2s'; sleep 2; done"],
            ),
        )

    plan.print("EEZ cross-chain devnet up: Pair B (ethereum-package) + Pair A (eez-node + follower), chain_id=" + chain_id)
