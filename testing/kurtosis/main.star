# Kurtosis local network: canonical L1, builder stack, and eez-node.

ethereum_package = import_module(
    "github.com/ethpandaops/ethereum-package/main.star@199620b24ac979c676010c5a68b2893c2bce4f1f"
)
blockscout = import_module("./blockscout.star")

# Pair A fixed ports inside the enclave.
EMBEDDED_L1_RPC_PORT = 18545
EMBEDDED_L1_ENGINE_PORT = 18551
L2_RPC_PORT = 18688
L2_ENGINE_PORT = 18684
L2_P2P_PORT = 30640
L1_XCHAIN_PORT = 18999
L2_XCHAIN_PORT = 18998
BUILDER_FLASHBOTS_RPC_PORT = 8645
PROOF_SIGNER_GRPC_PORT = 50061
L2_CHAIN_ID = "6290"


def run(plan, args):
    eth_args = args["ethereum_package"]
    eez = args.get("eez", {})
    enable_explorers = eez.get("enable_explorers", False)

    poster_key = eez.get("poster_key", "")
    proof_signer_key = eez.get("proof_signer_key", "")
    l2_system_key = eez.get("l2_system_key", "")
    if (
        poster_key in ["", "0xCHANGE_ME"]
        or proof_signer_key in ["", "0xCHANGE_ME"]
        or l2_system_key in ["", "0xCHANGE_ME"]
    ):
        fail(
            "set eez.poster_key, eez.proof_signer_key, and eez.l2_system_key in the args file "
            + "(set deterministic test keys in the selected args file)"
        )

    # Pair B: canonical L1, validators, and MEV stack.
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
            fail(
                "no rbuilder participant found (service name containing 'builder'); "
                + "set eez.builder_rpc_url explicitly"
            )
        builder_rpc = "http://{}:{}".format(
            builder_el.dns_name, BUILDER_FLASHBOTS_RPC_PORT
        )

    chain_id = str(eth_args.get("network_params", {}).get("network_id", "7331"))
    l2_chain_id = L2_CHAIN_ID

    # Pair A engine API JWT.
    jwt = plan.run_sh(
        description="mint engine-API JWT (embedded reth <-> follower)",
        image="alpine:3.20",
        run="mkdir -p /jwt && tr -dc 'a-f0-9' < /dev/urandom | head -c 64 > /jwt/jwtsecret",
        store=[StoreSpec(src="/jwt/jwtsecret", name="eez-jwt")],
    )

    # Deploy protocol contracts and emit /out/deployments.env + /out/l2-genesis.json.
    deploy = plan.run_sh(
        description="deploy EEZ contracts + generate L2 genesis on the shared L1",
        image=eez.get("deploy_image", "eez-deploy:dev"),
        env_vars={
            "EEZ_L1_RPC_URL": l1_el.rpc_http_url,
            "EEZ_L1_POSTER_KEY": poster_key,
            "EEZ_PROOF_SIGNER_KEY": proof_signer_key,
            "EEZ_L2_SYSTEM_KEY": l2_system_key,
            "EEZ_DEPLOYMENTS_FILE": "/out/deployments.env",
            "EEZ_GENESIS_OUT": "/out/l2-genesis.json",
        },
        run="mkdir -p /out && bash /repo/scripts/deploy.sh && cp -R /repo/contracts/broadcast /out/foundry-broadcast",
        store=[StoreSpec(src="/out", name="eez-deployments")],
        wait="900s",
    )

    signer_cmd = " ".join(
        [
            "set -eu;",
            "test -f /out/deployments.env;",
            "set -a; . /out/deployments.env; set +a;",
            "exec eez-proof-signer",
            "--listen-addr=0.0.0.0:{}".format(PROOF_SIGNER_GRPC_PORT),
            "--chain-config=/out/l2-genesis.json",
        ]
    )

    plan.add_service(
        name="eez-proof-signer",
        config=ServiceConfig(
            image=eez.get("proof_signer_image", "eez-proof-signer:dev"),
            ports={
                "grpc": PortSpec(
                    number=PROOF_SIGNER_GRPC_PORT,
                    transport_protocol="TCP",
                    wait="2m",
                ),
            },
            files={
                "/out": deploy.files_artifacts[0],
            },
            env_vars={
                "EEZ_PROOF_SIGNER_KEY": proof_signer_key,
                "EEZ_L2_SYSTEM_KEY": l2_system_key,
                "RUST_LOG": eez.get("proof_signer_rust_log", "info"),
            },
            entrypoint=["/bin/sh", "-c"],
            cmd=[signer_cmd],
        ),
    )

    # The deployment-specific values are expanded into one file before exec;
    # eez-composer itself receives no EEZ_* environment configuration.
    node_config = "\n".join(
        [
            'l2_system_key = "{}"'.format(l2_system_key),
            "max_speculative_depth = {}".format(eez.get("max_speculative_depth", 0)),
            "",
            "[l1]",
            'rpc_url = "http://127.0.0.1:{}"'.format(EMBEDDED_L1_RPC_PORT),
            "chain_id = {}".format(chain_id),
            'registry_address = "$EEZ_REGISTRY_ADDRESS"',
            "registry_deploy_block = $EEZ_REGISTRY_DEPLOY_BLOCK",
            "rollup_id = $EEZ_ROLLUP_ID",
            "",
            "[timing]",
            "l1_block_time_ms = {}".format(eez.get("l1_block_time_ms", 12000)),
            "l2_block_time_ms = {}".format(eez.get("l2_block_time_ms", 2000)),
            "proof_time_ms = {}".format(eez.get("proof_time_ms", 5000)),
            "submission_slack_ms = {}".format(eez.get("submission_slack_ms", 2500)),
            "",
            "[prover]",
            'url = "http://eez-proof-signer:{}"'.format(PROOF_SIGNER_GRPC_PORT),
            'attester_address = "$EEZ_ATTESTER_ADDRESS"',
            "",
            "[submission]",
            'builder_rpc_url = "{}"'.format(builder_rpc),
            'target_rpc_url = "{}"'.format(l1_el.rpc_http_url),
            'poster_key = "{}"'.format(poster_key),
            'proof_system_address = "$EEZ_ECDSA_PROOF_SYSTEM_ADDRESS"',
            "",
            "[cross_chain]",
            "l1_port = {}".format(L1_XCHAIN_PORT),
            "l2_port = {}".format(L2_XCHAIN_PORT),
            "",
            "[embedded_l1]",
            'kind = "devnet"',
            'chain = "/genesis/genesis.json"',
            'datadir = "/data/embedded-l1"',
            "http_port = {}".format(EMBEDDED_L1_RPC_PORT),
            "auth_port = {}".format(EMBEDDED_L1_ENGINE_PORT),
            "p2p_port = 30444",
            'jwt_secret = "/jwt/jwtsecret"',
            'trusted_peers = ["{}"]'.format(l1_el.enode),
        ]
    )

    node_exec = " ".join(
        [
            "exec eez-composer node",
            "--chain=/out/l2-genesis.json",
            "--datadir=/data/l2",
            "--eez.config=/tmp/eez-composer.toml",
            "--http --http.addr=0.0.0.0 --http.port={} --http.api=eth,net,web3,debug,trace".format(L2_RPC_PORT),
            "--authrpc.addr=127.0.0.1 --authrpc.port={}".format(L2_ENGINE_PORT),
            "--port={} --discovery.port={}".format(L2_P2P_PORT, L2_P2P_PORT),
            "--discovery.v5.port={}".format(L2_P2P_PORT + 1),
            "--ipcdisable --disable-discovery",
        ]
    )
    node_cmd = "\n".join(
        [
            "set -eu",
            "test -f /out/deployments.env",
            ". /out/deployments.env",
            "cat > /tmp/eez-composer.toml <<EOF",
            node_config,
            "EOF",
            node_exec,
        ]
    )

    # Follower beacon drives eez-node's embedded reth over the engine API.
    plan.add_service(
        name="eez-node",
        config=ServiceConfig(
            image=eez.get("eez_node_image", "eez-node:dev"),
            ports={
                "l1-engine": PortSpec(
                    number=EMBEDDED_L1_ENGINE_PORT, transport_protocol="TCP"
                ),
                "l2-rpc": PortSpec(
                    number=L2_RPC_PORT,
                    transport_protocol="TCP",
                    application_protocol="http",
                ),
                "l1-xchain": PortSpec(
                    number=L1_XCHAIN_PORT,
                    transport_protocol="TCP",
                    application_protocol="http",
                ),
                "l2-xchain": PortSpec(
                    number=L2_XCHAIN_PORT,
                    transport_protocol="TCP",
                    application_protocol="http",
                ),
            },
            files={
                "/out": deploy.files_artifacts[0],
                "/genesis": "el_cl_genesis_data",
                "/jwt": jwt.files_artifacts[0],
            },
            env_vars={"RUST_LOG": eez.get("eez_node_rust_log", "info")},
            entrypoint=["/bin/sh", "-c"],
            cmd=[node_cmd],
        ),
    )

    plan.add_service(
        name="eez-follower",
        config=ServiceConfig(
            image=eez.get("follower_image", "sigp/lighthouse:v8.1.2"),
            private_ip_address_placeholder="FOLLOWER_IP",
            ports={
                "http": PortSpec(
                    number=5252, transport_protocol="TCP", application_protocol="http"
                ),
            },
            files={
                "/testnet": "el_cl_genesis_data",
                "/jwt": jwt.files_artifacts[0],
            },
            entrypoint=["lighthouse"],
            cmd=[
                "beacon_node",
                "--testnet-dir=/testnet",
                "--datadir=/data",
                "--execution-endpoint=http://eez-node:{}".format(
                    EMBEDDED_L1_ENGINE_PORT
                ),
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
                "--suggested-fee-recipient="
                + eez.get(
                    "fee_recipient", "0x0000000000000000000000000000000000000000"
                ),
            ],
        ),
    )

    if enable_explorers:
        explorer_params = {
            "postgres_image": eez.get("blockscout_postgres_image", "postgres:alpine"),
            "backend_image": eez.get(
                "blockscout_image", "ghcr.io/blockscout/blockscout:latest"
            ),
            "verifier_image": eez.get(
                "blockscout_verifier_image",
                "ghcr.io/blockscout/smart-contract-verifier:latest",
            ),
            "frontend_image": eez.get(
                "blockscout_frontend_image", "ghcr.io/blockscout/frontend:latest"
            ),
        }
        blockscout.launch(
            plan=plan,
            prefix="l1",
            network_name="EEZ L1",
            chain_id=chain_id,
            rpc_url=l1_el.rpc_http_url,
            params=explorer_params,
        )
        l2_explorer_params = dict(explorer_params)
        # EEZL2 is a genesis predeploy, so Blockscout must import the L2 alloc
        # to classify it as a contract before accepting source verification.
        # The Geth adapter understands alloc-based genesis files and uses the
        # debug namespace exposed by the L2 Reth node for internal calls.
        l2_explorer_params["json_rpc_variant"] = "geth"
        l2_explorer_params["chain_spec_artifact"] = deploy.files_artifacts[0]
        l2_explorer_params["chain_spec_path"] = "/chain-spec/l2-genesis.json"
        blockscout.launch(
            plan=plan,
            prefix="l2",
            network_name="EEZ L2",
            chain_id=l2_chain_id,
            rpc_url="http://eez-node:{}/".format(L2_RPC_PORT),
            params=l2_explorer_params,
        )

    plan.print(
        "EEZ local network ready, L1 chain_id={}, L2 chain_id={}".format(
            chain_id, l2_chain_id
        )
    )
