# A small, prefix-aware Blockscout launcher for disposable Kurtosis networks.
# The upstream ethereum-package launcher uses fixed service names, so it cannot
# be instantiated once for L1 and again for L2 in the same enclave.

POSTGRES_PORT = 5432
VERIFIER_PORT = 8050
BACKEND_PORT = 4000
FRONTEND_PORT = 3000
DEFAULT_FAVICON_URL = "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIxODAiIGhlaWdodD0iMTgwIiB2aWV3Qm94PSIwIDAgMTgwIDE4MCI+PHJlY3Qgd2lkdGg9IjE4MCIgaGVpZ2h0PSIxODAiIHJ4PSIzMiIgZmlsbD0iIzFhMjAyYyIvPjxwYXRoIGZpbGw9IiNmZmZmZmYiIGQ9Ik00MiA0Mmg5NnYyMkg2NnYxNmg2MHYyMkg2NnYxNmg3MnYyMkg0MnoiLz48L3N2Zz4="


def launch(plan, prefix, network_name, chain_id, rpc_url, params={}):
    postgres_name = prefix + "-blockscout-postgres"
    verifier_name = prefix + "-blockscout-verif"
    backend_name = prefix + "-blockscout"
    frontend_name = prefix + "-blockscout-frontend"

    postgres = plan.add_service(
        name=postgres_name,
        config=ServiceConfig(
            image=params.get("postgres_image", "postgres:alpine"),
            ports={
                "postgres": PortSpec(
                    number=POSTGRES_PORT,
                    transport_protocol="TCP",
                    wait="2m",
                ),
            },
            env_vars={
                "POSTGRES_DB": "blockscout",
                "POSTGRES_USER": "blockscout",
                "POSTGRES_PASSWORD": "blockscout",
            },
        ),
    )

    verifier = plan.add_service(
        name=verifier_name,
        config=ServiceConfig(
            image=params.get(
                "verifier_image",
                "ghcr.io/blockscout/smart-contract-verifier:latest",
            ),
            ports={
                "http": PortSpec(
                    number=VERIFIER_PORT,
                    transport_protocol="TCP",
                    application_protocol="http",
                    wait="2m",
                ),
            },
            env_vars={
                "SMART_CONTRACT_VERIFIER__SERVER__HTTP__ADDR": "0.0.0.0:{}".format(
                    VERIFIER_PORT
                ),
            },
        ),
    )

    database_url = "postgresql://blockscout:blockscout@{}:{}/blockscout".format(
        postgres.hostname,
        POSTGRES_PORT,
    )
    verifier_url = "http://{}:{}/".format(verifier.hostname, VERIFIER_PORT)

    # Kurtosis assigns the published frontend port at runtime. Blockscout's
    # proxy URL builder otherwise uses the container port from the environment
    # in the browser (127.0.0.1:3000), so requests from the published URL miss
    # the service and are rejected by the frontend's CSP. The image entrypoint
    # generates envs.js before executing this command; make its browser-facing
    # origin follow the page that was actually opened.
    frontend_cmd = " ".join(
        [
            "set -euo pipefail;",
            "env_file=public/assets/envs.js;",
            "sed -i",
            "-e 's|NEXT_PUBLIC_APP_PROTOCOL: \"http\",|NEXT_PUBLIC_APP_PROTOCOL: window.location.protocol.slice(0, -1),|'",
            "-e 's|NEXT_PUBLIC_APP_HOST: \"127.0.0.1\",|NEXT_PUBLIC_APP_HOST: window.location.hostname,|'",
            "-e 's|NEXT_PUBLIC_APP_PORT: \"{}\",|NEXT_PUBLIC_APP_PORT: window.location.port,|'".format(
                FRONTEND_PORT
            ),
            '"$env_file";',
            "grep -q 'NEXT_PUBLIC_APP_HOST: window.location.hostname,' \"$env_file\";",
            "grep -q 'NEXT_PUBLIC_APP_PORT: window.location.port,' \"$env_file\";",
            "exec node server.js",
        ]
    )

    backend_files = {}
    backend_env = {
        "ETHEREUM_JSONRPC_VARIANT": params.get("json_rpc_variant", "erigon"),
        "ETHEREUM_JSONRPC_HTTP_URL": rpc_url,
        "ETHEREUM_JSONRPC_TRACE_URL": rpc_url,
        "DATABASE_URL": database_url,
        "CHAIN_ID": str(chain_id),
        "COIN": "ETH",
        # A disposable private network has no meaningful market price,
        # and does not need public-API rate limiting.
        "DISABLE_MARKET": "true",
        "API_RATE_LIMIT_DISABLED": "true",
        "MICROSERVICE_SC_VERIFIER_ENABLED": "true",
        "MICROSERVICE_SC_VERIFIER_URL": verifier_url,
        "MICROSERVICE_SC_VERIFIER_TYPE": "sc_verifier",
        "INDEXER_DISABLE_PENDING_TRANSACTIONS_FETCHER": "true",
        "ECTO_USE_SSL": "false",
        "NETWORK": network_name,
        "SUBNETWORK": network_name,
        "PORT": str(BACKEND_PORT),
        "SECRET_KEY_BASE": "56NtB48ear7+wMSf0IQuWDAAazhpb31qyc7GiyspBP2vh7t5zlCsF5QDv76chXeN",
    }

    chain_spec_artifact = params.get("chain_spec_artifact")
    if chain_spec_artifact != None:
        backend_files["/chain-spec"] = chain_spec_artifact
        backend_env["CHAIN_SPEC_PATH"] = params.get(
            "chain_spec_path", "/chain-spec/l2-genesis.json"
        )
        backend_env["CHAIN_SPEC_PROCESSING_DELAY"] = params.get(
            "chain_spec_processing_delay", "1s"
        )

    backend = plan.add_service(
        name=backend_name,
        config=ServiceConfig(
            image=params.get("backend_image", "ghcr.io/blockscout/blockscout:latest"),
            ports={
                "http": PortSpec(
                    number=BACKEND_PORT,
                    transport_protocol="TCP",
                    application_protocol="http",
                    wait="5m",
                ),
            },
            files=backend_files,
            env_vars=backend_env,
            cmd=[
                "/bin/sh",
                "-c",
                'bin/blockscout eval "Elixir.Explorer.ReleaseTasks.create_and_migrate()" && bin/blockscout start',
            ],
        ),
    )

    plan.add_service(
        name=frontend_name,
        config=ServiceConfig(
            image=params.get("frontend_image", "ghcr.io/blockscout/frontend:latest"),
            ports={
                "http": PortSpec(
                    number=FRONTEND_PORT,
                    transport_protocol="TCP",
                    application_protocol="http",
                    wait="5m",
                ),
            },
            env_vars={
                "HOSTNAME": "0.0.0.0",
                "NEXT_PUBLIC_API_PROTOCOL": "http",
                "NEXT_PUBLIC_API_WEBSOCKET_PROTOCOL": "ws",
                "NEXT_PUBLIC_NETWORK_NAME": network_name,
                "NEXT_PUBLIC_NETWORK_SHORT_NAME": network_name,
                "NEXT_PUBLIC_NETWORK_ID": str(chain_id),
                "NEXT_PUBLIC_NETWORK_RPC_URL": rpc_url,
                "NEXT_PUBLIC_API_HOST": "{}:{}".format(backend.hostname, BACKEND_PORT),
                "NEXT_PUBLIC_AD_BANNER_PROVIDER": "none",
                "NEXT_PUBLIC_AD_TEXT_PROVIDER": "none",
                "NEXT_PUBLIC_IS_TESTNET": "true",
                "NEXT_PUBLIC_GAS_TRACKER_ENABLED": "true",
                "NEXT_PUBLIC_HAS_BEACON_CHAIN": "false",
                # The in-enclave RPC hostname is not resolvable by a browser on
                # the host, so do not advertise wallet actions from the UI.
                "NEXT_PUBLIC_WEB3_WALLETS": "none",
                "NEXT_PUBLIC_NETWORK_VERIFICATION_TYPE": "validation",
                "FAVICON_MASTER_URL": params.get("favicon_url", DEFAULT_FAVICON_URL),
                "NEXT_PUBLIC_APP_PROTOCOL": "http",
                "NEXT_PUBLIC_APP_HOST": "127.0.0.1",
                "NEXT_PUBLIC_APP_PORT": str(FRONTEND_PORT),
                "NEXT_PUBLIC_USE_NEXT_JS_PROXY": "true",
                "PORT": str(FRONTEND_PORT),
            },
            cmd=["/bin/bash", "-c", frontend_cmd],
        ),
    )
