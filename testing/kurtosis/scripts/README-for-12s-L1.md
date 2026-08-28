# Kurtosis L1 stack — setup runbook

How to launch the 12s Kurtosis devnet L1 that an external eez L2 runs against,
and how to pull the enode / peer-id / genesis the L2 needs to attach.

Two-machine split: **Machine 1** runs the Kurtosis enclave, **Machine 2** runs
the eez stack (composer + embedded reth L1 + follower CL + L2). Both can be the
same box; the fixed-port forwarding below makes the split work either way.

## What the enclave contains

`testing/kurtosis/l1-only-args.yaml` is the upstream `ethereum-package`
`ethereum_package:` block promoted to top level — no bundled L2:

- `el-1-reth-lighthouse` + `cl-1-lighthouse-reth` — the validator pair, canonical EL
- `el-2-reth-builder-lighthouse` + `cl-2-…` — the flashbots builder pair (rbuilder)
- `mev-relay-api` / `mev-boost-1-…` — the relay the bundles go through
- `dora`, `blockscout` — visibility only

Chain **7331**, **12s** slots, 60M gas limit, deneb + electra at epoch 0. Hardhat
accounts #0–#2 and the builder coinbase are prefunded 1M ETH each via
`prefunded_accounts` — #0 `0xf39F…` is poster / L2-system, #1 `0x7099…` is proof
signer, #2 `0x3C44…` is target deployer.

> `prefunded_accounts` must stay single-line compact JSON — the genesis
> generator's `yq` rejects a folded scalar.

## 1. Launch (Machine 1)

`testing/kurtosis/scripts/12s-l1-up.sh` is the whole runbook in one idempotent script: it
tears down the old enclave + forwarders, launches, resolves ports, forwards them
to fixed ones, resolves the enode + CL peer-id, downloads the genesis artifact,
and writes `machine2.env`.

```bash
cd /root/eez-rollup0
bash testing/kurtosis/scripts/12s-l1-up.sh
```

Overrides: `KURTOSIS_ENCLAVE` (default `eez-l1-12s`), `KURTOSIS_ARGS_FILE`
(default `testing/kurtosis/l1-only-args.yaml`), `IP1` (default = autodetected
source IP), `BIND_ADDR` (default `0.0.0.0`).

`BIND_ADDR=0.0.0.0` publishes **unauthenticated** EL and builder RPC on every
network this host is attached to. That is the point on a throwaway devnet behind
a firewall; anywhere else, set `BIND_ADDR` to the private address Machine 2
reaches this host by.

Requires on PATH: `kurtosis docker cast jq curl socat openssl`
(`testing/kurtosis/scripts/12s-l1-up.sh:27`).

The raw command underneath, if you want the enclave without the plumbing:

```bash
kurtosis run github.com/ethpandaops/ethereum-package \
  --enclave eez-l1-12s \
  --args-file testing/kurtosis/l1-only-args.yaml
```

Teardown:

```bash
kurtosis enclave rm -f eez-l1-12s
# The forwarders this script started, and only those ("PORT PID" per line).
awk '{print $2}' .12s-l1-forwarders.pids | xargs -r kill
rm -f .12s-l1-forwarders.pids
docker rm -f bs-frontend-public
```

## 2. Resolving ports

Kurtosis assigns **random host ports per enclave instance** — they change on
every recreate.

```bash
kurtosis enclave ls                            # is it up?
kurtosis enclave inspect eez-l1-12s            # every service + port at once

kurtosis port print eez-l1-12s el-1-reth-lighthouse rpc
kurtosis port print eez-l1-12s el-1-reth-lighthouse tcp-discovery
kurtosis port print eez-l1-12s cl-1-lighthouse-reth http
kurtosis port print eez-l1-12s cl-1-lighthouse-reth tcp-discovery
kurtosis port print eez-l1-12s el-2-reth-builder-lighthouse rbuilder-rpc
kurtosis port print eez-l1-12s dora http
```

> `testing/kurtosis/l1-endpoints.env` holds the *forwarded* fixed ports below, so
> it survives a recreate — but only while the step [4/7] forwarders are up. The
> enclave's own random ports are never valid across a recreate; re-resolve those.

Random ports are useless to a second machine, so step [4/7] socat-forwards them
onto fixed ports on `BIND_ADDR` (default `0.0.0.0`). The step fails loudly if any
of these ports is already taken, rather than handing Machine 2 dead endpoints:

| Fixed port | Service | Consumer |
|---|---|---|
| 8545 | canonical EL RPC (`el-1`) | `EEZ_L1_TARGET_RPC_URL` — tip oracle for N+1 targeting |
| 8645 | rbuilder RPC | `EEZ_L1_BUILDER_RPC_URL` — `eth_sendBundle` |
| 5052 | beacon HTTP (`cl-1`) | `L1_BEACON_URL` — follower checkpoint-sync |
| 30303 | EL P2P | the enode in `EEZ_L1_TRUSTED_PEERS` |
| 9010 | CL P2P | the multiaddr in `CL_MULTIADDR` |
| 8080 | dora | browser |
| 9060 | mev-relay website | browser |
| 4001 / 4000 | blockscout API / public frontend | browser |

## 3. Getting the enode

```bash
cast rpc admin_nodeInfo --rpc-url http://127.0.0.1:$EL_RPC | jq -r .enode
```

returns something like `enode://11056bed…76c0b@172.16.0.10:30303` — **that host
is the container-internal IP and is unusable from outside the enclave.** Keep
only the pubkey and re-attach the public host plus the forwarded P2P port
(`testing/kurtosis/scripts/12s-l1-up.sh:88`):

```bash
PUBKEY=$(cast rpc admin_nodeInfo --rpc-url http://127.0.0.1:$EL_RPC \
         | jq -r .enode | sed -E 's#enode://([0-9a-f]+)@.*#\1#')
echo "enode://$PUBKEY@$IP1:30303"
```

The CL needs a **peer id**, not an enode:

```bash
PEERID=$(curl -s http://127.0.0.1:$CL_HTTP/eth/v1/node/identity | jq -r .data.peer_id)
echo "/ip4/$IP1/tcp/9010/p2p/$PEERID"
```

`l1-genesis/bootstrap_nodes.txt` holds an ENR, but it encodes the internal IP
too — use the constructed enode instead.

## 4. Genesis artifact

```bash
kurtosis files download eez-l1-12s el_cl_genesis_data ./l1-genesis
```

One artifact, both halves:

- `genesis.json` → `EEZ_L1_CHAIN_PATH` for eez-node's embedded reth
- `config.yaml` + `genesis.ssz` → `--testnet-dir` for the follower lighthouse

## 5. Handing off to Machine 2

Step [7/7] writes `machine2.env` — plain `KEY=VAL`, so it works both as a
`docker compose --env-file` and with `set -a; source machine2.env; set +a`:

```
EEZ_L1_CHAIN_ID=7331
EEZ_L1_BLOCK_TIME_MS=12000
EEZ_L1_TARGET_RPC_URL=http://$IP1:8545
EEZ_L1_BUILDER_RPC_URL=http://$IP1:8645
L1_BEACON_URL=http://$IP1:5052
EEZ_L1_TRUSTED_PEERS=enode://$PUBKEY@$IP1:30303
CL_MULTIADDR=/ip4/$IP1/tcp/9010/p2p/$PEERID
CL_PEERID=$PEERID
```

Ship both to Machine 2:

```bash
scp -r <M1>:/root/eez-rollup0/l1-genesis   ./l1-genesis
scp    <M1>:/root/eez-rollup0/machine2.env .
```

Then, on Machine 2:

```bash
cp .env.kurtosis.example .env.kurtosis     # merge machine2.env values in
openssl rand -hex 32 > ./data/jwt.hex      # embedded reth ↔ follower JWT
make deploy-protocol                       # against EEZ_L1_TARGET_RPC_URL
docker compose --env-file .env.kurtosis -f docker-compose.kurtosis-node.yml up -d
```

`.env.kurtosis.example` documents the rest: host paths (`L1_GENESIS_DIR`,
`L2_DATA_DIR`, `JWT_FILE`, `FRESH_GENESIS`), the funded poster key, and
`EEZ_PROOF_SIGNER_KEY` — which must be the same key used for
`make deploy-protocol`, since its address is the on-chain `authorizedSigner`.

Timing is 12s L1 / 2s L2 → K=6, with `proof + slack = 3000ms < 12000ms`
(`docker-compose.kurtosis-node.yml`).

## Gotchas

- **The JWT is Machine-2-local.** It secures embedded-reth ↔ follower-lighthouse
  only. It is *not* the enclave's `jwt_file` artifact.
- **Recreating the enclave invalidates the handoff.** New enode, new peer-id, new
  genesis, new random ports — re-scp `l1-genesis/`, refresh `machine2.env`, and
  redeploy the protocol.
- **Embedded L1 P2P must not collide with L2 reth.** `EEZ_L1_P2P_PORT=30544` vs
  the L2's `--port=30640`.
- **Fund the poster before a run.** A drained poster looks exactly like a dead
  builder: postBatch is accepted but excluded, `posted` sticks, and the batch
  range grows unbounded.
