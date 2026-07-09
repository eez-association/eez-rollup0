# EEZ Local Tools

Small browser tools for a running local Docker deployment.

## RPC Info

```bash
python3 tools/server.py
```

Open <http://localhost:18080/rpc-info/>.

The server reads `.env` and `deployments.env`, serves static files from
`tools/`, and exposes same-origin JSON-RPC proxies:

- `/rpc/l1` -> raw Chiado L1 RPC, default `http://127.0.0.1:18645`
- `/rpc/l2` -> raw EEZ L2 RPC, default `http://127.0.0.1:18688`
- `/rpc/inbound` -> L1-to-L2 front, default `http://127.0.0.1:18999`
- `/rpc/outbound` -> L2-to-L1 front, default `http://127.0.0.1:18998`

Deployment metadata still comes from `.env` and `deployments.env`, but the RPC
targets default to the local Docker ports. Override browser-tool targets with
`EEZ_TOOL_L1_RPC_URL`, `EEZ_TOOL_L2_RPC_URL`, `EEZ_INBOUND_RPC_URL`, or
`EEZ_OUTBOUND_RPC_URL`.

## PostBatch Tracker

```bash
cd tools/postbatch-tracker
./serve.py
```

Open <http://localhost:8080/>.

The tracker scans `BatchPosted` events from `EEZ_REGISTRY_DEPLOY_BLOCK`, proxies
L1 RPC at `/rpc`, proxies L2 RPC at `/l2rpc`, and decodes postBatch calldata when
the Foundry artifact exists at `contracts/out/EEZ.sol/EEZ.json`.

## Notes

The source branch had C1/C2 and B0 endpoint names. This branch uses one rollup
with explicit direction-specific cross-chain fronts, so the imported endpoint
tooling has been reorganized around `l1`, `l2`, `inbound`, and `outbound`.
