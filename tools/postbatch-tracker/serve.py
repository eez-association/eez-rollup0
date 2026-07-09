#!/usr/bin/env python3
"""Tiny static server + CORS-adding RPC proxy for the PostBatch tracker.

Reads the deployment config from `.env` and `deployments.env` (found by walking
up from this file) — no parameters needed. The EEZ L1 node does not send
Access-Control-Allow-Origin, so a browser page can't call it cross-origin; this
serves index.html at /, proxies POST /rpc to EEZ_L1_RPC_URL injecting CORS, and
exposes GET /config so the page picks up the registry address / deploy block.

Usage:
    ./serve.py                       # everything from .env / deployments.env
    ./serve.py --port 8080           # override the listen port only
"""
import argparse
import http.server
import json
import os
import urllib.request
from functools import partial

HERE = os.path.dirname(os.path.abspath(__file__))


def find_repo_root():
    """Walk up from HERE until a dir holding .env or deployments.env is found."""
    d = HERE
    while True:
        if os.path.exists(os.path.join(d, ".env")) or os.path.exists(os.path.join(d, "deployments.env")):
            return d
        parent = os.path.dirname(d)
        if parent == d:
            return HERE
        d = parent


def parse_env_file(path):
    out = {}
    if not os.path.exists(path):
        return out
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            out[key.strip()] = val.strip().strip('"').strip("'")
    return out


def load_abi(root):
    """Read the EEZ contract ABI from the Foundry artifact, if present."""
    candidates = [
        os.path.join(root, "contracts", "out", "EEZ.sol", "EEZ.json"),
        os.path.join(root, "sync-rollups-protocol", "out", "EEZ.sol", "EEZ.json"),
        os.path.join(root, "out", "EEZ.sol", "EEZ.json"),
    ]
    for path in candidates:
        if os.path.exists(path):
            try:
                return json.load(open(path)).get("abi", [])
            except Exception:  # noqa: BLE001
                pass
    return []


def load_deployment_config():
    """Merge .env then deployments.env (latter wins), plus real env vars (win)."""
    root = find_repo_root()
    cfg = {}
    cfg.update(parse_env_file(os.path.join(root, ".env")))
    cfg.update(parse_env_file(os.path.join(root, "deployments.env")))
    for k in (
        "EEZ_TOOL_L1_RPC_URL",
        "EEZ_TOOL_L2_RPC_URL",
        "EEZ_L1_RPC_URL",
        "EEZ_L2_RPC_URL",
        "EEZ_REGISTRY_ADDRESS",
        "EEZ_REGISTRY_DEPLOY_BLOCK",
        "EEZ_L1_EXPLORER_URL",
    ):
        if os.environ.get(k):
            cfg[k] = os.environ[k]
    return root, cfg


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *a, upstreams=None, page_config=None, abi=None, **kw):
        self.upstreams = upstreams or {}  # path -> upstream RPC URL
        self.page_config = page_config or {}
        self.abi = abi or []
        super().__init__(*a, directory=HERE, **kw)

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _json(self, payload):
        body = json.dumps(payload).encode()
        self.send_response(200)
        self._cors()
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        if self.path.rstrip("/") == "/config":
            self._json(self.page_config)
            return
        if self.path.rstrip("/") == "/abi":
            self._json(self.abi)
            return
        super().do_GET()

    def do_POST(self):
        upstream = self.upstreams.get(self.path.rstrip("/"))
        if not upstream:
            self.send_error(404)
            return
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        try:
            req = urllib.request.Request(
                upstream, data=body,
                headers={"Content-Type": "application/json"}, method="POST",
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                payload = resp.read()
        except Exception as e:  # noqa: BLE001
            payload = json.dumps(
                {"jsonrpc": "2.0", "id": None,
                 "error": {"code": -32000, "message": f"proxy error: {e}"}}
            ).encode()
        self.send_response(200)
        self._cors()
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *a):  # quieter
        pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8080)
    args = ap.parse_args()

    root, cfg = load_deployment_config()
    upstream = cfg.get("EEZ_TOOL_L1_RPC_URL") or "http://127.0.0.1:18645"
    abi = load_abi(root)
    # L2 RPC (optional) — lets the page resolve real L2 block numbers per batch.
    l2_upstream = cfg.get("EEZ_TOOL_L2_RPC_URL") or "http://127.0.0.1:18688"
    upstreams = {"/rpc": upstream}
    if l2_upstream:
        upstreams["/l2rpc"] = l2_upstream
    page_config = {
        "rpc": "/rpc",  # the page talks to our CORS proxy, not the node directly
        "l2rpc": "/l2rpc" if l2_upstream else "",
        "registry": cfg.get("EEZ_REGISTRY_ADDRESS", ""),
        "fromBlock": cfg.get("EEZ_REGISTRY_DEPLOY_BLOCK", "0"),
        # Block explorer base (Chiado / chainId 10200). Override with EEZ_L1_EXPLORER_URL.
        "explorer": cfg.get("EEZ_L1_EXPLORER_URL", "https://gnosis-chiado.blockscout.com"),
    }

    httpd = http.server.ThreadingHTTPServer(
        ("0.0.0.0", args.port),
        partial(Handler, upstreams=upstreams, page_config=page_config, abi=abi),
    )
    print(f"PostBatch tracker → http://localhost:{args.port}/")
    print(f"config from        {root}/{{.env,deployments.env}}")
    print(f"proxying /rpc →    {upstream}")
    print(f"proxying /l2rpc →  {l2_upstream or '(disabled)'}")
    print(f"registry           {page_config['registry'] or '(unset)'} @ block {page_config['fromBlock']}")
    print(f"explorer           {page_config['explorer']}")
    print(f"abi                {'loaded (' + str(len(abi)) + ' items)' if abi else 'NOT FOUND — calldata decode disabled'}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
