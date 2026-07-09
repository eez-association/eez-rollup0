#!/usr/bin/env python3
"""Serve EEZ local tooling with same-origin JSON-RPC proxies.

Usage:
    python3 tools/server.py
    python3 tools/server.py --port 18080

The browser tools can be served by any static server, but live checks need
same-origin proxy routes because local RPC endpoints usually do not send CORS
headers.
"""

from __future__ import annotations

import argparse
import http.server
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent


DEPLOYMENT_KEYS = (
    "EEZ_REGISTRY_ADDRESS",
    "EEZ_REGISTRY_DEPLOY_BLOCK",
    "EEZ_PROOF_SYSTEM_KIND",
    "EEZ_PROOF_SYSTEM_ADDRESS",
    "EEZ_ROLLUP_MANAGER_ADDRESS",
    "EEZ_ROLLUP_ID",
    "EEZ_INITIAL_STATE_ROOT",
    "EEZ_L1_L2_PROXY",
    "EEZ_L1_BRIDGE_SENDER",
    "EEZ_L2_BRIDGE_RECEIVER",
    "EEZ_CCM_L2_ADDRESS",
    "EEZ_L2_SYSTEM_ADDRESS",
)


def parse_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    with path.open(encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip().strip("\"'")
    return values


def load_config() -> dict[str, str]:
    cfg: dict[str, str] = {}
    cfg.update(parse_env(REPO_ROOT / ".env"))
    cfg.update(parse_env(REPO_ROOT / "deployments.env"))
    cfg.update({k: v for k, v in os.environ.items() if k.startswith("EEZ_")})
    return cfg


def localhost_url(port: str) -> str:
    return f"http://127.0.0.1:{port}"


def rpc_targets(cfg: dict[str, str]) -> dict[str, str]:
    inbound_port = cfg.get("EEZ_L1_XCHAIN_PORT", "18999")
    outbound_port = cfg.get("EEZ_L2_XCHAIN_PORT", "18998")
    return {
        "l1": cfg.get("EEZ_TOOL_L1_RPC_URL") or localhost_url("18645"),
        "l2": cfg.get("EEZ_TOOL_L2_RPC_URL") or localhost_url("18688"),
        "inbound": cfg.get("EEZ_INBOUND_RPC_URL")
        or cfg.get("EEZ_L1_XCHAIN_RPC_URL")
        or localhost_url(inbound_port),
        "outbound": cfg.get("EEZ_OUTBOUND_RPC_URL")
        or cfg.get("EEZ_L2_XCHAIN_RPC_URL")
        or localhost_url(outbound_port),
    }


def deployment_config(cfg: dict[str, str], targets: dict[str, str]) -> dict[str, object]:
    return {
        "targets": targets,
        "deployment": {key: cfg.get(key, "") for key in DEPLOYMENT_KEYS},
        "ports": {
            "l1": "18645",
            "l2": "18688",
            "inbound": cfg.get("EEZ_L1_XCHAIN_PORT", "18999"),
            "outbound": cfg.get("EEZ_L2_XCHAIN_PORT", "18998"),
        },
        "explorer": cfg.get("EEZ_L1_EXPLORER_URL", "https://gnosis-chiado.blockscout.com"),
    }


class Handler(http.server.SimpleHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def __init__(
        self,
        *args,
        targets: dict[str, str],
        aliases: dict[str, str],
        page_config: dict[str, object],
        **kwargs,
    ):
        self.targets = targets
        self.aliases = aliases
        self.page_config = page_config
        super().__init__(*args, directory=str(HERE), **kwargs)

    def end_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        super().end_headers()

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self) -> None:
        path = urllib.parse.urlparse(self.path).path.rstrip("/")
        if path in ("", "/"):
            self.redirect("/rpc-info/")
            return
        if path == "/health":
            self.write_json({"ok": True, "targets": self.targets})
            return
        if path == "/config":
            self.write_json(self.page_config)
            return
        super().do_GET()

    def do_POST(self) -> None:
        name = urllib.parse.urlparse(self.path).path.removeprefix("/rpc/").strip("/")
        target = self.targets.get(name) or self.aliases.get(name)
        if not target:
            self.send_error(404, f"unknown RPC proxy target: {name}")
            return

        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        try:
            req = urllib.request.Request(
                target,
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                payload = resp.read()
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            payload = json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {"code": -32000, "message": f"proxy error for {name}: {exc}"},
                }
            ).encode("utf-8")

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def redirect(self, location: str) -> None:
        self.send_response(302)
        self.send_header("Location", location)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def write_json(self, payload: object) -> None:
        body = json.dumps(payload, indent=2).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=18080)
    args = parser.parse_args()

    cfg = load_config()
    targets = rpc_targets(cfg)
    aliases = {
        "raw-l1": targets["l1"],
        "raw-l2": targets["l2"],
        "l1-l2": targets["inbound"],
        "l2-l1": targets["outbound"],
        # Backward-compatible names from feat/crosschain-on-main. This branch
        # has one rollup and explicit direction-specific fronts, not C1/C2 B0s.
        "c1-l2": targets["l2"],
        "c1-l1-l2": targets["inbound"],
        "c1-b0": targets["inbound"],
    }
    page_config = deployment_config(cfg, targets)

    class BoundHandler(Handler):
        def __init__(self, *handler_args, **handler_kwargs):
            super().__init__(
                *handler_args,
                targets=targets,
                aliases=aliases,
                page_config=page_config,
                **handler_kwargs,
            )

    server = http.server.ThreadingHTTPServer((args.host, args.port), BoundHandler)
    print(f"Serving EEZ tools on http://{args.host}:{args.port}/rpc-info/")
    for name, target in targets.items():
        print(f"proxy /rpc/{name:<8} -> {target}")
    server.serve_forever()


if __name__ == "__main__":
    main()
