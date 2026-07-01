#!/usr/bin/env python3
"""Serve the Rabby/Remix info page with same-origin JSON-RPC proxies.

Usage:
    python3 tools/server.py
    python3 tools/server.py --port 18080

The page can be served by any static server, but browser live checks need these
proxy routes because several upstream RPCs reject CORS preflight requests.
"""

from __future__ import annotations

import argparse
import http.server
import json
import os
import urllib.error
import urllib.request
from pathlib import Path


HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent


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


def rpc_targets(cfg: dict[str, str]) -> dict[str, str]:
    return {
        "c1-l2": cfg.get("EEZ_C1_L2_RPC_URL", "http://127.0.0.1:18688"),
        "c1-l1-l2": cfg.get(
            "EEZ_C1_L1_L2_RPC_URL",
            cfg.get("EEZ_C1_B0_RPC_URL", "http://127.0.0.1:18649"),
        ),
        "c2-l2": cfg.get("EEZ_C2_L2_RPC_URL", "http://127.0.0.1:18788"),
        "c2-l1-l2": cfg.get(
            "EEZ_C2_L1_L2_RPC_URL",
            cfg.get("EEZ_C2_B0_RPC_URL", "http://127.0.0.1:18749"),
        ),
    }


class Handler(http.server.SimpleHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def __init__(self, *args, targets: dict[str, str], aliases: dict[str, str], **kwargs):
        self.targets = targets
        self.aliases = aliases
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
        if self.path in ("", "/"):
            self.send_response(302)
            self.send_header("Location", "/rabbit-remix-info.html")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if self.path.rstrip("/") == "/health":
            self.write_json({"ok": True, "targets": self.targets})
            return
        super().do_GET()

    def do_POST(self) -> None:
        name = self.path.removeprefix("/rpc/").strip("/")
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
        "c1-b0": targets["c1-l1-l2"],
        "c2-b0": targets["c2-l1-l2"],
    }

    class BoundHandler(Handler):
        def __init__(self, *handler_args, **handler_kwargs):
            super().__init__(*handler_args, targets=targets, aliases=aliases, **handler_kwargs)

    server = http.server.ThreadingHTTPServer((args.host, args.port), BoundHandler)
    print(f"Serving tools page on http://{args.host}:{args.port}/rabbit-remix-info.html")
    for name, target in targets.items():
        print(f"proxy /rpc/{name:<5} -> {target}")
    server.serve_forever()


if __name__ == "__main__":
    main()
