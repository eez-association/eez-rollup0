#!/usr/bin/env python3
"""Local devnet eth_sendBundle shim.

This is not a builder relay. It is only a Docker Compose helper that accepts the
bundle shape used by eez-l1, waits until the requested target block is near, and
forwards raw transactions to the local L1 via eth_sendRawTransaction.
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Optional


L1_RPC_URL = os.environ.get("EEZ_BUILDER_STUB_L1_RPC_URL", "http://l1:8545")
HOST = os.environ.get("EEZ_BUILDER_STUB_HOST", "0.0.0.0")
PORT = int(os.environ.get("EEZ_BUILDER_STUB_PORT", "8545"))
POLL_SECS = float(os.environ.get("EEZ_BUILDER_STUB_POLL_SECS", "0.2"))
TARGET_WAIT_SECS = float(os.environ.get("EEZ_BUILDER_STUB_TARGET_WAIT_SECS", "20"))


def rpc_call(method: str, params: Optional[list[Any]] = None) -> dict[str, Any]:
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params or []})
    req = urllib.request.Request(
        L1_RPC_URL,
        data=body.encode("utf-8"),
        headers={"content-type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))


def forward_raw(raw_body: bytes) -> tuple[int, bytes]:
    req = urllib.request.Request(
        L1_RPC_URL,
        data=raw_body,
        headers={"content-type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as err:
        return err.code, err.read()


def wait_for_target_parent(target_hex: str | None) -> None:
    if not target_hex:
        return

    target = int(target_hex, 16)
    deadline = time.monotonic() + TARGET_WAIT_SECS
    while time.monotonic() < deadline:
        resp = rpc_call("eth_blockNumber")
        if "error" in resp:
            raise RuntimeError(f"eth_blockNumber: {resp['error']}")
        latest = int(resp["result"], 16)
        if latest + 1 >= target:
            return
        time.sleep(POLL_SECS)


def handle_bundle(request_id: Any, params: list[Any]) -> dict[str, Any]:
    bundle = params[0] if params else {}
    txs = bundle.get("txs", [])
    target_block = bundle.get("blockNumber")

    if not isinstance(txs, list) or not all(isinstance(tx, str) for tx in txs):
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32602, "message": "eth_sendBundle requires params[0].txs"},
        }

    try:
        wait_for_target_parent(target_block)
        tx_hashes: list[str] = []
        for raw_tx in txs:
            resp = rpc_call("eth_sendRawTransaction", [raw_tx])
            if "error" in resp:
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": resp["error"],
                }
            tx_hashes.append(resp["result"])
    except Exception as exc:  # noqa: BLE001 - surfaced as JSON-RPC error.
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32000, "message": str(exc)},
        }

    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "result": {
            "bundleHash": "0x" + "00" * 32,
            "txHashes": tx_hashes,
        },
    }


def handle_payload(payload: Any) -> Optional[dict[str, Any]]:
    if not isinstance(payload, dict):
        return None
    if payload.get("method") != "eth_sendBundle":
        return None
    return handle_bundle(payload.get("id"), payload.get("params") or [])


class Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API.
        length = int(self.headers.get("content-length", "0"))
        raw_body = self.rfile.read(length)

        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except json.JSONDecodeError:
            self.send_json(400, {"error": "invalid JSON"})
            return

        if isinstance(payload, list):
            responses = [handle_payload(item) for item in payload]
            if all(resp is not None for resp in responses):
                self.send_json(200, responses)
                return
        else:
            response = handle_payload(payload)
            if response is not None:
                self.send_json(200, response)
                return

        status, body = forward_raw(raw_body)
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"builder-stub: {self.address_string()} - {fmt % args}", file=sys.stderr)

    def send_json(self, status: int, value: Any) -> None:
        body = json.dumps(value).encode("utf-8")
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    print(f"builder-stub: listening on {HOST}:{PORT}, forwarding to {L1_RPC_URL}", flush=True)
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
