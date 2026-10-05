#!/usr/bin/env python3
"""Minimal eth_sendBundle stub for tests / devnet.

Real Flashbots-style relays accept bundles and try to include them in a
specific L1 block. They don't consume the poster's nonce on miss.

This stub applies bundles to a backing Anvil in one explicitly mined block.
It snapshots the chain before submission and restores the snapshot when a
transaction outside `revertingTxHashes` fails, preserving bundle atomicity.
Suitable for tests; not for production.

Usage:
    builder-stub.py --listen 127.0.0.1:9001 --upstream http://127.0.0.1:8545
"""
import argparse
import http.server
import json
import socketserver
import threading
import urllib.request


class Handler(http.server.BaseHTTPRequestHandler):
    upstream = ""
    mode = "forward"
    block_time = 2
    bundle_lock = threading.Lock()

    def log_message(self, *_):
        pass

    def _rpc(self, method, params):
        payload = {
            "jsonrpc": "2.0",
            "id": 0,
            "method": method,
            "params": params,
        }
        try:
            raw = urllib.request.urlopen(
                urllib.request.Request(
                    self.upstream,
                    data=json.dumps(payload).encode(),
                    headers={"Content-Type": "application/json"},
                ),
                timeout=10,
            ).read()
            response = json.loads(raw)
        except Exception as error:
            raise RuntimeError(f"{method}: {error}") from error
        if "error" in response:
            raise RuntimeError(f"{method}: {response['error']}")
        return response.get("result")

    @staticmethod
    def _quantity(value, default=None):
        if value is None:
            return default
        return int(value, 16) if isinstance(value, str) else int(value)

    def _wait_for_block(self, target):
        if target <= 0:
            return 0
        import time
        for _ in range(60):
            try:
                bn = int(self._rpc("eth_blockNumber", []), 16)
                if bn >= target:
                    return bn
            except Exception:
                pass
            time.sleep(0.1)
        raise RuntimeError(f"timed out waiting for block {target}")

    def _apply_bundle(self, params):
        txs = params.get("txs", [])
        if not txs:
            raise ValueError("bundle must contain at least one transaction")
        target = self._quantity(params.get("blockNumber"), 0)
        allowed_reverts = {
            value.lower() for value in params.get("revertingTxHashes", [])
        }

        self._wait_for_block(target - 1)
        snapshot = None
        committed = False
        self._rpc("anvil_setIntervalMining", [0])
        try:
            tip = int(self._rpc("eth_blockNumber", []), 16)
            if target and tip != target - 1:
                raise RuntimeError(
                    f"target block {target} is no longer next after tip {tip}"
                )
            snapshot = self._rpc("evm_snapshot", [])

            latest = self._rpc("eth_getBlockByNumber", ["latest", False])
            timestamp = int(latest["timestamp"], 16) + self.block_time
            minimum = self._quantity(params.get("minTimestamp"), timestamp)
            maximum = self._quantity(params.get("maxTimestamp"))
            timestamp = max(timestamp, minimum)
            if maximum is not None and timestamp > maximum:
                raise RuntimeError("bundle timestamp window has already elapsed")
            self._rpc("evm_setNextBlockTimestamp", [timestamp])

            hashes = [self._rpc("eth_sendRawTransaction", [raw]) for raw in txs]
            self._rpc("evm_mine", [])
            for tx_hash in hashes:
                receipt = self._rpc("eth_getTransactionReceipt", [tx_hash])
                if receipt is None:
                    raise RuntimeError(f"transaction {tx_hash} was not included")
                if int(receipt["status"], 16) == 0 and tx_hash.lower() not in allowed_reverts:
                    raise RuntimeError(
                        f"transaction {tx_hash} reverted without being whitelisted"
                    )
            committed = True
            return {"bundleHash": hashes[-1]}
        finally:
            try:
                if snapshot is not None and not committed:
                    self._rpc("evm_revert", [snapshot])
            finally:
                self._rpc("anvil_setIntervalMining", [self.block_time])

    def do_POST(self):
        try:
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        except Exception:
            self.send_error(400)
            return

        if body.get("method") == "eez_setBuilderMode":
            mode = body.get("params", [None])[0]
            if mode not in ("forward", "drop"):
                resp = json.dumps({
                    "jsonrpc": "2.0",
                    "id": body.get("id"),
                    "error": {"code": -32602, "message": "invalid builder stub mode"},
                }).encode()
            else:
                Handler.mode = mode
                resp = json.dumps({
                    "jsonrpc": "2.0", "id": body.get("id"), "result": mode,
                }).encode()
        elif body.get("method") == "eth_sendBundle" and Handler.mode == "drop":
            # Model a relay that accepts a bundle but never includes it.
            resp = json.dumps({
                "jsonrpc": "2.0",
                "id": body.get("id"),
                "result": {"bundleHash": "0x" + "00" * 32},
            }).encode()
        elif body.get("method") == "eth_sendBundle":
            try:
                params = body["params"][0]
                with Handler.bundle_lock:
                    result = self._apply_bundle(params)
                resp = json.dumps({
                    "jsonrpc": "2.0",
                    "id": body.get("id"),
                    "result": result,
                }).encode()
            except (KeyError, TypeError, ValueError, RuntimeError) as error:
                resp = json.dumps({
                    "jsonrpc": "2.0",
                    "id": body.get("id"),
                    "error": {
                        "code": -32602,
                        "message": str(error),
                    },
                }).encode()
        else:
            resp = urllib.request.urlopen(
                urllib.request.Request(
                    self.upstream,
                    data=json.dumps(body).encode(),
                    headers={"Content-Type": "application/json"},
                ),
                timeout=10,
            ).read()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(resp)))
        self.end_headers()
        self.wfile.write(resp)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--listen", required=True, help="host:port")
    p.add_argument("--upstream", required=True, help="anvil RPC url")
    p.add_argument("--block-time", type=int, default=2)
    args = p.parse_args()
    host, port = args.listen.split(":")
    Handler.upstream = args.upstream
    Handler.block_time = args.block_time

    class Reusable(socketserver.ThreadingTCPServer):
        allow_reuse_address = True

    with Reusable((host, int(port)), Handler) as srv:
        srv.serve_forever()


if __name__ == "__main__":
    main()
