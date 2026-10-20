#!/usr/bin/env python3
"""Minimal eth_sendBundle stub for tests / devnet.

Real Flashbots-style relays accept bundles and try to include them in a
specific L1 block. They don't consume the poster's nonce on miss.

This stub fakes a relay: it accepts single-transaction `eth_sendBundle`
requests and forwards the signed tx via `eth_sendRawTransaction` to a
backing anvil. Bundles containing zero or multiple transactions are
rejected explicitly because forwarding them individually would not preserve
bundle atomicity. The nonce IS consumed on miss (anvil doesn't replay-
protect), but the Composer's cursor-race guard + pending-nonce read keeps it
correct. Suitable for tests; not for production.

Usage:
    builder-stub.py --listen 127.0.0.1:9001 --upstream http://127.0.0.1:8545
"""
import argparse
import http.server
import json
import socketserver
import time
import urllib.request


class Handler(http.server.BaseHTTPRequestHandler):
    upstream = ""
    beacon = ""
    mode = "forward"

    def _rpc(self, method, params, timeout=10):
        body = json.dumps({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        }).encode()
        response = urllib.request.urlopen(
            urllib.request.Request(
                self.upstream,
                data=body,
                headers={"Content-Type": "application/json"},
            ),
            timeout=timeout,
        ).read()
        decoded = json.loads(response)
        if "error" in decoded:
            raise RuntimeError(decoded["error"])
        return decoded.get("result")

    def _tell_beacon(self, raw):
        tx_hash = self._rpc("eth_sendRawTransaction", [raw])
        receipt = None
        for _ in range(80):
            receipt = self._rpc("eth_getTransactionReceipt", [tx_hash])
            if receipt:
                break
            time.sleep(0.1)
        if not receipt:
            raise RuntimeError(f"type-3 transaction {tx_hash} was not mined")
        block = self._rpc("eth_getBlockByHash", [receipt["blockHash"], False])
        notice = json.dumps({
            "raw": raw,
            "blockHash": receipt["blockHash"],
            "timestamp": int(block["timestamp"], 16),
        }).encode()
        urllib.request.urlopen(
            urllib.request.Request(
                self.beacon.rstrip("/") + "/ingest",
                data=notice,
                headers={"Content-Type": "application/json"},
            ),
            timeout=10,
        ).read()
        return tx_hash

    def log_message(self, *_):
        pass

    def _reply(self, response):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    def _wait_for_block(self, target):
        if target <= 0:
            return
        for _ in range(60):
            try:
                bn = int(self._rpc("eth_blockNumber", [], timeout=2), 16)
                if bn >= target:
                    return
            except Exception:
                pass
            time.sleep(0.1)

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
            params = body["params"][0]
            txs = params.get("txs", [])
            if len(txs) != 1:
                resp = json.dumps({
                    "jsonrpc": "2.0",
                    "id": body.get("id"),
                    "error": {
                        "code": -32602,
                        "message": "builder stub accepts exactly one transaction per bundle",
                        "data": {"transactionCount": len(txs)},
                    },
                }).encode()
                self._reply(resp)
                return
            raw = txs[0]
            target_hex = params.get("blockNumber", "0x0")
            target = int(target_hex, 16) if isinstance(target_hex, str) else int(target_hex)

            # Wait until anvil's tip is one short of `target` so the
            # forwarded tx lands *in* `target` (anvil mines a block as
            # soon as a tx arrives at >= block_time cadence).
            self._wait_for_block(target - 1)

            if self.beacon and raw.startswith(("0x03", "0X03")):
                try:
                    tx_hash = self._tell_beacon(raw)
                except Exception as error:
                    message = json.dumps({
                        "jsonrpc": "2.0",
                        "id": body.get("id"),
                        "error": {
                            "code": -32000,
                            "message": f"beacon stub ingest failed: {error}",
                        },
                    }).encode()
                    self._reply(message)
                    return
                resp = json.dumps({
                    "jsonrpc": "2.0",
                    "id": body.get("id"),
                    "result": {"bundleHash": tx_hash},
                }).encode()
            else:
                forwarded = {
                    "jsonrpc": "2.0",
                    "id": body.get("id"),
                    "method": "eth_sendRawTransaction",
                    "params": [raw],
                }
                resp = urllib.request.urlopen(
                    urllib.request.Request(
                        self.upstream,
                        data=json.dumps(forwarded).encode(),
                        headers={"Content-Type": "application/json"},
                    ),
                    timeout=10,
                ).read()
        else:
            resp = urllib.request.urlopen(
                urllib.request.Request(
                    self.upstream,
                    data=json.dumps(body).encode(),
                    headers={"Content-Type": "application/json"},
                ),
                timeout=10,
            ).read()
        self._reply(resp)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--listen", required=True, help="host:port")
    p.add_argument("--upstream", required=True, help="anvil RPC url")
    p.add_argument("--beacon", default="", help="beacon stub base URL")
    args = p.parse_args()
    host, port = args.listen.split(":")
    Handler.upstream = args.upstream
    Handler.beacon = args.beacon

    class Reusable(socketserver.ThreadingTCPServer):
        allow_reuse_address = True

    with Reusable((host, int(port)), Handler) as srv:
        srv.serve_forever()


if __name__ == "__main__":
    main()
