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
import urllib.request


class Handler(http.server.BaseHTTPRequestHandler):
    upstream = ""

    def log_message(self, *_):
        pass

    def _wait_for_block(self, target):
        if target <= 0:
            return
        import time
        for _ in range(60):
            try:
                resp = urllib.request.urlopen(
                    urllib.request.Request(
                        self.upstream,
                        data=json.dumps({
                            "jsonrpc": "2.0",
                            "id": 0,
                            "method": "eth_blockNumber",
                            "params": [],
                        }).encode(),
                        headers={"Content-Type": "application/json"},
                    ),
                    timeout=2,
                ).read()
                bn = int(json.loads(resp).get("result", "0x0"), 16)
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

        if body.get("method") == "eth_sendBundle":
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
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(resp)))
                self.end_headers()
                self.wfile.write(resp)
                return
            raw = txs[0]
            target_hex = params.get("blockNumber", "0x0")
            target = int(target_hex, 16) if isinstance(target_hex, str) else int(target_hex)

            # Wait until anvil's tip is one short of `target` so the
            # forwarded tx lands *in* `target` (anvil mines a block as
            # soon as a tx arrives at >= block_time cadence).
            self._wait_for_block(target - 1)

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
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(resp)))
        self.end_headers()
        self.wfile.write(resp)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--listen", required=True, help="host:port")
    p.add_argument("--upstream", required=True, help="anvil RPC url")
    args = p.parse_args()
    host, port = args.listen.split(":")
    Handler.upstream = args.upstream

    class Reusable(socketserver.ThreadingTCPServer):
        allow_reuse_address = True

    with Reusable((host, int(port)), Handler) as srv:
        srv.serve_forever()


if __name__ == "__main__":
    main()
