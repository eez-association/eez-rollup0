#!/usr/bin/env python3
"""Minimal eth_sendBundle builder for tests / devnet.

Real Flashbots-style builders accept a bundle, answer at once, and try to
include it in its target L1 block. They don't consume the poster's nonce on
miss.

This stub owns block production on a backing Anvil started with
`--no-mining`. A ticker mines one block per `--block-time` slot, stamped on
that slot's boundary, so L1 keeps the cadence the composer pins bundles to.
`eth_sendBundle` queues the bundle for its target block; the ticker applies
every bundle queued for that block in arrival order, each atomically,
honouring `minTimestamp`/`maxTimestamp` and `revertingTxHashes`. A bundle
whose pin misses the slot, whose target has passed, or with a non-whitelisted
revert is left out and the block is built without it. Anvil must also run
with `--order fifo` so a block keeps each bundle's transactions in order.
Suitable for tests; not for production.

Extra methods for tests: `eez_setBuilderMode` ("forward" | "drop"),
`eez_setMining` (pause or resume the ticker) and `eez_mine` (mine one block).

Usage:
    builder-stub.py --listen 127.0.0.1:9001 --upstream http://127.0.0.1:8545
"""

import argparse
import hashlib
import http.server
import json
import socketserver
import sys
import threading
import time
import urllib.request


def _rpc(method, params):
    payload = {"jsonrpc": "2.0", "id": 0, "method": method, "params": params}
    try:
        raw = urllib.request.urlopen(
            urllib.request.Request(
                Builder.upstream,
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


def _quantity(value, default=None):
    if value is None:
        return default
    return int(value, 16) if isinstance(value, str) else int(value)


def _log(message):
    print(message, file=sys.stderr, flush=True)


class _BundleFailure(Exception):
    def __init__(self, index, reason):
        super().__init__(str(reason))
        self.index = index
        self.reason = reason


class Builder:
    upstream = ""
    block_time = 2
    mode = "forward"
    mining = True
    # Serialises every block the stub mines; Anvil mines nothing else.
    lock = threading.Lock()
    # Target block number -> queued bundle params, in arrival order.
    pending = {}

    @classmethod
    def latest(cls):
        block = _rpc("eth_getBlockByNumber", ["latest", False])
        return int(block["number"], 16), int(block["timestamp"], 16)

    @classmethod
    def slot_timestamp(cls, latest_ts, now):
        # The next slot boundary, skipping slots that already elapsed.
        return latest_ts + cls.block_time * max(
            1, int(now - latest_ts) // cls.block_time
        )

    @classmethod
    def queue(cls, params):
        txs = params.get("txs", [])
        if not txs:
            raise ValueError("bundle must contain at least one transaction")
        target = _quantity(params.get("blockNumber"), 0)
        with cls.lock:
            number, _ = cls.latest()
            if target <= number:
                # A real builder accepts it and never includes it.
                _log(
                    f"bundle for block {target} arrived after block {number}; not included"
                )
            elif cls.mode == "forward":
                cls.pending.setdefault(target, []).append(params)
        digest = hashlib.sha256(json.dumps(txs).encode()).hexdigest()
        return {"bundleHash": "0x" + digest}

    @classmethod
    def mine_slot(cls, timestamp):
        """Mine the next block at `timestamp` with every valid queued bundle."""
        number, _ = cls.latest()
        target = number + 1
        bundles = [
            bundle
            for bundle in cls.pending.pop(target, [])
            if cls.pin_matches(bundle, timestamp)
        ]
        for stale in [block for block in cls.pending if block < target]:
            del cls.pending[stale]
        try:
            # Drop the bundle that broke the block and retry, so one bad bundle
            # never takes the others down with it.
            while bundles:
                failed = cls.try_bundles(bundles, timestamp)
                if failed is None:
                    return
                bundles.pop(failed)
        except RuntimeError:
            # The candidate was rolled back after an infrastructure failure.
            # Keep its remaining bundles queued for the same next block.
            cls.pending[target] = bundles + cls.pending.get(target, [])
            raise
        _rpc("evm_setNextBlockTimestamp", [timestamp])
        _rpc("evm_mine", [])

    @staticmethod
    def pin_matches(params, timestamp):
        minimum = _quantity(params.get("minTimestamp"))
        maximum = _quantity(params.get("maxTimestamp"))
        if (minimum is not None and timestamp < minimum) or (
            maximum is not None and timestamp > maximum
        ):
            _log(
                f"bundle pinned to [{minimum}, {maximum}] misses slot {timestamp}; not included"
            )
            return False
        return True

    @staticmethod
    def pooled_raw_txs():
        """Raw txs waiting in Anvil's pool, in sender/nonce order."""
        content = _rpc("txpool_content", [])
        txs = [
            tx
            for section in ("pending", "queued")
            for by_nonce in content.get(section, {}).values()
            for tx in by_nonce.values()
        ]
        txs.sort(key=lambda tx: (tx["from"].lower(), _quantity(tx["nonce"])))
        return [_rpc("eth_getRawTransactionByHash", [tx["hash"]]) for tx in txs]

    @classmethod
    def try_bundles(cls, bundles, timestamp):
        """Mine `bundles` into one block; on failure roll back and name the culprit."""
        # evm_revert also empties the pool, so keep what other senders queued.
        pooled = cls.pooled_raw_txs()
        snapshot = _rpc("evm_snapshot", [])
        sent = []
        try:
            _rpc("evm_setNextBlockTimestamp", [timestamp])
            for index, bundle in enumerate(bundles):
                allowed = {
                    value.lower() for value in bundle.get("revertingTxHashes", [])
                }
                for raw in bundle["txs"]:
                    try:
                        sent.append(
                            (index, _rpc("eth_sendRawTransaction", [raw]), allowed)
                        )
                    except RuntimeError as error:
                        raise _BundleFailure(index, error) from error
            _rpc("evm_mine", [])
            for index, tx_hash, allowed in sent:
                receipt = _rpc("eth_getTransactionReceipt", [tx_hash])
                if receipt is None:
                    raise _BundleFailure(
                        index, f"transaction {tx_hash} was not included"
                    )
                if int(receipt["status"], 16) == 0 and tx_hash.lower() not in allowed:
                    raise _BundleFailure(
                        index,
                        f"transaction {tx_hash} reverted without being whitelisted",
                    )
            return None
        except _BundleFailure as failure:
            _log(f"bundle rolled back: {failure.reason}")
            cls.restore(snapshot, pooled)
            return failure.index
        except RuntimeError:
            cls.restore(snapshot, pooled)
            raise

    @staticmethod
    def restore(snapshot, pooled):
        _rpc("evm_revert", [snapshot])
        for raw in pooled:
            try:
                _rpc("eth_sendRawTransaction", [raw])
            except RuntimeError:
                pass  # Already back in the pool.

    @classmethod
    def tick_forever(cls):
        while True:
            wait = 0.1
            try:
                with cls.lock:
                    if cls.mining:
                        _, latest_ts = cls.latest()
                        due = latest_ts + cls.block_time
                        now = time.time()
                        if now >= due:
                            cls.mine_slot(cls.slot_timestamp(latest_ts, now))
                        else:
                            wait = min(due - now, 0.25)
            except Exception as error:  # Keep producing blocks across transient errors.
                _log(f"ticker: {error}")
                wait = 0.5
            time.sleep(wait)


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def _result(self, body, result):
        return {"jsonrpc": "2.0", "id": body.get("id"), "result": result}

    def _error(self, body, message):
        return {
            "jsonrpc": "2.0",
            "id": body.get("id"),
            "error": {"code": -32602, "message": message},
        }

    def _dispatch(self, body):
        method = body.get("method")
        params = body.get("params", [])
        if method == "eez_setBuilderMode":
            mode = params[0] if params else None
            if mode not in ("forward", "drop"):
                return self._error(body, "invalid builder stub mode")
            with Builder.lock:
                Builder.mode = mode
            return self._result(body, mode)
        if method == "eez_setMining":
            mining = bool(params[0]) if params else True
            with Builder.lock:
                Builder.mining = mining
            return self._result(body, mining)
        if method == "eez_mine":
            with Builder.lock:
                _, latest_ts = Builder.latest()
                # Like evm_mine: now, or just after the tip if that is later.
                Builder.mine_slot(max(latest_ts + 1, int(time.time())))
            return self._result(body, True)
        if method == "eth_sendBundle":
            try:
                return self._result(body, Builder.queue(params[0]))
            except (IndexError, KeyError, TypeError, ValueError, RuntimeError) as error:
                return self._error(body, str(error))
        return None

    def do_POST(self):
        try:
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        except Exception:
            self.send_error(400)
            return

        response = self._dispatch(body)
        if response is not None:
            resp = json.dumps(response).encode()
        else:
            # The stub is the harness's only Anvil gateway. Serialising
            # forwarded calls keeps snapshot rollback from losing a request
            # that arrived while a candidate block was being built.
            with Builder.lock:
                resp = urllib.request.urlopen(
                    urllib.request.Request(
                        Builder.upstream,
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
    p.add_argument(
        "--upstream", required=True, help="anvil RPC url (started with --no-mining)"
    )
    p.add_argument("--block-time", type=int, default=2)
    args = p.parse_args()
    host, port = args.listen.split(":")
    Builder.upstream = args.upstream
    Builder.block_time = args.block_time

    class Reusable(socketserver.ThreadingTCPServer):
        allow_reuse_address = True
        daemon_threads = True

    threading.Thread(target=Builder.tick_forever, daemon=True).start()
    with Reusable((host, int(port)), Handler) as srv:
        srv.serve_forever()


if __name__ == "__main__":
    main()
