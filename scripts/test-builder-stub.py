#!/usr/bin/env python3
"""Check builder dispatch boundaries without starting a chain or HTTP server."""

import importlib.util
import io
import json
from pathlib import Path
import unittest
from unittest.mock import Mock, patch


spec = importlib.util.spec_from_file_location(
    "builder_stub", Path(__file__).with_name("builder-stub.py")
)
stub = importlib.util.module_from_spec(spec)
spec.loader.exec_module(stub)


class BuilderDispatchTests(unittest.TestCase):
    def handler(self, body):
        """Build a request with in-memory HTTP streams and recorded headers."""
        raw = json.dumps(body).encode()
        handler = object.__new__(stub.Handler)
        handler.rfile = io.BytesIO(raw)
        handler.wfile = io.BytesIO()
        handler.headers = {"Content-Length": str(len(raw))}
        handler.send_response = Mock()
        handler.send_header = Mock()
        handler.end_headers = Mock()
        handler.send_error = Mock()
        return handler

    def test_ordinary_batch_preserves_requests_and_response_ids(self):
        """Forward ordinary RPC batches through the existing upstream gateway."""
        body = [
            {"jsonrpc": "2.0", "id": 17, "method": "eth_chainId", "params": []},
            {"jsonrpc": "2.0", "id": 28, "method": "eth_blockNumber", "params": []},
        ]
        responses = [
            {"jsonrpc": "2.0", "id": 28, "result": "0x123"},
            {"jsonrpc": "2.0", "id": 17, "result": "0x1"},
        ]
        handler = self.handler(body)
        with patch.object(stub.Builder, "upstream", "http://upstream.invalid"), patch.object(
            stub.urllib.request,
            "urlopen",
            return_value=io.BytesIO(json.dumps(responses).encode()),
        ) as upstream:
            handler.do_POST()
        self.assertEqual(json.loads(upstream.call_args.args[0].data), body)
        self.assertEqual(json.loads(handler.wfile.getvalue()), responses)
        handler.send_response.assert_called_once_with(200)

    def test_builder_methods_cannot_bypass_local_state_in_a_batch(self):
        """Reject the whole mixed batch before executing or forwarding any item."""
        methods = [
            "eth_sendBundle",
            "eez_setBuilderMode",
            "eez_setMining",
            "eez_mine",
            "eez_getRejectedTransactions",
            "eez_futureControl",
        ]
        for method in methods:
            for mode in ("forward", "drop"):
                with self.subTest(method=method, mode=mode):
                    body = [
                        {"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber"},
                        {"jsonrpc": "2.0", "id": 2, "method": method},
                    ]
                    handler = self.handler(body)
                    with patch.object(stub.Builder, "passthrough", True), patch.object(
                        stub.Builder, "mode", mode
                    ), patch.object(
                        stub.Builder, "upstream", "http://upstream.invalid"
                    ), patch.object(stub.Builder, "queue") as queue, patch.object(
                        stub.urllib.request, "urlopen", return_value=io.BytesIO(b"[]")
                    ) as upstream:
                        handler.do_POST()
                        self.assertEqual(stub.Builder.mode, mode)
                    queue.assert_not_called()
                    upstream.assert_not_called()
                    handler.send_error.assert_called_once_with(
                        400, "builder methods require a single request"
                    )

    def test_single_builder_control_keeps_local_dispatch(self):
        """Keep the existing singleton control path available."""
        body = {
            "jsonrpc": "2.0",
            "id": 31,
            "method": "eez_setBuilderMode",
            "params": ["drop"],
        }
        handler = self.handler(body)
        with patch.object(stub.Builder, "mode", "forward"), patch.object(
            stub.urllib.request, "urlopen"
        ) as upstream:
            handler.do_POST()
            self.assertEqual(stub.Builder.mode, "drop")
        upstream.assert_not_called()
        self.assertEqual(
            json.loads(handler.wfile.getvalue()),
            {"jsonrpc": "2.0", "id": 31, "result": "drop"},
        )


if __name__ == "__main__":
    unittest.main()
