#!/usr/bin/env python3
"""Verify that every observed settlement uses a dispatched Sync height."""

import argparse
import json
import sys


def number(value):
    return int(value, 0) if isinstance(value, str) else int(value)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("log", nargs="?", help="JSON node log; defaults to stdin")
    args = parser.parse_args()
    stream = open(args.log, encoding="utf-8") if args.log else sys.stdin
    dispatched = {}
    settled = []
    try:
        for line in stream:
            try:
                fields = json.loads(line).get("fields", {})
            except json.JSONDecodeError:
                continue
            event = fields.get("event_name")
            if event in (
                "eez.composer.bundle.dispatched",
                "eez.composer.phase1.bundle.dispatched",
            ):
                dispatched[fields["post_batch_hash"].lower()] = number(
                    fields["sync_height"]
                )
            elif event == "eez.composer.emission.historical_chunk":
                dispatched[fields["post_batch_hash"].lower()] = number(
                    fields["boundary"]
                )
            elif event == "eez.deriver.safe.advanced":
                settled.append((fields["tx_hash"].lower(), number(fields["to_block"])))
    finally:
        if args.log:
            stream.close()

    if not settled:
        raise SystemExit("no included settlements found")
    missing = [tx_hash for tx_hash, _ in settled if tx_hash not in dispatched]
    if missing:
        raise SystemExit(
            "included settlements without a correlated dispatch: " + ", ".join(missing)
        )
    unexpected = [
        (tx_hash, height, dispatched[tx_hash])
        for tx_hash, height in settled
        if height != dispatched[tx_hash]
    ]
    if unexpected:
        raise SystemExit(
            "settlements outside their dispatched Sync heights: "
            + ", ".join(
                f"{tx_hash} settled={height} dispatched={dispatched_height}"
                for tx_hash, height, dispatched_height in unexpected
            )
        )
    print(f"verified {len(settled)} settlement(s) at dispatched Sync heights")


if __name__ == "__main__":
    main()
