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
    dispatched = set()
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
                dispatched.add(number(fields["sync_height"]))
            elif event == "eez.deriver.safe.advanced":
                settled.append(number(fields["to_block"]))
    finally:
        if args.log:
            stream.close()

    if not settled:
        raise SystemExit("no included settlements found")
    unexpected = [height for height in settled if height not in dispatched]
    if unexpected:
        raise SystemExit(
            "settlements outside their dispatched Sync heights: "
            + ", ".join(map(str, unexpected))
        )
    print(f"verified {len(settled)} settlement(s) at dispatched Sync heights")


if __name__ == "__main__":
    main()
