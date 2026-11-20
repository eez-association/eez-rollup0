#!/usr/bin/env python3
"""Check integrated prover sources, then run the prover's execution checks."""
import json
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[1]
metadata = sys.stdin.read()
for package in json.loads(metadata)['packages']:
    expected = root / 'crates' / package['name'] / 'Cargo.toml'
    source = package['source'] or ''
    if expected.is_file() or source.startswith('git+https://github.com/eez-association/eez-rollup0?'):
        if source or Path(package['manifest_path']).resolve() != expected:
            sys.exit(f"{package['name']} must come from {expected}, got "
                     f"{source or package['manifest_path']}")

subprocess.run(
    [sys.executable, str(root / 'provers/scripts/check-execution-deps.py'), *sys.argv[1:]],
    input=metadata,
    text=True,
    check=True,
)
