#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.analysis.provider_audit import audit_all_providers, audit_provider, render_provider_audit_markdown


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider")
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    parser.add_argument("--null-threshold", type=float, default=0.5)
    args = parser.parse_args()

    if args.provider:
        reports = [audit_provider(args.provider, null_threshold=args.null_threshold)]
    else:
        reports = audit_all_providers(null_threshold=args.null_threshold)

    if args.format == "json":
        print(json.dumps(reports, indent=2))
        return

    print(render_provider_audit_markdown(reports), end="")


if __name__ == "__main__":
    main()
