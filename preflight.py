#!/usr/bin/env python3
"""Corporate-machine pre-flight command.

Examples:
  python preflight.py --environment test
  python preflight.py --environment production --online --sample-blobs 5
"""

from __future__ import annotations

import argparse
import json
from dataclasses import replace
from pathlib import Path

from engine.config import load_config
from engine.manifest import ManifestStore
from engine.preflight import PreflightAuditor


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=str(Path(__file__).with_name("config.json")))
    parser.add_argument("--environment", required=True, help="Profile ID from config.json")
    parser.add_argument("--online", action="store_true")
    parser.add_argument("--sample-blobs", type=int, default=0)
    parser.add_argument("--state-db")
    args = parser.parse_args()
    cfg = replace(load_config(args.config), default_environment=args.environment)
    db_path = args.state_db or str(Path(__file__).with_name(f"migration_state_v2_{args.environment}.db"))
    report = PreflightAuditor(
        cfg.environment(), ManifestStore(db_path), cfg.migration_settings
    ).run(online=args.online, sample_blobs=max(0, args.sample_blobs))
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0 if report["status"] != "FAIL" else 2


if __name__ == "__main__":
    raise SystemExit(main())
