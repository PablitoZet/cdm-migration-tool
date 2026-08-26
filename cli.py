#!/usr/bin/env python3
"""Headless operator interface for the corporate migration VM."""

from __future__ import annotations

import argparse
import json
from dataclasses import replace
from pathlib import Path

from engine.config import load_config
from engine.pipeline import MigrationPipeline
from engine.preflight import PreflightAuditor
from engine.reconciler import AutomatedVerifier

ROOT = Path(__file__).resolve().parent


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=str(ROOT / "config.json"))
    parser.add_argument("--environment", required=True, help="Arbitrary profile ID from config.json")
    parser.add_argument("--state-db")
    sub = parser.add_subparsers(dest="command", required=True)
    extract = sub.add_parser("extract")
    extract.add_argument("--root", type=int)
    extract.add_argument("--force", action="store_true")
    preflight = sub.add_parser("preflight")
    preflight.add_argument("--online", action="store_true")
    preflight.add_argument("--sample-blobs", type=int, default=0)
    run_command = sub.add_parser("run")
    run_command.add_argument("--mode", choices=("dry_run", "pilot", "full"), required=True)
    run_command.add_argument("--threads", type=int, default=8)
    run_command.add_argument("--max-documents", type=int)
    recover = sub.add_parser("recover")
    recover.add_argument("run_id")
    recover.add_argument("--threads", type=int, default=8)
    verify = sub.add_parser("verify")
    verify.add_argument("run_id", nargs="?")
    verify.add_argument("--live", action="store_true")
    verify.add_argument("--redownload", action="store_true")
    backup = sub.add_parser("backup")
    backup.add_argument("target")
    inspect_source = sub.add_parser("inspect-source")
    inspect_source.add_argument("--root", type=int)
    inspect_target = sub.add_parser("inspect-target")
    inspect_target.add_argument("--node-id", type=int)
    sub.add_parser("compatibility")
    freeze = sub.add_parser("confirm-freeze")
    freeze.add_argument("--operator", required=True)
    freeze.add_argument("--note", default="")
    args = parser.parse_args()

    cfg = replace(load_config(args.config), default_environment=args.environment)
    state = args.state_db or str(ROOT / f"migration_state_v2_{args.environment}.db")
    pipeline = MigrationPipeline(cfg, state)

    if args.command == "extract":
        if pipeline.manifest.inventory_summary()["total_nodes"] and not args.force:
            raise SystemExit("Manifest exists; rerun with --force after taking a backup")
        _print(pipeline.run_extraction(args.root))
        return 0
    if args.command == "preflight":
        report = PreflightAuditor(
            cfg.environment(), pipeline.manifest, cfg.migration_settings
        ).run(online=args.online, sample_blobs=max(0, args.sample_blobs))
        _print(report)
        return 2 if report["status"] == "FAIL" else 0
    if args.command == "run":
        _confirm_production(cfg.environment())
        max_docs = args.max_documents or (100 if args.mode == "pilot" else None)
        run_id = pipeline.start_migration(
            max_items=max_docs, dry_run=args.mode == "dry_run",
            threads=args.threads, mode=args.mode,
        )
        return _watch(pipeline, run_id)
    if args.command == "recover":
        _confirm_production(cfg.environment())
        pipeline.recover_run(args.run_id, args.threads)
        return _watch(pipeline, args.run_id)
    if args.command == "verify":
        selected_run = pipeline.manifest.latest_run() if not args.run_id else pipeline.manifest.run_status(args.run_id)
        if not selected_run:
            raise SystemExit("No run found")
        client = pipeline.target if args.live else None
        report = AutomatedVerifier(pipeline.manifest, client).run_all_tests(
            selected_run["run_id"], live=args.live, redownload=args.redownload
        )
        _print(report)
        return 0 if report["overall_status"] == "PASS" else 3
    if args.command == "backup":
        pipeline.manifest.backup(args.target)
        print(args.target)
        return 0
    if args.command == "inspect-source":
        _print(pipeline.inspect_source_scope(args.root))
        return 0
    if args.command == "inspect-target":
        _print(pipeline.inspect_target_root(args.node_id))
        return 0
    if args.command == "compatibility":
        report = pipeline.manifest.parity_report(cfg.environment())
        _print(report)
        return 0 if report["status"] == "PASS" else 5
    if args.command == "confirm-freeze":
        _confirm_production(cfg.environment())
        _print(pipeline.confirm_source_freeze(args.operator, args.note))
        return 0
    return 1


def _watch(pipeline: MigrationPipeline, run_id: str) -> int:
    while not pipeline.wait(5):
        telemetry = pipeline.get_telemetry()
        print(json.dumps({
            "run_id": run_id, "status": telemetry["status"],
            "progress_percent": telemetry["progress_percent"],
            "speed_mb_per_sec": telemetry["speed_mb_per_sec"],
            "failed_nodes": telemetry["failed_nodes"],
        }, ensure_ascii=False), flush=True)
    final = pipeline.manifest.run_status(run_id)
    _print(final)
    return 0 if final["status"] == "COMPLETED" else 4


def _print(value) -> None:
    print(json.dumps(value, indent=2, ensure_ascii=False, default=str))


def _confirm_production(environment) -> None:
    if not environment.is_production:
        return
    if input("Confirm production target (type YES): ").strip().upper() != "YES":
        raise SystemExit("Production confirmation rejected")


if __name__ == "__main__":
    raise SystemExit(main())
