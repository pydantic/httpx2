from __future__ import annotations

import argparse
import json
import random
import subprocess
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import run as harness


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare experimental clients in a reproducibly shuffled order.")
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--lib", action="append", required=True)
    parser.add_argument("--scenario", type=harness.ScenarioSpec.parse, action="append", required=True)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--seconds", type=float, default=3.0)
    parser.add_argument("--seed", type=int, default=20260919)
    parser.add_argument("--mode", choices=("read", "stream"), default="read")
    parser.add_argument("--no-zuvloop", action="store_true")
    parser.add_argument("--max-connections", type=int, default=None)
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.client_cpu = None
    harness.CLIENT = Path(__file__).with_name("worker.py")
    libs = list(dict.fromkeys(args.lib))
    specs = list(dict.fromkeys(args.scenario))
    rng = random.Random(args.seed)
    command = [sys.executable, str(harness.SERVER), "--port", str(args.port)]
    if args.no_zuvloop:
        command.append("--no-zuvloop")
    server = subprocess.Popen(command, stdout=subprocess.PIPE)
    results: dict[tuple[str, str], list[dict[str, Any]]] = {}
    try:
        harness.wait_for_server(server, args.port)
        for repetition in range(1, args.rounds + 1):
            jobs = [(spec, lib) for spec in specs for lib in libs]
            rng.shuffle(jobs)
            for order, (spec, lib) in enumerate(jobs):
                result = harness.run_worker(args.python, lib, spec, args)
                result.update(round=repetition, order=order, seed=args.seed)
                results.setdefault((spec.label, lib), []).append(result)
                summary = result.get("error") or f"{result['rps']:,.0f} rps, {result['cpu_us']:.1f} us/req"
                print(f"[{repetition}/{args.rounds}] {spec.label} {lib}: {summary}", flush=True)
                args.output.write_text(
                    json.dumps({f"{key[0]} {key[1]}": rows for key, rows in results.items()}, indent=2)
                )
    finally:
        server.terminate()
        server.wait(timeout=5)
    harness.print_table(specs, libs, results)
    if any("error" in row for rows in results.values() for row in rows):
        raise RuntimeError("Some benchmark runs failed; inspect the raw results")


if __name__ == "__main__":
    main()
