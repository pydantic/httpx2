"""
Benchmark orchestrator: starts the origin server, runs the client matrix, and prints a comparison table.

Examples:

    python run.py                                   # default matrix, current interpreter, httpx2 vs aiohttp
    python run.py --quick --lib httpx2              # three scenarios, one library
    python run.py --python main=../httpx2-main/.venv/bin/python --python work=.venv/bin/python

Each `--python LABEL=PATH` is an interpreter with the libraries installed; runs are interleaved across
interpreters and libraries on every round so machine noise affects all columns equally.

Two headline numbers are reported per column:

* `cpu us/req` at the smallest 1KB GET scenario: per-request CPU cost of the client stack.
* `retention`: throughput at the largest 1KB GET concurrency divided by throughput at the smallest.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import queue
import statistics
import subprocess
import sys
import threading
from dataclasses import dataclass
from typing import Any

HERE = pathlib.Path(__file__).parent
SERVER = HERE / "server.py"
CLIENT = HERE / "client.py"
DEFAULT_PORT = 8765

DEFAULT_SCENARIOS = ["c1/1k", "c16/1k", "c128/1k", "c512/1k", "c64/100k", "c8/5m", "c64/1k/post", "c8/1m/post"]
QUICK_SCENARIOS = ["c16/1k", "c512/1k", "c64/100k"]
PROFILE_LIBS = ("httpx2", "httpcore2")


@dataclass(frozen=True)
class ScenarioSpec:
    concurrency: int
    size: int
    post: bool

    @classmethod
    def parse(cls, text: str) -> ScenarioSpec:
        parts = text.split("/")
        valid = len(parts) in (2, 3) and parts[0].startswith("c") and (len(parts) == 2 or parts[2] == "post")
        if not valid:
            raise argparse.ArgumentTypeError(f"Expected 'c<concurrency>/<size>[/post]', got {text!r}.")
        return cls(concurrency=int(parts[0][1:]), size=parse_size(parts[1]), post=len(parts) == 3)

    @property
    def label(self) -> str:
        return f"c{self.concurrency}/{format_size(self.size)}" + ("/post" if self.post else "")


def parse_size(text: str) -> int:
    units = {"k": 1024, "m": 1024 * 1024}
    unit = text[-1].lower()
    if unit in units:
        return int(text[:-1]) * units[unit]
    return int(text)


def format_size(size: int) -> str:
    if size % (1024 * 1024) == 0:
        return f"{size // (1024 * 1024)}m"
    if size % 1024 == 0:
        return f"{size // 1024}k"
    return str(size)


def available_cpus() -> list[int]:
    # Respect the affinity mask: in containers the usable CPU IDs need not start at zero.
    if hasattr(os, "sched_getaffinity"):
        return sorted(os.sched_getaffinity(0))
    return list(range(os.cpu_count() or 1))


def parse_python(text: str) -> tuple[str, str]:
    label, sep, path = text.partition("=")
    if not sep:
        raise argparse.ArgumentTypeError(f"Expected 'LABEL=PATH', got {text!r}.")
    return label, path


def wait_for_server(server: subprocess.Popen[bytes], port: int, timeout: float = 10.0) -> None:
    # Wait for the server's readiness line rather than probing the port, so a failed
    # bind (for example, the port already in use) is an error instead of a silent
    # benchmark against whatever else is listening there.
    assert server.stdout is not None
    lines: queue.Queue[bytes] = queue.Queue()
    threading.Thread(target=lambda: lines.put(server.stdout.readline()), daemon=True).start()  # type: ignore[union-attr]
    try:
        line = lines.get(timeout=timeout)
    except queue.Empty:
        raise RuntimeError(f"Benchmark server did not start listening on port {port} within {timeout}s.") from None
    if not line.startswith(b"listening on "):
        code = server.wait(timeout=5)
        raise RuntimeError(f"Benchmark server exited with code {code}; is port {port} already in use?")


def interpreter_summary(python: str) -> str:
    code = (
        "import sys, json; gil = sys._is_gil_enabled() if sys.version_info >= (3, 13) else True; "
        "print(json.dumps({'python': sys.version.split()[0], 'gil': gil}))"
    )
    info = json.loads(subprocess.run([python, "-c", code], check=True, capture_output=True, text=True).stdout)
    return f"{info['python']}{'' if info['gil'] else ' (free-threaded)'}"


def run_worker(
    python: str, lib: str, spec: ScenarioSpec, args: argparse.Namespace, profile: pathlib.Path | None = None
) -> dict[str, Any]:
    command = [
        python,
        str(CLIENT),
        "--lib",
        lib,
        "--concurrency",
        str(spec.concurrency),
        "--size",
        str(spec.size),
        "--mode",
        args.mode,
        "--seconds",
        str(args.seconds),
        "--port",
        str(args.port),
    ]
    if spec.post:
        command.append("--post")
    if args.max_connections is not None:
        command += ["--max-connections", str(args.max_connections)]
    if args.client_cpu is not None:
        command += ["--cpu", str(args.client_cpu)]
    if args.no_zuvloop:
        command.append("--no-zuvloop")
    if profile is not None:
        command += ["--profile", str(profile)]

    completed = subprocess.run(command, capture_output=True, text=True)
    if completed.returncode != 0:
        tail = "\n".join(completed.stderr.strip().splitlines()[-3:])
        return {"error": tail or f"exit code {completed.returncode}"}
    result: dict[str, Any] = json.loads(completed.stdout.strip().splitlines()[-1])
    return result


def median(results: list[dict[str, Any]], key: str) -> float:
    return float(statistics.median(r[key] for r in results))


def print_table(
    specs: list[ScenarioSpec], columns: list[str], results: dict[tuple[str, str], list[dict[str, Any]]]
) -> None:
    header = ["scenario", *columns]
    rows = []
    for spec in specs:
        row = [spec.label]
        for column in columns:
            ok = [r for r in results.get((spec.label, column), []) if "error" not in r]
            if not ok:
                errors = [r["error"] for r in results.get((spec.label, column), []) if "error" in r]
                row.append(f"FAILED: {errors[0]}" if errors else "-")
                continue
            row.append(
                f"{median(ok, 'rps'):,.0f} rps · {median(ok, 'p50_ms'):.1f}/{median(ok, 'p99_ms'):.1f} ms"
                f" · {median(ok, 'cpu_us'):.0f} us"
            )
        rows.append(row)
    widths = [max(len(line[i]) for line in [header, *rows]) for i in range(len(header))]
    print("Cells: median rps · p50/p99 latency ms · CPU us per request")
    print("| " + " | ".join(cell.ljust(widths[i]) for i, cell in enumerate(header)) + " |")
    print("|" + "|".join("-" * (width + 2) for width in widths) + "|")
    for row in rows:
        print("| " + " | ".join(cell.ljust(widths[i]) for i, cell in enumerate(row)) + " |")


def print_kpis(
    specs: list[ScenarioSpec], columns: list[str], results: dict[tuple[str, str], list[dict[str, Any]]]
) -> None:
    get_1k = sorted((s for s in specs if s.size == 1024 and not s.post), key=lambda s: s.concurrency)
    if not get_1k:
        return
    low, high = get_1k[0], get_1k[-1]
    print()
    if high is low:
        print(f"KPIs (cpu at {low.label}):")
    else:
        print(f"KPIs (cpu at {low.label}, retention = rps {high.label} / rps {low.label}):")
    for column in columns:
        low_ok = [r for r in results.get((low.label, column), []) if "error" not in r]
        high_ok = [r for r in results.get((high.label, column), []) if "error" not in r]
        if not low_ok:
            continue
        line = f"  {column}: {median(low_ok, 'cpu_us'):.0f} us/req"
        if high_ok and high is not low:
            line += f", retention {median(high_ok, 'rps') / median(low_ok, 'rps'):.0%}"
        print(line)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--python", type=parse_python, action="append", default=[], metavar="LABEL=PATH", help="Interpreter to run."
    )
    parser.add_argument(
        "--lib", action="append", default=[], help="Client library (repeatable). Default: httpx2, aiohttp."
    )
    parser.add_argument(
        "--scenario", type=ScenarioSpec.parse, action="append", default=[], metavar="c<N>/<size>[/post]"
    )
    parser.add_argument("--quick", action="store_true", help=f"Use the short matrix: {', '.join(QUICK_SCENARIOS)}.")
    parser.add_argument("--mode", choices=("stream", "read"), default="stream")
    parser.add_argument("--rounds", type=int, default=2, help="Interleaved repetitions; medians are reported.")
    parser.add_argument("--seconds", type=float, default=4.0, help="Measured duration per run.")
    parser.add_argument("--max-connections", type=int, default=None)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument(
        "--server-cpu", type=int, default=None, help="Default: the first usable CPU when at least two are available."
    )
    parser.add_argument(
        "--client-cpu", type=int, default=None, help="Default: the second usable CPU when at least two are available."
    )
    parser.add_argument("--no-pin", action="store_true", help="Do not pin the server and clients to CPUs.")
    parser.add_argument(
        "--no-zuvloop", action="store_true", help="Use the stdlib event loop for the server and clients."
    )
    parser.add_argument("--profile", type=pathlib.Path, default=None, help="Directory for pyinstrument profiles.")
    parser.add_argument("--output", type=pathlib.Path, default=None, help="Write raw results as JSON.")
    args = parser.parse_args(argv)

    pythons: list[tuple[str, str]] = args.python or [("current", sys.executable)]
    if len({label for label, _ in pythons}) != len(pythons):
        parser.error("--python labels must be unique.")
    libs: list[str] = list(dict.fromkeys(args.lib or ["httpx2", "aiohttp"]))
    # Deduplicate by value, so `c16/1k` and `c16/1024` do not become two rows sharing one bucket.
    specs: list[ScenarioSpec] = list(
        dict.fromkeys(
            args.scenario or [ScenarioSpec.parse(s) for s in (QUICK_SCENARIOS if args.quick else DEFAULT_SCENARIOS)]
        )
    )
    cpus = available_cpus()
    if not args.no_pin and len(cpus) >= 2:
        args.server_cpu = cpus[0] if args.server_cpu is None else args.server_cpu
        args.client_cpu = cpus[1] if args.client_cpu is None else args.client_cpu

    columns = [f"{lib}@{label}" if len(pythons) > 1 else lib for label, _ in pythons for lib in libs]
    print(f"cpus: {len(cpus)}, server cpu: {args.server_cpu}, client cpu: {args.client_cpu}, mode: {args.mode}")
    for label, python in pythons:
        print(f"{label}: {python} ({interpreter_summary(python)})")
    print(f"rounds: {args.rounds}, {args.seconds}s per run, {len(specs)} scenarios, {len(columns)} columns")
    print()

    server_command = [sys.executable, str(SERVER), "--port", str(args.port)]
    if args.server_cpu is not None:
        server_command += ["--cpu", str(args.server_cpu)]
    if args.no_zuvloop:
        server_command.append("--no-zuvloop")
    server = subprocess.Popen(server_command, stdout=subprocess.PIPE)
    results: dict[tuple[str, str], list[dict[str, Any]]] = {}
    try:
        wait_for_server(server, args.port)
        for round_number in range(1, args.rounds + 1):
            for spec in specs:
                for label, python in pythons:
                    for lib in libs:
                        column = f"{lib}@{label}" if len(pythons) > 1 else lib
                        result = run_worker(python, lib, spec, args)
                        results.setdefault((spec.label, column), []).append(result)
                        summary = result.get("error") or f"{result['rps']:,.0f} rps, {result['cpu_us']:.0f} us/req"
                        print(f"[round {round_number}] {spec.label:<14} {column:<24} {summary}", flush=True)
        if args.profile is not None:
            args.profile.mkdir(parents=True, exist_ok=True)
            label, python = pythons[0]
            for spec in specs:
                for lib in libs:
                    if lib not in PROFILE_LIBS:
                        continue
                    path = args.profile / f"{lib}-{label}-{spec.label.replace('/', '-')}.txt"
                    result = run_worker(python, lib, spec, args, profile=path)
                    print(f"[profile] {spec.label:<14} {lib:<24} {result.get('error') or path}", flush=True)
    finally:
        server.terminate()
        server.wait(timeout=5)

    print()
    print_table(specs, columns, results)
    print_kpis(specs, columns, results)
    if args.output is not None:
        args.output.write_text(json.dumps({f"{k[0]} {k[1]}": v for k, v in results.items()}, indent=2))
        print(f"\nraw results: {args.output}")


if __name__ == "__main__":
    main()
