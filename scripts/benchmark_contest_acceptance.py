"""Bounded 60s mixed workload: 120 users + three complete official replays.

Separately records historical replay rate, not 100k/s sustained input capacity.
No paid models, deployment changes or private credentials in the output.
"""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    stamp = time.strftime("%Y%m%d-%H%M%S")
    folder = ROOT / "tmp" / ("acceptance-load-" + stamp)
    folder.mkdir(parents=True)
    before_http = set((ROOT / "docs/contest").glob("performance-http-node-*.json"))
    seconds = int(os.getenv("BENCH_SECONDS", "60"))
    repeats = int(os.getenv("BENCH_REPLAYS", "3"))
    rate = int(os.getenv("BENCH_RATE", "0"))
    env = {
        **os.environ,
        "BENCH_SECONDS": str(seconds),
        "BENCH_STAGGER": "1",
        "BENCH_SCENARIO": f"{seconds}s mixed load / 120 staggered users / {repeats} complete replays / configured input rate {rate} (0=unpaced)",
    }
    with (folder / "http.log").open("w") as log:
        http = subprocess.Popen(
            ["node", "scripts/benchmark_contest_http.mjs"],
            cwd=ROOT,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        reports = []
        for i in range(repeats):
            before = set((ROOT / "docs/contest").glob("performance-pipeline-*.json"))
            with (folder / ("pipeline-" + str(i) + ".log")).open("w") as output:
                subprocess.run(
                    [sys.executable, "scripts/benchmark_contest_pipeline.py"],
                    cwd=ROOT,
                    env=env,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    check=True,
                    timeout=240,
                )
            paths = (
                set((ROOT / "docs/contest").glob("performance-pipeline-*.json"))
                - before
            )
            if len(paths) != 1:
                raise AssertionError("Expected one independent pipeline report")
            report = json.loads(paths.pop().read_text())
            if (
                report["received_unique_states"] != report["expected_states"]
                or not report["batch_stream_parity"]
            ):
                raise AssertionError("Incomplete replay")
            reports.append({k: v for k, v in report.items() if k != "snapshots"})
        if http.wait(timeout=120):
            raise RuntimeError("HTTP test failed; see local log")
    files = (
        set((ROOT / "docs/contest").glob("performance-http-node-*.json")) - before_http
    )
    if len(files) != 1:
        raise AssertionError("Expected one HTTP report")
    path = files.pop()
    result = {
        "scenario": env["BENCH_SCENARIO"],
        "http_report": path.name,
        "replays": reports,
        "paid_calls": 0,
        "scope_limit": "bounded mixed soak; not a multi-day durability test or a 100k/s saturated-input benchmark",
    }
    destination = ROOT / "docs/contest" / ("mixed-acceptance-" + stamp + ".json")
    destination.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
