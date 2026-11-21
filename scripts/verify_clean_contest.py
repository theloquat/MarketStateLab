#!/usr/bin/env python3
"""Build acceptance for a disposable Compose project with fresh volumes and credentials."""

from __future__ import annotations
import json
import os
import secrets
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path
import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))
from marketlab.access_setup import initialize_access  # noqa: E402


def run(command, log, timeout=300):
    with log.open("ab") as output:
        subprocess.run(
            command,
            cwd=ROOT,
            stdout=output,
            stderr=subprocess.STDOUT,
            check=True,
            timeout=timeout,
        )


def wait_http(client, path="/health", timeout=180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            response = client.get(path)
            if response.status_code == 200:
                return response.json()
        except httpx.HTTPError:
            pass
        time.sleep(1)
    raise TimeoutError("HTTP readiness deadline: " + path)


def wait_task(client, response, timeout=900):
    response.raise_for_status()
    tid = response.json()["task_id"]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        response = client.get("/api/lab/tasks/" + tid)
        response.raise_for_status()
        task = response.json()
        if task["status"] == "completed":
            return task["detail"]
        if task["status"] not in ("queued", "running"):
            raise RuntimeError(
                "Task "
                + task["kind"]
                + " failed: "
                + str(task["detail"].get("error", "unknown"))
            )
        time.sleep(1)
    raise TimeoutError("Task deadline exceeded: " + tid)


def main():
    tag = uuid.uuid4().hex[:10]
    folder = ROOT / "tmp/clean-acceptance" / tag
    folder.mkdir(parents=True)
    workspace = folder / "workspace"
    initialize_access(workspace)
    env_file = folder / "secrets.env"
    values = {
        k: secrets.token_hex(32)
        for k in [
            "CONTEST_PASSWORD",
            "CONTEST_ROOT_PASSWORD",
            "CONTEST_JWT_SECRET",
            "CONTEST_INTERNAL_TOKEN",
        ]
    }
    values.update(CONTEST_UID=str(os.getuid()), CONTEST_GID=str(os.getgid()))
    fd = os.open(env_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as output:
        output.write("".join(k + "=" + v + "\n" for k, v in values.items()))
    services = {
        name: {"env_file": [str(env_file)], "volumes": [str(workspace) + ":/contest"]}
        for name in ["bootstrap", "data-api", "feature-model", "alerts"]
    }
    services["gateway"] = {"env_file": [str(env_file)]}
    # Bind only the gateway on a spare loopback port. Every other endpoint
    # remains inside the disposable Compose network.
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    base_url = f"http://127.0.0.1:{port}"
    override = folder / "compose.json"
    override.write_text(json.dumps({"services": services}))
    ports = folder / "ports.yml"
    ports.write_text(
        "services:\n"
        + "".join(f"  {name}:\n    ports: !override []\n" for name in
                  ("mysql", "redis", "kafka", "clickhouse", "minio", "jobmanager"))
        + f"  gateway:\n    ports: !override ['127.0.0.1:{port}:8080']\n"
    )
    project = "quantos-clean-" + tag
    compose = [
        "docker",
        "compose",
        "-p",
        project,
        "--env-file",
        str(env_file),
        "-f",
        str(ROOT / "docker-compose.yml"),
        "-f",
        str(override),
        "-f", str(ports),
    ]
    report = {
        "created_at": time.time(),
        "isolated_project": project,
        "checks": {},
        "paid_calls": 0,
        "uses_prebuilt_images": True,
        "image_build_from_source_separate_gate": True,
        "image_ids": {
            name: subprocess.check_output(
                ["docker", "image", "inspect", "--format", "{{.Id}}", name],
                cwd=ROOT,
                text=True,
            ).strip()
            for name in [
                "quantos-contest:local",
                "quantos-flink:local",
                "quantos-gateway:local",
            ]
        },
    }
    checks = report["checks"]
    try:
        run(
            compose + ["up", "-d", "--no-build"],
            folder / "fresh-start.log",
            timeout=300,
        )
        with httpx.Client(base_url=base_url, timeout=30) as client:
            wait_http(client)
            password = next(
                line
                for line in (workspace / "access/credentials.txt")
                .read_text()
                .splitlines()
                if line.startswith("admin ")
            ).split()[1]
            login = client.post(
                "/auth/login", json={"username": "admin", "password": password}
            )
            login.raise_for_status()
            summary = client.get("/api/lab/summary")
            summary.raise_for_status()
            summary = summary.json()
            checks["starts_empty"] = (
                not summary["datasets"]
                and not summary["models"]
                and summary["llm_calls"] == 0
            )
            manifest = wait_task(client, client.post("/api/lab/import"), timeout=1200)
            checks["official_asset_count"] = manifest["series_count"] == 39
            checks["official_observation_count"] = (
                manifest["observation_count"] == 314318
            )
            model = wait_task(
                client,
                client.post(
                    "/api/lab/models",
                    json={
                        "method": "hmm",
                        "components": 3,
                        "reduction": "pca95",
                        "train_end": "2022-12-31",
                    },
                ),
            )
            spec = {
                "model_id": model["id"],
                "start": "2020-03-01",
                "end": "2020-06-01",
                "warning_threshold": 0.75,
            }
            response = client.post("/api/lab/runs", json=spec)
            response.raise_for_status()
            reference = response.json()["id"]
            wait_task(
                client,
                client.post(
                    "/api/lab/runs/" + reference + "/advance", json={"steps": 5000}
                ),
            )
            deadline = time.monotonic() + 120
            while time.monotonic() < deadline:
                state = client.get("/api/lab/stream").json()
                if state.get("ready"):
                    break
                time.sleep(1)
            else:
                raise TimeoutError("Fresh distributed workers/Flink not ready")
            stream = wait_task(client, client.post("/api/lab/stream", json=spec))
            rid = stream["id"]
            deadline = time.monotonic() + 180
            while time.monotonic() < deadline:
                runs = client.get("/api/lab/stream").json()["runs"]
                current = next(row for row in runs if row["id"] == rid)
                if current["status"] == "features_completed":
                    break
                time.sleep(1)
            else:
                raise TimeoutError("Fresh stream failed to complete")
            batch = client.get(
                "/api/lab/runs/" + reference + "/results?limit=5000"
            ).json()["items"]
            streamed = client.get("/api/lab/stream/" + rid).json()["items"]
            left = {row["day"]: row for row in batch}
            right = {row["day"]: row["payload"] for row in streamed}
            checks["same_snapshot_days"] = bool(left) and left.keys() == right.keys()
            difference = max(
                abs(left[d]["probabilities"][k] - right[d]["probabilities"][k])
                for d in left
                for k in left[d]["probabilities"]
            )
            checks["same_probabilities"] = difference < 1e-8
            checks["historical_in_sample_marked"] = all(
                r["evaluation"] == "in_sample" for r in right.values()
            )
            research = client.post(
                "/api/lab/research",
                json={
                    "tool": "quality_audit",
                    "hypothesis": "从空环境核对官方导入质量，缺失和未知时点不能伪造",
                },
            )
            research.raise_for_status()
            research_id = research.json()["id"]
            decision = client.post(
                "/api/lab/research/" + research_id + "/decision",
                json={"status": "approved", "note": "isolated clean-start acceptance"},
            )
            decision.raise_for_status()
            result = wait_task(
                client, client.post("/api/lab/research/" + research_id + "/execute")
            )
            checks["research_tool_executes"] = (
                result["tool"] == "quality_audit" and result["llm_calls"] == 0
            )
            report.update(
                dataset_id=manifest["id"],
                model_id=model["id"],
                run_id=rid,
                reference_id=reference,
                snapshots=len(left),
                max_probability_error=difference,
            )
            checks["no_llm_config_inherited"] = (
                client.get("/api/lab/admin/llm").json()["configured"] is False
            )
        # Read all real stores from inside this isolated network, never the original.
        diagnostic = """import json,time
from marketlab.distributed import mysql,ch
for _ in range(120):
 with mysql() as c,c.cursor() as q:
  q.execute('SELECT COUNT(*) n FROM ch_outbox WHERE sent=FALSE');pending=q.fetchone()['n']
  q.execute('SELECT COUNT(*) n FROM outbox WHERE sent=FALSE');pending+=q.fetchone()['n']
 if not pending:break
 time.sleep(.5)
counts={t:int(ch('SELECT count() FROM '+t+' FINAL').strip()) for t in ['observations','features','states','alerts']}
print(json.dumps({'pending':pending,'counts':counts}))"""
        result = subprocess.run(
            compose + ["exec", "-T", "data-api", "python", "-c", diagnostic],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=True,
            timeout=90,
        )
        stores = json.loads(result.stdout)
        report["stores"] = stores
        checks["durable_outboxes_drained"] = stores["pending"] == 0
        checks["columnar_observation_feature_counts_equal"] = (
            stores["counts"]["observations"] > 0
            and stores["counts"]["observations"] == stores["counts"]["features"]
        )
        checks["columnar_state_count"] = (
            stores["counts"]["states"] == report["snapshots"]
        )
    except Exception as exc:
        report["error"] = type(exc).__name__ + ": " + str(exc)[:1000]
    finally:
        try:
            run(compose + ["down", "--volumes", "--timeout", "15"], folder / "fresh-stop.log")
            checks["temporary_containers_and_volumes_removed"] = True
        except Exception as exc:
            report["cleanup_error"] = type(exc).__name__ + ": " + str(exc)[:500]
        if "error" not in report and "cleanup_error" not in report:
            import shutil
            shutil.rmtree(folder)
        report["passed"] = "error" not in report and "cleanup_error" not in report and all(checks.values())
        report["finished_at"] = time.time()
        destination = ROOT / "docs/contest" / (
            "clean-start-" + time.strftime("%Y%m%d") + "-" + tag + ".json"
        )
        destination.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
        print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
