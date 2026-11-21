"""Persist a Flink savepoint location as a Compose override, without credentials."""

from __future__ import annotations
import json
import time
from pathlib import Path
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
BASE = "http://127.0.0.1:18081"


def api(path, body=None):
    request = Request(
        BASE + path,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urlopen(request, timeout=5) as response:
        return json.load(response)


def capture():
    jobs = [j for j in api("/jobs/overview")["jobs"] if j["state"] == "RUNNING"]
    if len(jobs) != 1:
        raise RuntimeError("需要一个健康的 Flink 作业才能保存恢复位置")
    jid = jobs[0]["jid"]
    trigger = api(
        "/jobs/" + jid + "/savepoints", {"target-directory": "s3://quantos/savepoints"}
    )["request-id"]
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        result = api("/jobs/" + jid + "/savepoints/" + trigger)
        if result["status"]["id"] == "COMPLETED":
            location = result.get("operation", {}).get("location")
            if not location:
                raise RuntimeError(
                    "保存点失败："
                    + json.dumps(result.get("operation", {}), ensure_ascii=False)[:500]
                )
            path = ROOT / ".quantos-contest" / "restore.json"
            override = {
                "services": {
                    "jobmanager": {
                        "command": [
                            "standalone-job",
                            "--job-classname",
                            "io.quantos.MarketFeatures",
                            "--fromSavepoint",
                            location,
                        ]
                    }
                }
            }
            temp = path.with_suffix(".part")
            temp.write_text(json.dumps(override, indent=2))
            temp.replace(path)
            print("Savepoint verified and saved for next start: " + location)
            return location
        time.sleep(1)
    raise TimeoutError("保存点45秒内未完成，保留服务以避免无记录停机")


if __name__ == "__main__":
    capture()
