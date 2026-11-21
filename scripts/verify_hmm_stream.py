"""Run inside data-api container. Frozen HMM actual Kafka/Flink vs batch reference."""

import json
import time
import os
from marketlab.storage import Lab
from marketlab.distributed import start_stream, publish, mysql, stream_rows
from marketlab.replay import create_run, advance
from marketlab.contracts import ReplayRequest

l = Lab()
model = "f7817b51d0d3aa6d431a2adb"
spec = ReplayRequest(model_id=model, start="2023-01-01", end="2026-05-02")
reference = os.getenv("REFERENCE_RUN") or create_run(l, spec)
advance(l, reference, 5000)
run = (
    {"id": os.environ["RESUME_RUN"]}
    if os.getenv("RESUME_RUN")
    else start_stream(l, model, "2023-01-01", "2026-05-02", 0.75, 5)
)
print(json.dumps({"reference": reference, "stream": run["id"]}), flush=True)
t = time.monotonic()
publish(l, run["id"])
print("published", flush=True)
for _ in range(600):
    with mysql() as c, c.cursor() as q:
        q.execute("SELECT * FROM stream_runs WHERE id=%s", (run["id"],))
        row = q.fetchone()
        q.execute("SELECT COUNT(*) n FROM outbox WHERE sent=FALSE")
        pending = q.fetchone()["n"]
    if row["status"] == "features_completed" and pending == 0:
        break
    time.sleep(1)
else:
    raise RuntimeError("stream did not complete in 600 seconds")
actual = {
    r["day"]: r["payload"] for r in stream_rows("stream_results", run["id"], 5000)
}
expected = {
    r["day"]: json.loads(r["payload"])
    for r in l.rows("SELECT day,payload FROM results WHERE run_id=?", (reference,))
}
if actual.keys() != expected.keys():
    raise ValueError("snapshot day mismatch")
error = max(
    abs(actual[d]["probabilities"][k] - expected[d]["probabilities"][k])
    for d in expected
    for k in expected[d]["probabilities"]
)
assert error < 1e-8
report = {
    "reference": reference,
    "stream": run["id"],
    "snapshots": len(actual),
    "max_probability_difference": error,
    "seconds": time.monotonic() - t,
    "model_id": model,
    "paid_calls": 0,
}
(l.root / "reports" / "hmm-stream-parity.json").write_text(json.dumps(report, indent=2))
print(json.dumps(report, indent=2), flush=True)
