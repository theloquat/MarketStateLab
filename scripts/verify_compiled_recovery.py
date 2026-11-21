"""Real SIGKILL recovery and quarantine probe. No deletion or paid model calls."""

import json
import subprocess
import time
from pathlib import Path
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = [
    "docker",
    "compose",
    "--env-file",
    "deploy/contest/secrets.env",
    "-f",
    "docker-compose.yml",
    "-f",
    ".quantos-contest/restore.json",
]


def inside(code):
    result = subprocess.run(
        COMPOSE + ["exec", "-T", "data-api", "timeout", "120", "python", "-c", code],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode:
        (ROOT / "tmp/compiled-probe-error.log").write_text(result.stderr)
        raise RuntimeError(
            "Container probe failed; inspect tmp/compiled-probe-error.log"
        )
    return result.stdout.strip()


def main():
    rid = inside(
        """
from marketlab.storage import Lab
from marketlab.distributed import start_stream,publish
import json
from marketlab.replay import create_run,advance
from marketlab.contracts import ReplayRequest
l=Lab();cards=[json.loads(r['card']) for r in l.rows('SELECT card FROM models ORDER BY rowid DESC')]
mid=next((c['id'] for c in cards if c['method']=='hmm'),None)
if not mid: raise RuntimeError('Train a frozen HMM before this fault test')
spec=ReplayRequest(model_id=mid,start='2023-01-01',end='2026-05-02')
refs=[r for r in l.rows('SELECT * FROM runs') if all(json.loads(r['spec']).get(k)==v for k,v in [('model_id',mid),('start','2023-01-01'),('end','2026-05-02'),('warning_threshold',.75),('cooldown_days',5)])]
if not refs or refs[0]['status']!='completed':
 ref=refs[0]['id'] if refs else create_run(l,spec);advance(l,ref,5000)
s=start_stream(l,mid,'2023-01-01','2026-05-02',.75,5)
publish(l,s['id'],35000);print(s['id'])
"""
    )
    assert len(rid) == 24
    # Capture while the run is partially processed, not after completion.
    subprocess.run(
        [str(ROOT / ".venv/bin/python"), "scripts/contest_checkpoint.py"],
        cwd=ROOT,
        check=True,
    )
    log = ROOT / "tmp/compiled-recovery-publisher.log"
    with log.open("w") as output:
        process = subprocess.Popen(
            COMPOSE
            + [
                "exec",
                "-T",
                "data-api",
                "python",
                "-c",
                f"from marketlab.storage import Lab;from marketlab.distributed import publish;publish(Lab(),'{rid}')",
            ],
            cwd=ROOT,
            stdout=output,
            stderr=subprocess.STDOUT,
        )
        time.sleep(0.2)
        killed_at = time.time()
        subprocess.run(
            COMPOSE
            + ["kill", "-s", "SIGKILL", "taskmanager", "feature-model", "alerts"],
            cwd=ROOT,
            check=True,
        )
        subprocess.run(
            COMPOSE
            + [
                "up",
                "-d",
                "--no-build",
                "--no-deps",
                "taskmanager",
                "feature-model",
                "alerts",
            ],
            cwd=ROOT,
            check=True,
        )
        if process.wait(timeout=120):
            raise RuntimeError("Publisher failed; see private local log")
    for _ in range(180):
        state = json.loads(
            inside(
                f"""
import json
from marketlab.distributed import mysql
with mysql() as c,c.cursor() as q:
 q.execute('SELECT status,sent FROM stream_runs WHERE id=%s',('{rid}',));r=q.fetchone()
 q.execute('SELECT COUNT(*) n FROM stream_results WHERE run_id=%s',('{rid}',));r['states']=q.fetchone()['n']
 q.execute('SELECT (SELECT COUNT(*) FROM outbox WHERE sent=FALSE)+(SELECT COUNT(*) FROM ch_outbox WHERE sent=FALSE) n');r['pending']=q.fetchone()['n']
print(json.dumps(r))
"""
            )
        )
        if (
            state["status"] == "features_completed"
            and state["states"] == 861
            and state["pending"] == 0
        ):
            break
        time.sleep(1)
    else:
        raise TimeoutError(str(state))
    recovery_seconds = time.time() - killed_at
    # Read a genuine final observation, then inject an exact duplicate, an older
    # timestamp and a conflicting identity. They must not change accepted facts.
    quarantine = json.loads(
        inside(
            f"""
import json,uuid
from kafka import KafkaConsumer,TopicPartition,KafkaProducer
from marketlab.distributed import mysql,producer,TOPICS
from marketlab.storage import Lab
import pandas as pd
with mysql() as c,c.cursor() as q:
 q.execute("SELECT payload FROM day_features WHERE run_id=%s ORDER BY day DESC LIMIT 1",('{rid}',));f=json.loads(q.fetchone()['payload'])['features'][0]
l=Lab();series=next(s for s in l.series() if s['id']==f['series_id'])
event=pd.read_parquet(l.root/series['path'],filters=[('event_id','==',f['event_id'])]).iloc[0].to_dict()
event={{k:(v.tolist() if hasattr(v,'tolist') else v) for k,v in event.items()}}
event.update(stream_run_id='{rid}',watermark_date=f['watermark_date']);tag=uuid.uuid4().hex
consumer=KafkaConsumer(bootstrap_servers='kafka:9092',enable_auto_commit=False,consumer_timeout_ms=1000)
tp=TopicPartition(TOPICS[4],0);consumer.assign([tp]);consumer.seek(tp,consumer.end_offsets([tp])[tp])
pub=producer(0);pub.send(TOPICS[0],event).get(30)
revision={{**event,'event_id':tag+'-revision'}}
older={{**event,'event_id':tag+'-late','available_date':'2019-01-01'}}
bad_date={{**event,'event_id':tag+'-invalid-date','available_date':'not-a-date'}}
for e in [revision,older,bad_date]:pub.send(TOPICS[0],e).get(30)
pub.close()
rawpub=KafkaProducer(bootstrap_servers='kafka:9092');rawpub.send(TOPICS[0],('{{broken:'+tag).encode()).get(30);rawpub.close()
import time
found={{}};duplicate_emitted=False;deadline=time.monotonic()+20
while time.monotonic()<deadline and len(found)<4:
 for messages in consumer.poll(timeout_ms=500).values():
  for m in messages:
   e=json.loads(m.value)
   if e.get('event_id')==event['event_id']:duplicate_emitted=True
   if e.get('event_id','').startswith(tag):found[e['event_id']]=e.get('rejection_reason')
   if e.get('rejection_reason')=='invalid_json' and tag in e.get('rejected_payload',''):found[tag+'-invalid-json']='invalid_json'
consumer.close();print(json.dumps({{'reasons':sorted(found.values()),'duplicate_emitted':duplicate_emitted}}))
"""
        )
    )
    if quarantine["duplicate_emitted"] or quarantine["reasons"] != [
        "conflicting_revision",
        "invalid_date",
        "invalid_json",
        "late",
    ]:
        raise AssertionError(quarantine)
    evidence = ROOT / (
        "docs/contest/compiled-recovery-" + time.strftime("%Y%m%d-%H%M%S") + ".json"
    )
    verification = evidence.with_name(evidence.stem + "-parity.json")
    subprocess.run(
        [
            str(ROOT / ".venv/bin/python"),
            "scripts/verify_contest.py",
            "--run-id",
            rid,
            "--output",
            str(verification),
        ],
        cwd=ROOT,
        check=True,
    )
    with urlopen("http://127.0.0.1:18081/jobs/overview") as response:
        running = [j for j in json.load(response)["jobs"] if j["state"] == "RUNNING"]
    report = {
        "run_id": rid,
        "killed_services": ["taskmanager", "feature-model", "alerts"],
        "signal": "SIGKILL",
        "partial_source_records": 35000,
        "recovery_seconds_including_catchup": recovery_seconds,
        "final": state,
        "quarantine": quarantine,
        "healthy_flink_jobs": len(running),
        "parity_report": verification.name,
        "paid_calls": 0,
    }
    assert len(running) == 1
    evidence.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
