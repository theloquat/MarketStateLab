"""Real Kafka/Flink path. SQLite is the offline reference, not the stream engine.

MySQL owns stream runs, idempotency, features and transactional outbox. ClickHouse
stores versioned facts; Redis is rebuildable. At-least-once delivery with stable IDs,
not an assertion of distributed cross-database exactly-once transactions.
"""

from __future__ import annotations

import argparse
import io
import orjson as json
import os
import time
import threading
import uuid
from functools import lru_cache
from datetime import date, timedelta

import httpx
import pandas as pd
import redis
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from minio import Minio

from .contracts import canonical, identity, utcnow
from .payloads import wire
from .features import CORE_NAMES
from .models import load
from .storage import Lab

TOPICS = [
    "quantos.observations.v1",
    "quantos.features.v1",
    "quantos.states.v1",
    "quantos.alerts.v1",
    "quantos.late.v1",
    "quantos.persisted-states.v1",
]


def password():
    value = os.environ.get("CONTEST_PASSWORD")
    if not value:
        raise ValueError("缺少赛事独立配置，请运行 scripts/contest.sh setup")
    return value


def mysql():
    from .connections import POOL

    return POOL.connection()


def s3():
    return Minio(
        os.getenv("CONTEST_MINIO", "127.0.0.1:19000"),
        access_key="quantos",
        secret_key=password(),
        secure=False,
    )


@lru_cache(maxsize=16)
def object_model(model_id, expected_hash):
    response = s3().get_object("quantos", "models/" + model_id + ".json")
    try:
        artifact = json.loads(response.read())
    finally:
        response.close()
        response.release_conn()
    if identity(artifact) != expected_hash:
        raise ValueError("object store model hash mismatch")
    return artifact


def cache():
    return redis.Redis(
        host=os.getenv("CONTEST_REDIS_HOST", "127.0.0.1"),
        port=int(os.getenv("CONTEST_REDIS_PORT", "6379")),
        socket_timeout=3,
    )


@lru_cache(maxsize=1)
def ch_client():
    return httpx.Client(
        timeout=30, limits=httpx.Limits(max_connections=4, max_keepalive_connections=4)
    )


def ch(query, data=None, database="quantos"):
    # Stable IDs + FINAL make bounded replay safe after uncertain HTTP outcomes.
    for attempt in range(4):
        try:
            r = ch_client().post(
                os.getenv("CONTEST_CLICKHOUSE", "http://127.0.0.1:18123"),
                params={"database": database, "query": query},
                content=data,
                auth=("quantos", password()),
            )
            if r.is_error:
                transient = r.status_code >= 500 and (
                    "MEMORY_LIMIT_EXCEEDED" in r.text
                    or r.status_code in (502, 503, 504)
                )
                if transient and attempt < 3:
                    time.sleep(0.5 * (2**attempt))
                    continue
                raise RuntimeError(f"ClickHouse {r.status_code}: {r.text[:1000]}")
            return r.text
        except httpx.TransportError:
            if attempt == 3:
                raise
            time.sleep(0.5 * (2**attempt))


def producer(linger_ms=10):
    return KafkaProducer(
        bootstrap_servers=os.getenv("CONTEST_KAFKA", "127.0.0.1:19092"),
        acks="all",
        retries=10,
        max_in_flight_requests_per_connection=1,
        value_serializer=lambda v: wire(v).encode(),
        compression_type="gzip",
        linger_ms=linger_ms,
    )


def bootstrap():
    with mysql() as c, c.cursor() as cur:
        for ddl in [
            "CREATE TABLE IF NOT EXISTS lab_meta(id VARCHAR(80) PRIMARY KEY, payload LONGTEXT NOT NULL)",
            "CREATE TABLE IF NOT EXISTS stream_runs(id VARCHAR(40) PRIMARY KEY,payload LONGTEXT NOT NULL,status VARCHAR(30),sent BIGINT DEFAULT 0,processed BIGINT DEFAULT 0,error TEXT)",
            "CREATE TABLE IF NOT EXISTS day_features(run_id VARCHAR(40),day VARCHAR(10),payload LONGTEXT NOT NULL,PRIMARY KEY(run_id,day))",
            "CREATE TABLE IF NOT EXISTS stream_results(run_id VARCHAR(40),day VARCHAR(10),payload LONGTEXT NOT NULL,PRIMARY KEY(run_id,day))",
            "CREATE TABLE IF NOT EXISTS stream_alerts(id VARCHAR(64) PRIMARY KEY,run_id VARCHAR(40),day VARCHAR(10),payload LONGTEXT,disposition VARCHAR(20) DEFAULT 'open',note TEXT)",
            "CREATE TABLE IF NOT EXISTS outbox(id VARCHAR(100) PRIMARY KEY,topic VARCHAR(80),payload LONGTEXT,sent BOOLEAN DEFAULT FALSE)",
            "CREATE TABLE IF NOT EXISTS worker_offsets(worker VARCHAR(40),partition_id INT,last_offset BIGINT,PRIMARY KEY(worker,partition_id))",
            "CREATE TABLE IF NOT EXISTS state_receipts(run_id VARCHAR(40),day VARCHAR(10),PRIMARY KEY(run_id,day))",
            "CREATE TABLE IF NOT EXISTS ch_outbox(id VARCHAR(100) PRIMARY KEY,table_name VARCHAR(20),payload LONGTEXT,sent BOOLEAN DEFAULT FALSE)",
            "CREATE TABLE IF NOT EXISTS worker_health(role VARCHAR(30) PRIMARY KEY,updated_at DATETIME(6))",
            "CREATE TABLE IF NOT EXISTS filter_states(run_id VARCHAR(40),day VARCHAR(10),payload TEXT,PRIMARY KEY(run_id,day))",
        ]:
            cur.execute(ddl)
        for table in ("outbox", "ch_outbox"):
            cur.execute(f"SHOW INDEX FROM {table} WHERE Key_name='pending_delivery'")
            if not cur.fetchone():
                cur.execute(f"CREATE INDEX pending_delivery ON {table}(sent,id)")
        c.commit()
    ch("CREATE DATABASE IF NOT EXISTS quantos", database="default")
    for table in ["observations", "features", "states", "alerts"]:
        ch(
            f"CREATE TABLE IF NOT EXISTS {table} (id String, run_id String, day Date, payload String) ENGINE=ReplacingMergeTree PARTITION BY toYYYYMM(day) ORDER BY (run_id,day,id)"
        )
    client = s3()
    if not client.bucket_exists("quantos"):
        client.make_bucket("quantos")
    admin = KafkaAdminClient(
        bootstrap_servers=os.getenv("CONTEST_KAFKA", "127.0.0.1:19092")
    )
    for topic in TOPICS:
        try:
            admin.create_topics(
                [
                    NewTopic(
                        topic,
                        num_partitions=1,
                        replication_factor=1,
                        topic_configs={
                            "retention.ms": "604800000",
                            "retention.bytes": "536870912",
                        },
                    )
                ]
            )
        except TopicAlreadyExistsError:
            pass
    admin.close()
    print("bootstrap ready", flush=True)


def put_object(key, content):
    s3().put_object(
        "quantos",
        key,
        io.BytesIO(content),
        len(content),
        content_type="application/json",
    )


def start_stream(
    lab,
    model_id,
    start="2023-01-01",
    end="2026-05-02",
    threshold=0.75,
    cooldown=5,
    rate_per_second=0,
):
    card, artifact = load(lab, model_id)
    if start > end:
        raise ValueError("start after end")
    if type(rate_per_second) is not int or not (
        rate_per_second == 0 or 500 <= rate_per_second <= 1000000
    ):
        raise ValueError("invalid replay input rate")
    rid = uuid.uuid4().hex[:24]
    metadata = lab.series(card["dataset_id"])
    spec = {
        "id": rid,
        "dataset_id": card["dataset_id"],
        "model_id": model_id,
        "inference_engine": os.getenv("CONTEST_INFERENCE_ENGINE", "python"),
        "start": start,
        "end": end,
        "warning_threshold": threshold,
        "cooldown_days": cooldown,
        "rate_per_second": rate_per_second,
        "engine": "kafka-flink-v1",
        "created_at": utcnow(),
        "core_ids": [s["id"] for s in metadata if s["name"] in CORE_NAMES],
    }
    obj = s3()
    # Data version and model are immutable and independently retrievable from object storage.
    manifest = lab.dataset(card["dataset_id"])
    obj.fput_object(
        "quantos", manifest["raw_object"], str(lab.root / manifest["raw_object"])
    )
    for s in metadata:
        if s["status"] == "ready":
            obj.fput_object("quantos", s["path"], str(lab.root / s["path"]))
    put_object("models/" + model_id + ".json", canonical(artifact).encode())
    with mysql() as c, c.cursor() as cur:
        # Separate replay watermark clock from historical decision dates. A shared
        # Flink job's watermark cannot move backwards when a new historical run starts.
        cur.execute(
            "INSERT IGNORE INTO lab_meta VALUES('replay_clock','\"2026-05-15\"')"
        )
        cur.execute("SELECT payload FROM lab_meta WHERE id='replay_clock' FOR UPDATE")
        clock_start = date.fromisoformat(
            json.loads(cur.fetchone()["payload"])
        ) + timedelta(days=10)
        cur.execute(
            "SELECT id FROM stream_runs WHERE status IN ('publishing','paused','pause_requested','publish_failed') LIMIT 1"
        )
        unfinished = cur.fetchone()
        if unfinished:
            raise ValueError(
                "先完成或恢复已有发布运行 " + unfinished["id"] + "，再建立新的历史回放"
            )
        spec["watermark_offset_days"] = (clock_start - date(2019, 10, 1)).days
        allocated_end = date.fromisoformat(end) + timedelta(
            days=spec["watermark_offset_days"] + 4
        )
        cur.execute(
            "UPDATE lab_meta SET payload=%s WHERE id='replay_clock'",
            (canonical(allocated_end.isoformat()),),
        )
        for key, payload in [
            ("dataset:" + card["dataset_id"], manifest),
            ("model:" + model_id, card),
            ("series:" + card["dataset_id"], metadata),
        ]:
            cur.execute(
                "INSERT INTO lab_meta VALUES(%s,%s) ON DUPLICATE KEY UPDATE payload=VALUES(payload)",
                (key, canonical(payload)),
            )
        cur.execute(
            "INSERT INTO stream_runs(id,payload,status) VALUES(%s,%s,%s)",
            (rid, canonical(spec), "publishing"),
        )
        c.commit()
    return spec


def publish_isolated(lab, rid):
    """Keep CPU-heavy replay serialization outside the HTTP/WS interpreter."""
    import re
    import subprocess
    import sys

    if not re.fullmatch(r"[a-f0-9]{24}", rid):
        raise ValueError("invalid stream run ID")
    path = lab.root / "reports" / ("publisher-" + rid + ".log")
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
    try:
        with os.fdopen(fd, "a") as output:
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "marketlab.distributed",
                    "publish",
                    "--run-id",
                    rid,
                ],
                env={**os.environ, "QUANTOS_CONTEST_DIR": str(lab.root.resolve())},
                stdout=output,
                stderr=subprocess.STDOUT,
                check=True,
                timeout=900,
            )
    except Exception:
        with mysql() as c, c.cursor() as cur:
            cur.execute(
                "UPDATE stream_runs SET status='publish_failed',error=COALESCE(error,'Isolated publisher failed; see local publisher log and resume explicitly.') WHERE id=%s AND status IN ('publishing','pause_requested')",
                (rid,),
            )
            c.commit()
        raise


def publish(lab, rid, max_records=None):
    with mysql() as lease, lease.cursor() as lock:
        lock.execute("SELECT GET_LOCK('quantos_stream_publisher',0) acquired")
        if not lock.fetchone()["acquired"]:
            raise ValueError("已有分布式发布器运行，不能重复启动")
        try:
            return _publish(lab, rid, max_records)
        finally:
            lock.execute("SELECT RELEASE_LOCK('quantos_stream_publisher')")


def _publish(lab, rid, max_records=None):
    with mysql() as c, c.cursor() as cur:
        cur.execute("SELECT * FROM stream_runs WHERE id=%s", (rid,))
        row = cur.fetchone()
    if not row:
        raise ValueError("stream run not found")
    spec = json.loads(row["payload"])
    frames = []
    for s in lab.series(spec["dataset_id"]):
        if s["status"] != "ready":
            continue
        f = pd.read_parquet(lab.root / s["path"])
        # Include genuine past observations for rolling warmup. Never future rows.
        f = f[(f.available_date >= "2019-10-01") & (f.available_date <= spec["end"])]
        frames.append(f)
    frame = (
        pd.concat(frames)
        .sort_values(["available_date", "series_id"])
        .reset_index(drop=True)
    )
    spec["input_records"] = len(frame)
    spec["expected_days"] = int(frame.available_date.nunique())
    with mysql() as c, c.cursor() as cur:
        cur.execute(
            "UPDATE stream_runs SET payload=%s WHERE id=%s", (canonical(spec), rid)
        )
        c.commit()
    pub = producer()
    raw_batch = []
    pending = []
    try:
        if spec.get("inference_engine") == "flink-compiled-v1" and row["sent"] == 0:
            card, artifact = load(lab, spec["model_id"])
            control = {
                "kind": "run_config",
                "stream_run_id": rid,
                "series_id": "_config",
                "event_id": identity([rid, "frozen-config-v1"]),
                "available_date": "2019-09-30",
                "watermark_date": (
                    date(2019, 9, 30)
                    + timedelta(days=spec.get("watermark_offset_days", 0))
                ).isoformat(),
                "run_config": {
                    "spec": spec,
                    "card": card,
                    "artifact_canonical": canonical(artifact),
                },
            }
            pub.send(TOPICS[0], value=control, key=rid.encode()).get(timeout=30)
        stop = (
            min(len(frame), row["sent"] + max_records)
            if max_records is not None
            else len(frame)
        )
        columns = list(frame.columns)
        publish_started = time.monotonic()
        for offset, values in enumerate(
            frame.iloc[row["sent"] : stop].itertuples(index=False, name=None),
            start=row["sent"],
        ):
            event = dict(zip(columns, values))
            for k, v in list(event.items()):
                if hasattr(v, "tolist"):
                    event[k] = v.tolist()
            event["stream_run_id"] = rid
            event["watermark_date"] = (
                date.fromisoformat(event["available_date"])
                + timedelta(days=spec.get("watermark_offset_days", 0))
            ).isoformat()
            raw_batch.append(
                {
                    "id": event["event_id"],
                    "run_id": rid,
                    "day": event["available_date"],
                    "payload": wire(event),
                }
            )
            pending.append(pub.send(TOPICS[0], value=event, key=rid.encode()))
            rate = spec.get("rate_per_second", 0)
            if rate and (offset + 1 - row["sent"]) % 32 == 0:
                delay = (offset + 1 - row["sent"]) / rate - (
                    time.monotonic() - publish_started
                )
                if delay > 0:
                    time.sleep(delay)
            if (offset + 1) % 500 == 0:
                for future in pending:
                    future.get(timeout=30)
                pending.clear()
                ch(
                    "INSERT INTO observations FORMAT JSONEachRow",
                    "\n".join(wire(v) for v in raw_batch),
                )
                raw_batch.clear()
                with mysql() as c, c.cursor() as cur:
                    cur.execute(
                        "SELECT status FROM stream_runs WHERE id=%s FOR UPDATE", (rid,)
                    )
                    requested = cur.fetchone()["status"] == "pause_requested"
                    cur.execute(
                        "UPDATE stream_runs SET sent=%s,status=%s WHERE id=%s",
                        (offset + 1, "paused" if requested else "publishing", rid),
                    )
                    c.commit()
                    if requested:
                        return {"id": rid, "status": "paused", "sent": offset + 1}
        for future in pending:
            future.get(timeout=30)
        if raw_batch:
            ch(
                "INSERT INTO observations FORMAT JSONEachRow",
                "\n".join(wire(v) for v in raw_batch),
            )
        if stop < len(frame):
            with mysql() as c, c.cursor() as cur:
                cur.execute(
                    "UPDATE stream_runs SET sent=%s,status=%s WHERE id=%s",
                    (stop, "paused", rid),
                )
                c.commit()
            return {"id": rid, "status": "paused", "sent": stop}
        # Explicit clock control advances watermarks. This is NOT fabricated market data.
        control = {
            "kind": "watermark_control",
            "stream_run_id": rid,
            "available_date": (
                date.fromisoformat(spec["end"]) + timedelta(days=3)
            ).isoformat(),
            "watermark_date": (
                date.fromisoformat(spec["end"])
                + timedelta(days=spec.get("watermark_offset_days", 0) + 3)
            ).isoformat(),
        }
        pub.send(TOPICS[0], value=control, key=rid.encode()).get(timeout=30)
        pub.flush()
        with mysql() as c, c.cursor() as cur:
            spec["input_records"] = len(frame)
            cur.execute(
                "UPDATE stream_runs SET sent=%s,status=%s,payload=%s WHERE id=%s",
                (len(frame), "published", canonical(spec), rid),
            )
            c.commit()
    except BaseException as exc:
        with mysql() as c, c.cursor() as cur:
            cur.execute(
                "UPDATE stream_runs SET status=%s,error=%s WHERE id=%s",
                ("publish_failed", str(exc), rid),
            )
            c.commit()
        raise
    finally:
        pub.close()


def outbox(cur, key, topic, payload):
    cur.execute(
        "INSERT IGNORE INTO outbox(id,topic,payload) VALUES(%s,%s,%s)",
        (key, topic, canonical(payload)),
    )


def enqueue_ch(cur, table, rows):
    if not rows:
        return
    # One durable delivery envelope per window, retaining every original fact ID.
    key = table + ":batch:" + identity([(row["run_id"], row["id"]) for row in rows])
    cur.execute(
        "INSERT IGNORE INTO ch_outbox(id,table_name,payload) VALUES(%s,%s,%s)",
        (key, table, wire({"batch": rows})),
    )


def ch_facts(payload):
    value = json.loads(payload)
    return value["batch"] if "batch" in value else [value]


def flush_ch():
    # Batch small daily facts; avoid thousands of tiny MergeTree parts. Session lock
    # permits either worker to recover a pending batch without double ownership.
    with mysql() as c, c.cursor() as cur:
        cur.execute("SELECT GET_LOCK('quantos_ch_flush',0) acquired")
        if not cur.fetchone()["acquired"]:
            return
        try:
            cur.execute("SELECT * FROM ch_outbox WHERE sent=FALSE LIMIT 100")
            rows = cur.fetchall()
            for table in ("features", "states", "alerts"):
                group = [r for r in rows if r["table_name"] == table]
                if group:
                    ch(
                        f"INSERT INTO {table} FORMAT JSONEachRow",
                        "\n".join(
                            wire(fact) for r in group for fact in ch_facts(r["payload"])
                        ),
                    )
                    cur.execute(
                        "UPDATE ch_outbox SET sent=TRUE WHERE id IN ("
                        + ",".join(["%s"] * len(group))
                        + ")",
                        tuple(r["id"] for r in group),
                    )
            c.commit()
        finally:
            cur.execute("SELECT RELEASE_LOCK('quantos_ch_flush')")


def drain(pub, role=None):
    with mysql() as c, c.cursor() as cur:
        if role:
            topics = [TOPICS[2]] if role == "features" else [TOPICS[3], TOPICS[5]]
            cur.execute(
                "SELECT * FROM outbox WHERE sent=FALSE AND topic IN ("
                + ",".join(["%s"] * len(topics))
                + ") ORDER BY id LIMIT 200",
                tuple(topics),
            )
        else:
            cur.execute("SELECT * FROM outbox WHERE sent=FALSE ORDER BY id LIMIT 200")
        rows = cur.fetchall()
        pending = []
        for row in rows:
            value = json.loads(row["payload"])
            pending.append(
                (
                    row,
                    value,
                    pub.send(
                        row["topic"],
                        value=value,
                        key=str(value.get("run_id", "")).encode(),
                    ),
                )
            )
        pipeline = cache().pipeline(transaction=False)
        for row, value, future in pending:
            future.get(timeout=30)
            if row["topic"] in (TOPICS[2], TOPICS[5]):
                # Model facts and outbox have already committed. Cache never leads SQL.
                pipeline.eval(
                    "local old=redis.call('GET',KEYS[1]); if not old or cjson.decode(old)['day']<=ARGV[2] then redis.call('SET',KEYS[1],ARGV[1],'EX',86400); return 1; end; return 0",
                    1,
                    "quantos:state:" + value["run_id"],
                    wire(value),
                    value["day"],
                )
            if row["topic"] in (TOPICS[2], TOPICS[3], TOPICS[5]):
                pipeline.publish(
                    "quantos:events",
                    wire(
                        {
                            "type": (
                                "state"
                                if row["topic"] in (TOPICS[2], TOPICS[5])
                                else "alert"
                            ),
                            "event_id": row["id"],
                            "payload": value,
                            "published_at_ns": time.time_ns(),
                        }
                    ),
                )
        if pending:
            pipeline.execute()
            cur.execute(
                "UPDATE outbox SET sent=TRUE WHERE id IN ("
                + ",".join(["%s"] * len(rows))
                + ")",
                tuple(row["id"] for row in rows),
            )
            c.commit()
        return len(rows)


def consume(role, stop_event=None):
    stop_event = stop_event or threading.Event()
    topic = TOPICS[1] if role == "features" else TOPICS[2]
    consumer = KafkaConsumer(
        bootstrap_servers=os.getenv("CONTEST_KAFKA", "kafka:9092"),
        group_id="quantos-" + role + "-v1",
        auto_offset_reset="none",
        enable_auto_commit=False,
        value_deserializer=json.loads,
        max_poll_records=64,
        fetch_max_wait_ms=10,
    )
    from .offsets import OffsetGuard, guarded_poll

    guard = OffsetGuard(consumer, role, mysql)
    consumer.subscribe([topic], listener=guard)
    wake = threading.Event()
    relay_stop = threading.Event()

    def relay():
        import logging

        pub = producer(linger_ms=0)
        try:
            while not relay_stop.is_set():
                wake.wait(0.1)
                wake.clear()
                try:
                    if drain(pub, role) >= 200:
                        wake.set()
                except Exception:
                    logging.exception(
                        "%s outbox delivery delayed; durable rows retained", role
                    )
                    relay_stop.wait(1)
        finally:
            pub.close()

    def materialize():
        import logging

        while not relay_stop.is_set():
            try:
                flush_ch()
            except Exception:
                logging.exception(
                    "Analytical delivery delayed; durable envelope retained"
                )
            relay_stop.wait(0.25)

    materializer = threading.Thread(
        target=materialize, name=role + "-columnar", daemon=True
    )
    materializer.start()
    worker = threading.Thread(target=relay, name=role + "-outbox", daemon=True)
    worker.start()
    print(role + " worker ready", flush=True)
    heartbeat = 0
    try:
        while not stop_event.is_set():
            batches = guarded_poll(consumer, guard)
            if time.monotonic() - heartbeat >= 5:
                with mysql() as c, c.cursor() as cur:
                    cur.execute(
                        "INSERT INTO worker_health VALUES(%s,UTC_TIMESTAMP(6)) ON DUPLICATE KEY UPDATE updated_at=UTC_TIMESTAMP(6)",
                        (role,),
                    )
                    c.commit()
                heartbeat = time.monotonic()
            from .batching import feature_messages, state_messages

            for _, messages in batches.items():
                if role == "features":
                    feature_messages(messages, role)
                else:
                    state_messages(messages, role)
                if messages:
                    wake.set()
                    consumer.commit()
            stop_event.wait(0 if any(batches.values()) else 0.01)
    finally:
        consumer.close()
        relay_stop.set()
        wake.set()
        worker.join(timeout=10)
        materializer.join(timeout=10)


def record_offset(cur, receipt):
    if receipt:
        cur.execute(
            "INSERT INTO worker_offsets VALUES(%s,%s,%s) ON DUPLICATE KEY UPDATE last_offset=GREATEST(last_offset,VALUES(last_offset))",
            receipt,
        )


def handle_features(batch, receipt=None):
    from .batching import feature_group

    return feature_group([batch], receipt)


def handle_state(result, receipt=None):
    from .batching import state_group

    return state_group([result], receipt)


def hot_state(rid):
    key = "quantos:state:" + rid
    value = cache().get(key)
    if value:
        return json.loads(value)
    with mysql() as c, c.cursor() as cur:
        cur.execute(
            "SELECT payload FROM stream_results WHERE run_id=%s ORDER BY day DESC LIMIT 1",
            (rid,),
        )
        row = cur.fetchone()
    if not row:
        return None
    cache().set(key, row["payload"], ex=86400, nx=True)
    return json.loads(row["payload"])


def infrastructure_status():
    checks = {}

    def check(name, fn):
        try:
            checks[name] = bool(fn())
        except Exception:
            checks[name] = False

    def db_check():
        with mysql() as c, c.cursor() as cur:
            cur.execute("SELECT 1")
            return bool(cur.fetchone())

    def workers():
        with mysql() as c, c.cursor() as cur:
            cur.execute(
                "SELECT role FROM worker_health WHERE updated_at>UTC_TIMESTAMP()-INTERVAL 30 SECOND"
            )
            return {r["role"] for r in cur.fetchall()} == {"features", "alerts"}

    def flink():
        url = os.getenv(
            "CONTEST_FLINK",
            (
                "http://jobmanager:8081"
                if os.getenv("CONTEST_MYSQL_HOST")
                else "http://127.0.0.1:18081"
            ),
        )
        r = httpx.get(url + "/jobs/overview", timeout=3)
        r.raise_for_status()
        return any(j["state"] == "RUNNING" for j in r.json()["jobs"])

    def kafka_check():
        admin = KafkaAdminClient(
            bootstrap_servers=os.getenv("CONTEST_KAFKA", "127.0.0.1:19092"),
            request_timeout_ms=3000,
            api_version_auto_timeout_ms=2000,
        )
        try:
            return set(TOPICS).issubset(set(admin.list_topics()))
        finally:
            admin.close()

    check("mysql", db_check)
    check("redis", lambda: cache().ping())
    check("minio", lambda: s3().bucket_exists("quantos"))
    check("clickhouse", lambda: ch("SELECT 1").strip() == "1")
    check("flink", flink)
    check("kafka", kafka_check)
    check("workers", workers)
    return {
        "available": checks.get("mysql", False),
        "ready": all(checks.values()),
        "components": checks,
    }


def stream_rows(table, rid=None, limit=500):
    if table not in {"stream_runs", "stream_results", "stream_alerts"}:
        raise ValueError("invalid table")
    with mysql() as c, c.cursor() as cur:
        if rid:
            column = "id" if table == "stream_runs" else "run_id"
            order = "id" if table == "stream_runs" else "day"
            cur.execute(
                f"SELECT * FROM {table} WHERE {column}=%s ORDER BY {order} LIMIT %s",
                (rid, limit),
            )
        else:
            cur.execute(f"SELECT * FROM {table} LIMIT %s", (limit,))
        return [{**r, "payload": json.loads(r["payload"])} for r in cur.fetchall()]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("role", choices=["bootstrap", "features", "alerts", "publish"])
    p.add_argument("--run-id")
    a = p.parse_args()
    if a.role == "bootstrap":
        bootstrap()
    elif a.role == "publish":
        publish(Lab(), a.run_id)
    else:
        consume(a.role)


if __name__ == "__main__":
    main()
