"""Run in data-api container. Deletes ONLY records created in a new probe topic."""

import json, time, uuid
from pathlib import Path
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from marketlab.offsets import OffsetGuard, guarded_poll
from marketlab.distributed import mysql

scope = uuid.uuid4().hex[:16]
topic = "quantos.retention-probe." + scope
role = "probe-" + scope
admin = KafkaAdminClient(bootstrap_servers="kafka:9092")
admin.create_topics([NewTopic(topic, 1, 1, topic_configs={"retention.ms": "60000"})])
producer = KafkaProducer(bootstrap_servers="kafka:9092")
for i in range(5):
    producer.send(topic, json.dumps({"synthetic_probe": True, "i": i}).encode()).get(
        timeout=10
    )
producer.close()
partition = TopicPartition(topic, 0)
admin.delete_records({partition: 4})
with mysql() as c, c.cursor() as q:
    q.execute("INSERT INTO worker_offsets VALUES(%s,0,0)", (role,))
    c.commit()
consumer = KafkaConsumer(
    bootstrap_servers="kafka:9092",
    group_id=role,
    auto_offset_reset="none",
    enable_auto_commit=False,
)
guard = OffsetGuard(consumer, role, mysql)
consumer.subscribe([topic], listener=guard)
blocked = False
message = ""
try:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        guarded_poll(consumer, guard)
except ValueError as exc:
    message = str(exc)
    blocked = "保留期缺口" in message
finally:
    consumer.close()
    admin.close()
if not blocked:
    raise AssertionError("retention gap was not rejected")
result = {
    "synthetic_topic": topic,
    "required_offset": 1,
    "new_beginning_offset": 4,
    "silently_skipped": False,
    "blocked": blocked,
    "error": message,
    "official_topics_modified": False,
    "paid_calls": 0,
}
Path("/contest/reports/retention-gap.json").write_text(
    json.dumps(result, ensure_ascii=False, indent=2)
)
print(json.dumps(result, ensure_ascii=False, indent=2))
