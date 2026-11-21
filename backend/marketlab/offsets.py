"""Fail closed on retention gaps. Offset ledger never advances before domain commit."""

from kafka.consumer.subscription_state import ConsumerRebalanceListener


def resume_offset(beginning, end, ledger, committed):
    desired = ledger + 1 if ledger is not None else committed
    if desired is None:
        if beginning != 0:
            raise ValueError(
                "Kafka历史已过期且没有消费基线；需显式从原始归档重建新运行"
            )
        desired = 0
    if not beginning <= desired <= end:
        raise ValueError(
            f"Kafka保留期缺口：需要偏移{desired}，当前范围[{beginning},{end}]；停止消费，须显式重建"
        )
    return desired


class OffsetGuard(ConsumerRebalanceListener):
    def __init__(self, consumer, role, mysql):
        self.consumer, self.role, self.mysql = consumer, role, mysql

    def on_partitions_revoked(self, revoked):
        pass

    def on_partitions_assigned(self, assigned):
        beginning = self.consumer.beginning_offsets(assigned)
        end = self.consumer.end_offsets(assigned)
        with self.mysql() as connection, connection.cursor() as cur:
            for tp in assigned:
                cur.execute(
                    "SELECT last_offset FROM worker_offsets WHERE worker=%s AND partition_id=%s",
                    (self.role, tp.partition),
                )
                row = cur.fetchone()
                ledger = row["last_offset"] if row else None
                wanted = resume_offset(
                    beginning[tp], end[tp], ledger, self.consumer.committed(tp)
                )
                self.consumer.seek(tp, wanted)


def guarded_poll(consumer, guard, timeout_ms=250):
    from kafka.errors import NoOffsetForPartitionError, OffsetOutOfRangeError

    try:
        return consumer.poll(timeout_ms=timeout_ms)
    except (NoOffsetForPartitionError, OffsetOutOfRangeError):
        # Kafka can request initial positions before assignment callbacks finish.
        # Never let an unset/expired position silently choose the newest record.
        guard.on_partitions_assigned(consumer.assignment())
        return consumer.poll(timeout_ms=timeout_ms)
