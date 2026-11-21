"""Small bounded MySQL pool, including advisory-lock lease connections."""

from __future__ import annotations
import os
import queue
import threading
from contextlib import contextmanager
import pymysql


class MysqlPool:
    def __init__(self, size=8):
        self.available = queue.LifoQueue(maxsize=size)
        self.slots = threading.BoundedSemaphore(size)
        self.local = threading.local()

    @contextmanager
    def connection(self):
        shared = getattr(self.local, "connection", None)
        if shared is not None:
            yield shared
            return
        if not self.slots.acquire(timeout=5):
            raise RuntimeError("数据库连接繁忙，请稍后重试")
        connection = None
        try:
            try:
                connection = self.available.get_nowait()
                connection.ping(reconnect=False)
            except (queue.Empty, pymysql.MySQLError):
                if connection and connection.open:
                    connection.close()
                secret = os.environ.get("CONTEST_PASSWORD")
                if not secret:
                    raise ValueError("缺少赛事独立配置")
                connection = pymysql.connect(
                    host=os.getenv("CONTEST_MYSQL_HOST", "127.0.0.1"),
                    port=int(os.getenv("CONTEST_MYSQL_PORT", "3306")),
                    user="quantos",
                    password=secret,
                    database="quantos",
                    charset="utf8mb4",
                    cursorclass=pymysql.cursors.DictCursor,
                    connect_timeout=5,
                    read_timeout=30,
                    write_timeout=30,
                )
            yield connection
            connection.rollback()
            self.available.put_nowait(connection)
            connection = None
        finally:
            if connection and connection.open:
                connection.close()
            self.slots.release()

    @contextmanager
    def transaction(self):
        if getattr(self.local, "connection", None) is not None:
            raise RuntimeError("Nested batch transaction is not supported")
        with self.connection() as connection:

            class DeferredCommit:
                def cursor(self):
                    return connection.cursor()

                def commit(self):
                    pass

            self.local.connection = DeferredCommit()
            try:
                yield
                connection.commit()
            finally:
                self.local.connection = None


POOL = MysqlPool()
