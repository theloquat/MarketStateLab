"""Bounded-memory, restartable readers for the supplied nested archive.

Archives are never extracted using archive paths. Original bytes remain immutable;
normalization errors are counted, not silently converted to zeros.
"""

from __future__ import annotations

import csv
import fcntl
import hashlib
import io
import math
import shutil
import zipfile
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook

from .contracts import Observation, POLICY, available_on, canonical, identity, utcnow
from .storage import Lab

csv.field_size_limit(32 * 1024 * 1024)


def file_hash(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def parse_day(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()[:10]
    text = str(value or "").strip()
    if not text:
        return None
    try:
        prefix = text.split(" ")[0].split("T")[0]
        parts = prefix.replace("/", "-").split("-")
        return date(*map(int, parts)).isoformat()
    except ValueError:
        return None


@contextmanager
def writer_lock(lab):
    with open(lab.root / "import.lock", "a") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ValueError("已有数据导入正在运行") from exc
        yield


def import_archive(lab: Lab, archive: Path, documents=True):
    with writer_lock(lab):
        return _import(lab, archive.resolve(), documents)


def _import(lab, archive, documents):
    sha = file_hash(archive)
    ds = identity({"archive": sha, "normalizer": "official-v1", "policy": POLICY})[:24]
    raw = lab.root / "raw" / sha
    raw.mkdir(exist_ok=True)
    inner_path = raw / "sources.zip"
    if not inner_path.exists():
        with zipfile.ZipFile(archive) as outer:
            members = [
                n
                for n in outer.namelist()
                if n.endswith(".zip") and not n.endswith("/")
            ]
            if len(members) != 1:
                raise ValueError("expected exactly one inner data archive")
            with outer.open(members[0]) as src, open(raw / "sources.part", "wb") as out:
                shutil.copyfileobj(src, out, length=1024 * 1024)
        (raw / "sources.part").replace(inner_path)
    manifest = {
        "id": ds,
        "archive_sha256": sha,
        "inner_sha256": file_hash(inner_path),
        "normalizer": "official-v1",
        "policy": POLICY,
        "imported_at": utcnow(),
        "documents_requested": documents,
        "raw_object": str(inner_path.relative_to(lab.root)),
        "limitations": [
            "日期+2自然日UTC为重建假设，非历史发布时间认证",
            "债券收益率不是债券投资收益；价格数据不是OHLCV",
            "文本与匿名指标仅供证据查询，尚未进入主状态模型",
            "导入和内部质量检查不等于供应商准确率或交易日完整性认证",
        ],
    }
    lab.execute(
        "INSERT OR REPLACE INTO datasets VALUES(?,?,?)",
        (ds, canonical(manifest), "importing"),
    )
    with zipfile.ZipFile(inner_path) as z:
        dictionary = next(n for n in z.namelist() if n.endswith("代码对照.csv"))
        with io.TextIOWrapper(
            z.open(dictionary), encoding="utf-8-sig", newline=""
        ) as f:
            catalog = list(csv.DictReader(f))
        for item in catalog:
            sid = item["Wind代码"].strip() or identity(item)[:16]
            filename = next(
                (n for n in z.namelist() if n.endswith("/" + item["名称"] + ".csv")),
                None,
            )
            meta = {
                "id": sid,
                "name": item["名称"],
                "asset_class": item["大类"],
                "quote_kind": item["字段"],
                "calendar": item["交易日历"],
                "notes": item.get("备注", ""),
                "status": "missing" if not filename else "pending",
                "dataset_id": ds,
            }
            if filename:
                meta = ingest_prices(lab, z, filename, meta)
            lab.execute(
                "INSERT OR REPLACE INTO series VALUES(?,?,?)",
                (ds, sid, canonical(meta)),
            )
        if documents:
            for name in sorted(
                n for n in z.namelist() if not n.endswith("/") and "目标资产" not in n
            ):
                if name.endswith((".csv", ".xlsx", ".zip")):
                    ingest_documents(lab, z, name, ds)
    manifest["series_count"] = sum(s["status"] == "ready" for s in lab.series(ds))
    manifest["observation_count"] = sum(s.get("count", 0) for s in lab.series(ds))
    manifest["document_count"] = lab.rows(
        "SELECT COUNT(*) n FROM documents WHERE dataset_id=?", (ds,)
    )[0]["n"]
    manifest["completed_at"] = utcnow()
    lab.execute(
        "UPDATE datasets SET manifest=?,status=? WHERE id=?",
        (canonical(manifest), "ready", ds),
    )
    return manifest


def ingest_prices(lab, z, filename, meta):
    content = z.read(filename)
    sha = hashlib.sha256(content).hexdigest()
    target = (
        lab.root
        / "lake"
        / meta["dataset_id"]
        / (identity(meta["id"])[:20] + ".parquet")
    )
    target.parent.mkdir(exist_ok=True)
    rows, errors, seen = [], [], set()
    with io.StringIO(content.decode("utf-8-sig"), newline="") as f:
        reader = csv.DictReader(f)
        for line, row in enumerate(reader, 2):
            try:
                day = date.fromisoformat(row["date"]).isoformat()
                if day in seen:
                    raise ValueError("duplicate date")
                value = float(row[meta["quote_kind"]])
                if not math.isfinite(value):
                    raise ValueError("nonfinite value")
                obs = Observation(
                    dataset_id=meta["dataset_id"],
                    series_id=meta["id"],
                    observation_date=day,
                    value=value,
                    quote_kind=meta["quote_kind"],
                    unit="percentage_point"
                    if meta["quote_kind"] == "yield"
                    else "provider_quote",
                    source_file=filename,
                    source_hash=sha,
                    source_row=line,
                    calendar=meta["calendar"],
                    quality_flags=["negative_preserved"] if value < 0 else [],
                )
                seen.add(day)
                rows.append(
                    {
                        **obs.model_dump(mode="json"),
                        "event_id": obs.event_id,
                        "available_date": available_on(day),
                    }
                )
            except (ValueError, KeyError) as exc:
                errors.append({"source_row": line, "error": str(exc), "raw": row})
    if not rows:
        raise ValueError(f"no valid observations: {filename}")
    df = pd.DataFrame(rows).sort_values("observation_date")
    df.to_parquet(target.with_suffix(".part"), index=False)
    target.with_suffix(".part").replace(target)
    report = {
        "bad_rows": errors,
        "negative_count": int((df.value < 0).sum()),
        "weekend_count": int(
            (pd.to_datetime(df.observation_date).dt.weekday >= 5).sum()
        ),
        "calendar_completeness": "not_certified",
        "availability_grade": "B_reconstructed",
    }
    return {
        **meta,
        "status": "ready",
        "path": str(target.relative_to(lab.root)),
        "source_file": filename,
        "source_hash": sha,
        "unit": rows[0]["unit"],
        "count": len(rows),
        "start": df.observation_date.iloc[0],
        "end": df.observation_date.iloc[-1],
        "quality": report,
    }


def ingest_documents(lab, z, name, ds):
    previous = lab.rows(
        "SELECT * FROM sources WHERE dataset_id=? AND path=?", (ds, name)
    )
    if previous and previous[0]["status"] == "ready":
        return
    lab.execute(
        "INSERT OR REPLACE INTO sources VALUES(?,?,?,?,?)",
        (ds, name, "importing", 0, "{}"),
    )
    batch, count, invalid = [], 0, 0

    def add(kind, line, title, body, day, raw, trust="C_unknown_time"):
        nonlocal count, invalid
        count += 1
        if day is None:
            invalid += 1
        batch.append(
            (
                identity([ds, name, line]),
                ds,
                kind,
                day,
                str(title or "")[:1000],
                str(body or ""),
                name,
                line,
                canonical(raw),
                trust,
            )
        )
        if len(batch) >= 1000:
            flush()

    def flush():
        if not batch:
            return
        with lab.db() as c:
            c.executemany(
                "INSERT OR IGNORE INTO documents VALUES(?,?,?,?,?,?,?,?,?,?)", batch
            )
            c.execute(
                "UPDATE sources SET count=? WHERE dataset_id=? AND path=?",
                (count, ds, name),
            )
        batch.clear()

    try:
        if name.endswith(".xlsx"):
            wb = load_workbook(io.BytesIO(z.read(name)), read_only=True, data_only=True)
            for sheet in wb:
                iterator = sheet.iter_rows(values_only=True)
                header = [str(v) for v in next(iterator)]
                for line, row in enumerate(iterator, 2):
                    record = dict(zip(header, row))
                    if "日历" in name:
                        day = parse_day(record.get("OccurDate"))
                        title = record.get("IndicatorName")
                        kind = "calendar"
                    elif "指标集" in name:
                        day = parse_day(row[0])
                        title = sheet.title
                        kind = "anonymous_macro"
                    else:
                        day = parse_day(row[0])
                        title = next((str(v) for v in row[1:] if v is not None), name)
                        kind = "wechat" if "公众号" in name else "turnover"
                    # sheet in row identity prevents cross-sheet collisions
                    add(
                        kind,
                        f"{sheet.title}:{line}",
                        title,
                        canonical(record),
                        day,
                        record,
                    )
            wb.close()
        elif name.endswith(".zip"):
            cache = lab.root / "raw" / (identity([ds, name]) + ".zip")
            if not cache.exists():
                with z.open(name) as src, open(cache.with_suffix(".part"), "wb") as out:
                    shutil.copyfileobj(src, out, 1024 * 1024)
                cache.with_suffix(".part").replace(cache)
            with zipfile.ZipFile(cache) as nested:
                for member in sorted(
                    n for n in nested.namelist() if not n.endswith("/")
                ):
                    if member.endswith(".csv"):
                        with io.TextIOWrapper(
                            nested.open(member), encoding="utf-8-sig", newline=""
                        ) as f:
                            for line, row in enumerate(csv.reader(f), 1):
                                if len(row) != 11:
                                    raise ValueError(
                                        f"news schema: expected 11 columns at {line}, got {len(row)}"
                                    )
                                add(
                                    "news",
                                    f"{member}:{line}",
                                    row[1],
                                    row[2],
                                    parse_day(row[9]),
                                    {
                                        "provider_id": row[0],
                                        "source": row[3],
                                        "columns_4_to_10": row[4:],
                                    },
                                )
                    elif member.endswith(".md"):
                        body = nested.read(member).decode("utf-8-sig")
                        import re

                        match = re.search(
                            r"20\d{2}-\d{2}-\d{2}", member + "\n" + body[:500]
                        )
                        add(
                            "fund_summary",
                            member,
                            member.split("/")[-1],
                            body,
                            parse_day(match[0]) if match else None,
                            {"member": member},
                        )
        elif name.endswith(".csv"):
            with io.TextIOWrapper(z.open(name), encoding="utf-8-sig", newline="") as f:
                for line, row in enumerate(csv.DictReader(f), 2):
                    day = parse_day(
                        row.get("created_at_twitter")
                        or row.get("created_at")
                        or row.get("date")
                    )
                    add(
                        "social_posthoc",
                        line,
                        next(
                            (v for k, v in row.items() if "text" in k.lower()), "推文"
                        ),
                        canonical(row),
                        day,
                        row,
                        "C_posthoc_collection",
                    )
        flush()
        lab.execute(
            "UPDATE sources SET status=?,count=?,detail=? WHERE dataset_id=? AND path=?",
            (
                "ready",
                count,
                canonical({"unknown_date": invalid, "semantic_model_input": False}),
                ds,
                name,
            ),
        )
    except BaseException as exc:
        lab.execute(
            "UPDATE sources SET status=?,detail=? WHERE dataset_id=? AND path=?",
            ("failed", canonical({"error": str(exc)}), ds, name),
        )
        raise
