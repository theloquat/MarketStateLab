import io
import json
import zipfile
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from marketlab.contracts import (
    Observation,
    ReplayRequest,
    available_on,
)
from marketlab.storage import Lab
from marketlab.ingest import import_archive, parse_day
from marketlab.features import CORE_NAMES, per_series, panel
from marketlab.models import train, infer, load
from marketlab.replay import create_run, advance
from marketlab.api import create_app


@pytest.fixture
def official(tmp_path):
    """Explicit synthetic fixture, never used as app data or claimed as real markets."""
    inner = io.BytesIO()
    with zipfile.ZipFile(inner, "w") as z:
        catalog = ["大类,名称,Wind代码,字段,交易日历,起始,结束,条数,状态,备注"]
        rng = np.random.default_rng(11)
        for i, name in enumerate(CORE_NAMES):
            kind = "yield" if "国债" in name else "close"
            catalog.append(
                f"测试,{name},TEST{i},{kind},fixture,,,,ok,SYNTHETIC TEST ONLY"
            )
            v = 4.0 if kind == "yield" else 100.0
            rows = [f"date,{kind}"]
            for j in range(420):
                v += (
                    float(rng.normal(0, 0.02))
                    if kind == "yield"
                    else v * float(rng.normal(0.0002, 0.01))
                )
                rows.append(f"{date(2020, 1, 1) + timedelta(days=j)},{v}")
            z.writestr(f"fixture/目标资产价格变量/{name}.csv", "\n".join(rows))
        catalog.append("测试,明确缺失,ABSENT,close,fixture,,,,missing,fixture missing")
        z.writestr("fixture/目标资产价格变量/代码对照.csv", "\n".join(catalog))
    archive = tmp_path / "fixture.zip"
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr("directory/", b"")
        z.writestr("directory/inner.zip", inner.getvalue())
    return archive


@pytest.fixture
def lab(tmp_path, official):
    workspace = Lab(tmp_path / "lab")
    import_archive(workspace, official, False)
    return workspace


def test_dates_and_contract_units():
    assert parse_day("2026/7/7 12:13:14") == "2026-07-07"
    assert parse_day("bad") is None
    assert available_on("2024-02-28") == "2024-03-01"
    with pytest.raises(ValueError):
        Observation(
            dataset_id="x",
            series_id="s",
            observation_date="2020-01-01",
            value=float("nan"),
            quote_kind="close",
            unit="x",
            source_file="f",
            source_hash="h",
            source_row=2,
        )
    with pytest.raises(ValueError):
        ReplayRequest(model_id="abcdef", start="2021-01-02", end="2021-01-01")


def test_import_idempotent_and_isolated(lab, official, tmp_path):
    first = lab.dataset()
    again = import_archive(lab, official, False)
    assert first["id"] == again["id"]
    assert again["observation_count"] == 8 * 420
    assert len(lab.series()) == 9
    assert lab.series()[0]["status"] in ("ready", "missing")
    (tmp_path / "incompatible-runtime" / "private").mkdir(parents=True)
    with pytest.raises(ValueError, match="incompatible private configuration"):
        Lab(tmp_path / "incompatible-runtime")
    for s in lab.series():
        if s["status"] == "ready":
            df = pd.read_parquet(lab.root / s["path"])
            assert df.source_row.min() == 2
            assert df.event_id.nunique() == 420


def test_yield_and_negative_price_semantics():
    def df(values, kind):
        return pd.DataFrame(
            {
                "value": values,
                "quote_kind": kind,
                "observation_date": [
                    str(date(2020, 1, 1) + timedelta(days=i))
                    for i in range(len(values))
                ],
            }
        )

    assert per_series(df([4.25, 4.30], "yield")).change1.iloc[-1] == pytest.approx(5)
    assert np.isnan(per_series(df([20, -13.1, 10], "close")).change1.iloc[-1])
    assert per_series(df(list(range(100, 121)), "close")).vol20.notna().sum() == 1


def test_frozen_training_and_append_future_invariant(lab):
    ds = lab.dataset()["id"]
    card = train(lab, ds, "2020-09-30")
    _, artifact = load(lab, card["id"])
    original, cols, _ = panel(lab, ds)
    before = infer(artifact, original[cols].iloc[200].to_numpy())
    # Alter all future values: prior train artifact and historic feature snapshots must remain identical.
    for s in lab.series():
        if s["status"] != "ready":
            continue
        path = lab.root / s["path"]
        df = pd.read_parquet(path)
        df.loc[df.available_date > "2020-09-30", "value"] *= 3
        df.to_parquet(path, index=False)
    again = train(lab, ds, "2020-09-30")
    assert again["id"] == card["id"]
    new, _, _ = panel(lab, ds)
    pd.testing.assert_frame_equal(
        original.loc[original.day <= "2020-09-30"].reset_index(drop=True),
        new.loc[new.day <= "2020-09-30"].reset_index(drop=True),
    )
    assert infer(artifact, new[cols].iloc[200].to_numpy()) == before


def test_resume_results_and_alerts_equal_uninterrupted(lab):
    card = train(lab, lab.dataset()["id"], "2020-09-30")
    request = ReplayRequest(
        model_id=card["id"],
        start="2020-10-01",
        end="2021-02-20",
        warning_threshold=0.5,
        cooldown_days=0,
    )
    rid = create_run(lab, request)
    advance(lab, rid, 13)
    # Reopen workspace to simulate process restart.
    recovered = Lab(lab.root)
    advance(recovered, rid, 5000)
    completed = lab.rows(
        "SELECT payload FROM results WHERE run_id=? ORDER BY day", (rid,)
    )
    alerts = lab.rows("SELECT payload FROM alerts WHERE run_id=? ORDER BY day", (rid,))
    advance(lab, rid, 5000)
    assert (
        lab.rows("SELECT payload FROM results WHERE run_id=? ORDER BY day", (rid,))
        == completed
    )
    lab.execute("DELETE FROM results WHERE run_id=?", (rid,))
    lab.execute("DELETE FROM alerts WHERE run_id=?", (rid,))
    lab.execute("UPDATE runs SET cursor=0,status=? WHERE id=?", ("paused", rid))
    advance(lab, rid, 5000)
    assert (
        lab.rows("SELECT payload FROM results WHERE run_id=? ORDER BY day", (rid,))
        == completed
    )
    assert (
        lab.rows("SELECT payload FROM alerts WHERE run_id=? ORDER BY day", (rid,))
        == alerts
    )
    assert all(
        json.loads(r["payload"])["evaluation"] == "frozen_forward_scoring"
        for r in completed
    )


def test_tampered_model_rejected(lab):
    card = train(lab, lab.dataset()["id"], "2020-09-30")
    (lab.root / card["path"]).write_text("{}")
    with pytest.raises(ValueError, match="哈希"):
        load(lab, card["id"])


def test_api_validation_evidence_and_origin(lab):
    with TestClient(create_app(lab)) as client:
        assert client.get("/health").json()["llm_calls"] == 0
        response = client.get("/api/lab/summary")
        assert response.status_code == 200
        sid = next(s["id"] for s in lab.series() if s["status"] == "ready")
        obs = client.get(f"/api/lab/series/{sid}/observations?limit=1").json()["items"][
            0
        ]
        evidence = client.get("/api/lab/evidence/" + obs["event_id"]).json()
        assert evidence["observation"]["source_row"] == 2
        assert (
            client.get(f"/api/lab/series/{sid}/observations?limit=99999").status_code
            == 400
        )
        assert (
            client.post(
                "/api/lab/train", json={}, headers={"Origin": "https://evil.example"}
            ).status_code
            == 403
        )
        assert (
            client.post("/api/lab/runs/no/advance", json={"steps": 1}).status_code
            == 404
        )
        assert (
            client.post("/api/lab/train", json={"train_end": "2020-99-99"}).status_code
            == 400
        )
        assert client.get("/api/lab/documents?q=%27%20OR%201=1--").status_code == 200


def test_serves_independent_frontend_without_rewriting_html(lab, tmp_path, monkeypatch):
    from marketlab import api

    dist = tmp_path / "frontend" / "dist"
    dist.mkdir(parents=True)
    index = dist / "index.html"
    html = "<title>市场状态实验室</title>"
    index.write_text(html)
    monkeypatch.setattr(api, "ROOT", tmp_path)
    with TestClient(api.create_app(lab)) as client:
        assert client.get("/lab").text == html
        redirect = client.get("/", follow_redirects=False)
        assert redirect.status_code == 307
        assert redirect.headers["location"] == "/lab"
    assert index.read_text() == html


def test_savepoint_override_only_changes_after_verified_completion(
    tmp_path, monkeypatch
):
    import importlib.util
    from pathlib import Path

    path = Path(__file__).resolve().parents[2] / "scripts" / "contest_checkpoint.py"
    spec = importlib.util.spec_from_file_location("contest_checkpoint_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "ROOT", tmp_path)
    (tmp_path / ".quantos-contest").mkdir()
    target = tmp_path / ".quantos-contest" / "restore.json"
    target.write_text("previous good location")

    def fake_api(route, body=None):
        if route == "/jobs/overview":
            return {"jobs": [{"state": "RUNNING", "jid": "job"}]}
        if body:
            assert body["target-directory"] == "s3://quantos/savepoints"
            return {"request-id": "trigger"}
        return {"status": {"id": "COMPLETED"}, "operation": {"failure-cause": "failed"}}

    monkeypatch.setattr(module, "api", fake_api)
    with pytest.raises(RuntimeError, match="保存点失败"):
        module.capture()
    assert target.read_text() == "previous good location"

    def success(route, body=None):
        if route.endswith("/trigger"):
            return {
                "status": {"id": "COMPLETED"},
                "operation": {"location": "s3://quantos/savepoints/verified"},
            }
        return fake_api(route, body)

    monkeypatch.setattr(module, "api", success)
    module.capture()
    assert (
        json.loads(target.read_text())["services"]["jobmanager"]["command"][-1]
        == "s3://quantos/savepoints/verified"
    )
