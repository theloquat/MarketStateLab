import json
import numpy as np
import pytest
from fastapi.testclient import TestClient
from marketlab.modeling import fit_candidate, filtered_inference, score_frame
from marketlab.models import load
from marketlab.features import panel
from marketlab.contracts import ReplayRequest
from marketlab.replay import create_run, advance
from marketlab.evaluation import warning_metrics
from marketlab.security import issue, verify, password_hash, password_ok
from marketlab.api import create_app

# Reuse the explicit synthetic fixture, never the actual user workspace.
from test_contest import official, lab  # noqa: F401, F811


def test_hmm_filtering_is_prefix_only():
    a = {
        "method": "hmm",
        "columns": ["x"],
        "mean": [0.0],
        "scale": [1.0],
        "means": [[-1.0], [1.0]],
        "covariances": [[1.0], [1.0]],
        "weights": [0.5, 0.5],
        "transition": [[0.95, 0.05], [0.05, 0.95]],
        "names": {"0": "低压力", "1": "高压力"},
        "risk_axis": [1.0],
    }
    first = filtered_inference(a, [-1])
    second = filtered_inference(a, [-1], first["filtering_state"])
    assert second["probabilities"]["低压力"] > first["probabilities"]["低压力"]
    old = first.copy()
    filtered_inference(a, [20], second["filtering_state"])
    assert first == old
    assert sum(second["probabilities"].values()) == pytest.approx(1)
    with pytest.raises(ValueError):
        filtered_inference(a, [np.nan])
    with pytest.raises(ValueError):
        filtered_inference(a, [1], [2.0, -1.0])


def test_hmm_artifact_pca_and_resume(lab):  # noqa: F811
    ds = lab.dataset()["id"]
    c = fit_candidate(lab, ds, "2020-09-30", "hmm", 3, "pca95")
    _, a = load(lab, c["id"])
    assert 1 <= c["latent_dimensions"] <= 16
    assert np.array(a["transition"]).sum(axis=1) == pytest.approx([1, 1, 1])
    r = create_run(
        lab, ReplayRequest(model_id=c["id"], start="2020-10-01", end="2021-02-20")
    )
    advance(lab, r, 5)
    advance(lab, r, 5000)
    f, cols, _ = panel(lab, ds)
    scored = {x["day"]: x for x in score_frame(a, f, cols)}
    for row in lab.rows("SELECT day,payload FROM results WHERE run_id=?", (r,)):
        assert json.loads(row["payload"])["filtering_state"] == pytest.approx(
            scored[row["day"]]["filtering_state"]
        )
    # Training never uses later observations to choose scaling or PCA.
    for s in lab.series():
        if s["status"] == "ready":
            import pandas as pd

            p = lab.root / s["path"]
            df = pd.read_parquet(p)
            df.loc[df.available_date > "2020-09-30", "value"] *= 2
            df.to_parquet(p, index=False)
    assert fit_candidate(lab, ds, "2020-09-30", "hmm", 3, "pca95")["id"] == c["id"]


def test_authentication_tampering_expiry_and_password():
    secret = "test-only-cryptographic-secret-32"
    token = issue("viewer", "viewer", secret)
    assert verify(token, secret)["role"] == "viewer"
    with pytest.raises(Exception):
        verify(token + "x", secret)
    with pytest.raises(Exception):
        verify(token, secret, now=10**12)
    hashed = password_hash("correct")
    assert password_ok("correct", hashed)
    assert not password_ok("wrong", hashed)


def test_graphql_executes_real_catalog_and_limits(lab):  # noqa: F811
    with TestClient(create_app(lab)) as c:
        response = c.post("/graphql", json={"query": "{ dashboard }"})
        assert response.status_code == 200
        assert (
            response.json()["data"]["dashboard"]["datasets"][0]["observation_count"]
            == 3360
        )
        bad = c.post("/graphql", json={"query": "{ dashboard " * 1000 + "}" * 1000})
        assert bad.status_code in (200, 400)
        assert bad.json().get("errors")


def test_warning_metrics_false_positives_and_cooldown():
    r = warning_metrics([0.9, 0.9, 0.1, 0.9], [True, False, True, False], cooldown=2)
    assert r["precision"] == pytest.approx(1 / 3)
    assert r["recall"] == 0.5
    assert r["false_positive_days_per_100"] == 50
    assert r["cooldown_alerts"] == 2
