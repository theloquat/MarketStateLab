"""Synthetic fixtures only; verify causal warmup/restart/staleness across microbatches."""

import copy
import pytest
from marketlab.batching import transition
from marketlab.models import infer


@pytest.fixture
def setup():
    artifact = {
        "method": "hmm",
        "inference": "causal-filter-v1",
        "columns": ["asset:momentum20"],
        "mean": [0.0],
        "scale": [1.0],
        "means": [[-1.0], [1.0]],
        "covariances": [[1.0], [1.0]],
        "weights": [0.5, 0.5],
        "transition": [[0.9, 0.1], [0.1, 0.9]],
        "names": {"0": "低压力", "1": "高压力"},
        "risk_axis": [1.0],
    }
    spec = {
        "id": "fixture-run",
        "core_ids": ["asset"],
        "model_id": "fixture-model",
        "dataset_id": "synthetic",
        "start": "2020-01-03",
        "end": "2020-03-01",
    }
    return artifact, spec, {"train_end": "2020-01-04"}


def batch(day, value=1.0, series="asset", available=None):
    return {
        "stream_run_id": "fixture-run",
        "day": day,
        "features": [
            {
                "series_id": series,
                "momentum20": value,
                "vol20": 0.01,
                "available_date": available or day,
                "observation_date": day,
                "event_id": "fixture-" + day,
                "source_row": 1,
                "value": 100.0,
            }
        ],
    }


def test_causal_filter_warmup_and_restart_match_single_steps(setup):
    artifact, spec, card = setup
    latest = {}
    alpha = None
    expected_alpha = None
    for day, value in [
        ("2020-01-01", -1.0),
        ("2020-01-02", -0.7),
        ("2020-01-03", 1.0),
        ("2020-01-04", 1.2),
        ("2020-01-05", 2.0),
    ]:
        original = copy.deepcopy(latest)
        updated, next_alpha, result, eligible = transition(
            spec, card, artifact, latest, alpha, batch(day, value)
        )
        assert latest == original  # snapshots/previous chunks are never mutated
        expected = infer(artifact, [value], expected_alpha)
        expected_alpha = expected["filtering_state"]
        assert next_alpha == pytest.approx(expected_alpha)
        if day < spec["start"]:
            assert result is None
        else:
            assert result["probabilities"] == pytest.approx(expected["probabilities"])
            assert result["evaluation"] == (
                "in_sample" if day <= card["train_end"] else "frozen_forward_scoring"
            )
        assert eligible
        # Simulate checkpoint roundtrip between every update.
        import json

        latest = json.loads(json.dumps(updated))
        alpha = json.loads(json.dumps(next_alpha))


def test_stale_or_nonadvancing_core_does_not_advance_filter(setup):
    artifact, spec, card = setup
    latest, alpha, _, _ = transition(
        spec, card, artifact, {}, None, batch("2020-01-03")
    )
    _, same, result, eligible = transition(
        spec, card, artifact, latest, alpha, batch("2020-01-04", series="other")
    )
    assert same == alpha and result is None and not eligible
    _, same, result, eligible = transition(
        spec, card, artifact, latest, alpha, batch("2020-01-20", available="2020-01-03")
    )
    assert same == alpha and result is None and not eligible
    missing = batch("2020-01-05", value=None)
    _, same, result, eligible = transition(spec, card, artifact, latest, alpha, missing)
    assert same == alpha and result is None and not eligible


def test_wire_preserves_values_and_canonical_identity_and_rejects_nonfinite():
    import json
    from marketlab.payloads import wire
    from marketlab.contracts import identity

    value = {
        "中文": [1e-7, 1.25, -2.0, None, {"x": 3.141592653589793}],
        "epoch_ns": 1794135435000000000,
    }
    before = identity(value)
    decoded = json.loads(wire(value))
    assert decoded == value and identity(decoded) == before
    for bad in [float("nan"), float("inf"), -float("inf")]:
        with pytest.raises(ValueError):
            wire({"nested": [{"bad": bad}]})


def test_warning_cooldown_is_identical_across_microbatch_boundaries():
    from marketlab.batching import warning_payloads

    spec = {"id": "synthetic", "warning_threshold": 0.75, "cooldown_days": 5}
    rows = [
        {"day": f"2020-01-{i:02d}", "state": "高压力", "probability": 0.8}
        for i in range(1, 20)
    ]
    whole = warning_payloads(spec, rows)
    first = warning_payloads(spec, rows[:8])
    second = warning_payloads(spec, rows[8:], first[-1]["result"]["day"])
    assert whole == first + second
    assert [a["result"]["day"] for a in whole] == [
        "2020-01-01",
        "2020-01-07",
        "2020-01-13",
        "2020-01-19",
    ]
    assert (
        warning_payloads(
            spec, [{"day": "2020-01-01", "state": "高压力", "probability": 0.74}]
        )
        == []
    )


def test_replay_pacing_is_bounded_and_does_not_change_model_contract():
    from marketlab.contracts import ReplayRequest

    values = dict(model_id="a" * 24, start="2023-01-01", end="2023-03-01")
    assert ReplayRequest(**values).rate_per_second == 0
    assert ReplayRequest(**values, rate_per_second=1000).rate_per_second == 1000
    for rate in (-1, 1, 499, 1_000_001, True, 1000.5):
        with pytest.raises(ValueError):
            ReplayRequest(**values, rate_per_second=rate)
