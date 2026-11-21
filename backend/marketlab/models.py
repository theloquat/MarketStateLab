from __future__ import annotations

import json

import numpy as np
from sklearn.mixture import GaussianMixture
from sklearn.preprocessing import StandardScaler

from .contracts import FEATURE_VERSION, POLICY, canonical, identity, utcnow
from .features import panel


def train(lab, dataset_id, train_end="2022-12-31"):
    # Crucially: never load post-cutoff rows into scaling, selection or naming.
    frame, columns, metadata = panel(lab, dataset_id, until=train_end)
    if len(frame) < 100:
        raise ValueError("训练快照不足100个；请检查核心资产覆盖或训练截止日期")
    x = frame[columns].to_numpy(dtype=float)
    scaler = StandardScaler().fit(x)
    z = scaler.transform(x)
    gmm = GaussianMixture(
        n_components=3,
        covariance_type="diag",
        n_init=5,
        random_state=17,
        reg_covar=1e-5,
    ).fit(z)
    # Labels are statistical profiles, not directional forecasts. Naming is fitted on train only.
    risk_axis = np.array(
        [
            (
                -1
                if any(s in m["name"] for s in ("标普", "沪深", "纳斯达克", "日经"))
                else 0
            )
            if m["measure"] == "momentum20"
            else (1 if m["unit"] == "fraction" else 0)
            for m in metadata
        ],
        dtype=float,
    )
    risk_axis /= max(abs(risk_axis).sum(), 1)
    rank = np.argsort(gmm.means_ @ risk_axis).tolist()
    state_names = {
        str(rank[0]): "低压力",
        str(rank[1]): "中等压力",
        str(rank[2]): "高压力",
    }
    artifact = {
        "columns": columns,
        "mean": scaler.mean_.tolist(),
        "scale": scaler.scale_.tolist(),
        "means": gmm.means_.tolist(),
        "covariances": gmm.covariances_.tolist(),
        "weights": gmm.weights_.tolist(),
        "names": state_names,
        "risk_axis": risk_axis.tolist(),
        "feature_version": FEATURE_VERSION,
        "policy": POLICY,
    }
    model_id = identity(
        {"artifact": artifact, "dataset_id": dataset_id, "train_end": train_end}
    )[:24]
    # Pure JSON model artifact: no untrusted pickle execution.
    path = lab.root / "models" / (model_id + ".json")
    path.write_text(canonical(artifact), encoding="utf-8")
    card = {
        "id": model_id,
        "dataset_id": dataset_id,
        "train_end": train_end,
        "train_start": frame.day.iloc[0],
        "train_rows": len(frame),
        "created_at": utcnow(),
        "features": metadata,
        "artifact_hash": identity(artifact),
        "path": str(path.relative_to(lab.root)),
        "method": "3-component diagonal GMM; frozen train-only scaling and labels",
        "limitations": [
            "状态概率不是涨跌准确率",
            "训练区间展示为样本内",
            "价格连续合约含展期口径限制；收益率变化非债券回报",
            "本版未完成模型优劣和预警有效性评估",
        ],
        "converged": bool(gmm.converged_),
    }
    lab.execute("INSERT OR IGNORE INTO models VALUES(?,?)", (model_id, canonical(card)))
    return card


def load(lab, model_id):
    rows = lab.rows("SELECT card FROM models WHERE id=?", (model_id,))
    if not rows:
        raise ValueError("模型不存在")
    card = json.loads(rows[0]["card"])
    artifact = json.loads((lab.root / card["path"]).read_text())
    if identity(artifact) != card["artifact_hash"]:
        raise ValueError("模型文件哈希不一致")
    return card, artifact


def infer(artifact, values, previous=None):
    if artifact.get("inference") == "causal-filter-v1":
        from .modeling import filtered_inference

        return filtered_inference(artifact, values, previous)
    x = np.asarray(values, dtype=float)
    if not np.isfinite(x).all():
        raise ValueError("模型输入包含缺失或非有限值")
    z = (x - artifact["mean"]) / artifact["scale"]
    mu, var = np.array(artifact["means"]), np.array(artifact["covariances"])
    logs = np.log(artifact["weights"]) - 0.5 * np.sum(
        np.log(2 * np.pi * var) + (z - mu) ** 2 / var, axis=1
    )
    probs = np.exp(logs - logs.max())
    probs /= probs.sum()
    state = int(np.argmax(probs))
    score = float(z @ artifact["risk_axis"])
    # Explicit deterministic baseline, thresholds fixed before holdout.
    baseline = "高压力" if score > 0.75 else "低压力" if score < -0.75 else "中等压力"
    contributions = (z * np.array(artifact["risk_axis"])).tolist()
    return {
        "state": artifact["names"][str(state)],
        "probability": float(probs[state]),
        "probabilities": {
            artifact["names"][str(i)]: float(p) for i, p in enumerate(probs)
        },
        "rule_baseline": baseline,
        "risk_score": score,
        "contributions": contributions,
    }
