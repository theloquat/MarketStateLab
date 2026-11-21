"""Frozen statistical models. HMM filtering uses only the prefix available at t."""

from __future__ import annotations
import numpy as np
from scipy.special import logsumexp
from sklearn.decomposition import PCA
from sklearn.mixture import GaussianMixture
from sklearn.preprocessing import StandardScaler
from .contracts import FEATURE_VERSION, POLICY, identity, canonical, utcnow
from .features import panel


def fit_candidate(
    lab,
    dataset_id,
    train_end="2022-12-31",
    method="hmm",
    components=3,
    reduction="pca95",
):
    if (
        method not in ("gmm", "hmm")
        or components not in (2, 3, 4)
        or reduction not in ("none", "pca95")
    ):
        raise ValueError("invalid model specification")
    frame, columns, metadata = panel(lab, dataset_id, until=train_end)
    if len(frame) < 100:
        raise ValueError("训练快照不足100个")
    x = frame[columns].to_numpy(float)
    scaler = StandardScaler().fit(x)
    z = scaler.transform(x)
    projection = None
    if reduction == "pca95":
        pca = PCA(n_components=0.95, svd_solver="full").fit(z)
        working = pca.transform(z)
        projection = {
            "components": pca.components_.tolist(),
            "center": pca.mean_.tolist(),
            "variance": pca.explained_variance_ratio_.tolist(),
        }
    else:
        working = z
    if method == "hmm":
        from hmmlearn.hmm import GaussianHMM

        estimator = GaussianHMM(
            n_components=components,
            covariance_type="diag",
            n_iter=200,
            tol=1e-4,
            random_state=17,
            min_covar=1e-3,
        ).fit(working)
        means = estimator.means_
        cov = estimator.covars_
        if cov.ndim == 3:
            cov = np.diagonal(cov, axis1=1, axis2=2)
        weights = estimator.startprob_
        trans = estimator.transmat_
        fit_status = {
            "converged": bool(estimator.monitor_.converged),
            "iterations": estimator.monitor_.iter,
            "train_log_likelihood": float(estimator.score(working)),
        }
    else:
        estimator = GaussianMixture(
            n_components=components,
            covariance_type="diag",
            n_init=5,
            random_state=17,
            reg_covar=1e-5,
        ).fit(working)
        means, cov, weights = (
            estimator.means_,
            estimator.covariances_,
            estimator.weights_,
        )
        trans = None
        fit_status = {
            "converged": bool(estimator.converged_),
            "iterations": int(estimator.n_iter_),
            "train_log_likelihood": float(estimator.score(working) * len(working)),
        }
    if not np.isfinite(cov).all() or np.any(cov <= 0):
        raise ValueError("invalid fitted covariance")
    axis = np.array(
        [
            (
                -1
                if any(n in m["name"] for n in ("标普", "沪深", "纳斯达克", "日经"))
                else 0
            )
            if m["measure"] == "momentum20"
            else (1 if m["unit"] == "fraction" else 0)
            for m in metadata
        ],
        float,
    )
    axis /= max(abs(axis).sum(), 1)
    original_means = (
        means @ np.array(projection["components"]) + projection["center"]
        if projection
        else means
    )
    rank = np.argsort(original_means @ axis)
    labels = {
        2: ["低压力", "高压力"],
        3: ["低压力", "中等压力", "高压力"],
        4: ["低压力", "中等压力", "偏高压力", "高压力"],
    }[components]
    artifact = {
        "method": method,
        "columns": columns,
        "mean": scaler.mean_.tolist(),
        "scale": scaler.scale_.tolist(),
        "projection": projection,
        "means": means.tolist(),
        "covariances": cov.tolist(),
        "weights": weights.tolist(),
        "transition": None if trans is None else trans.tolist(),
        "names": {str(int(k)): labels[i] for i, k in enumerate(rank)},
        "risk_axis": axis.tolist(),
        "feature_version": FEATURE_VERSION,
        "policy": POLICY,
        "inference": "causal-filter-v1",
    }
    model_id = identity(
        {"artifact": artifact, "dataset_id": dataset_id, "train_end": train_end}
    )[:24]
    path = lab.root / "models" / (model_id + ".json")
    path.write_text(canonical(artifact))
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
        "method": method,
        "components": components,
        "reduction": reduction,
        "latent_dimensions": working.shape[1],
        "fit": fit_status,
        "limitations": [
            "历史可得性采用披露的重建假设，非PIT认证",
            "状态概率不是涨跌准确率",
            "HMM使用单向过滤，禁止测试区间后向平滑",
            "效果需独立时间留出比较",
        ],
        "converged": fit_status["converged"],
    }
    lab.execute("INSERT OR IGNORE INTO models VALUES(?,?)", (model_id, canonical(card)))
    return card


def filtered_inference(artifact, values, previous=None):
    x = np.asarray(values, float)
    if x.shape != (len(artifact["columns"]),) or not np.isfinite(x).all():
        raise ValueError("invalid model input")
    z = (x - np.array(artifact["mean"])) / np.array(artifact["scale"])
    projection = artifact.get("projection")
    working = (
        (z - np.array(projection["center"])) @ np.array(projection["components"]).T
        if projection
        else z
    )
    mu, var = np.array(artifact["means"]), np.array(artifact["covariances"])
    emission = -0.5 * np.sum(
        np.log(2 * np.pi * var) + (working - mu) ** 2 / var, axis=1
    )
    if artifact.get("method") == "hmm":
        prior = (
            np.asarray(artifact["weights"])
            if previous is None
            else np.asarray(previous) @ np.asarray(artifact["transition"])
        )
        if (
            prior.shape != (len(mu),)
            or not np.isfinite(prior).all()
            or np.any(prior < 0)
            or not np.isclose(prior.sum(), 1)
        ):
            raise ValueError("invalid filtering state")
    else:
        prior = np.array(artifact["weights"])
    log_joint = np.log(np.maximum(prior, 1e-300)) + emission
    log_evidence = logsumexp(log_joint)
    posterior = np.exp(log_joint - log_evidence)
    k = int(posterior.argmax())
    score = float(z @ artifact["risk_axis"])
    result = {
        "state": artifact["names"][str(k)],
        "probability": float(posterior[k]),
        "probabilities": {
            artifact["names"][str(i)]: float(v) for i, v in enumerate(posterior)
        },
        "rule_baseline": "高压力"
        if score > 0.75
        else "低压力"
        if score < -0.75
        else "中等压力",
        "risk_score": score,
        "contributions": (z * np.array(artifact["risk_axis"])).tolist(),
        "log_evidence": float(log_evidence),
    }
    if artifact.get("method") == "hmm":
        result["filtering_state"] = posterior.tolist()
    return result


def score_frame(artifact, frame, columns):
    previous = None
    results = []
    # pandas attrs hold the evidence graph; never copy it once per row.
    values = frame[columns].to_numpy(float)
    for day, vector in zip(frame.day.tolist(), values):
        result = filtered_inference(artifact, vector, previous)
        previous = result.get("filtering_state")
        results.append({"day": day, **result})
    return results
