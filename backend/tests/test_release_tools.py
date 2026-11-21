import json
import pytest
from fastapi.testclient import TestClient
from marketlab.api import create_app
from marketlab.agent import save_config, public_config, plan
from marketlab.evaluation import warning_metrics
from marketlab.security import issue
from marketlab.access_setup import initialize_access
from test_contest import official, lab  # noqa: F401


def test_llm_budget_and_output_do_not_execute_tools(lab, monkeypatch):  # noqa: F811
    save_config(
        lab,
        {
            "base_url": "https://test.invalid/v1",
            "model": "fixture",
            "api_key": "fixture-secret",
            "max_calls_per_day": 1,
            "token_parameter": "max_tokens",
            "token_budget_per_day": 16000,
        },
    )
    assert "api_key" not in public_config(lab)
    sent = []

    class Client:
        def __init__(self, **kwargs):
            assert kwargs["follow_redirects"] is False

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def post(self, url, **kwargs):
            sent.append(url)

            class Response:
                status_code = 200

                def json(self):
                    return {
                        "usage": {"total_tokens": 40},
                        "choices": [
                            {
                                "message": {
                                    "content": json.dumps(
                                        {
                                            "tool": "quality_audit",
                                            "hypothesis": "检查数据质量及来源可得性，发现缺失则不能直接建模。",
                                            "day": "2020-03-16",
                                            "model_ids": [],
                                        }
                                    )
                                }
                            }
                        ],
                    }

            return Response()

    monkeypatch.setattr("marketlab.agent.httpx.Client", Client)
    result = plan(lab, "核对官方数据质量", "fixture")
    assert len(sent) == 1
    assert (
        lab.rows("SELECT status FROM research WHERE id=?", (result["research_id"],))[0][
            "status"
        ]
        == "proposed"
    )
    with pytest.raises(ValueError, match="预算"):
        plan(lab, "再次请求", "fixture")
    assert len(sent) == 1
    with pytest.raises(ValueError, match="重新填写"):
        save_config(lab, {"base_url": "https://other.invalid"})


def test_failed_llm_attempt_remains_budgeted(lab, monkeypatch):  # noqa: F811
    save_config(
        lab,
        {
            "base_url": "https://test.invalid",
            "model": "fixture",
            "api_key": "do-not-leak",
            "max_calls_per_day": 1,
            "token_budget_per_day": 16000,
        },
    )

    class Broken:
        def __init__(self, **kwargs):
            raise RuntimeError("do-not-leak")

    monkeypatch.setattr("marketlab.agent.httpx.Client", Broken)
    with pytest.raises(ValueError) as caught:
        plan(lab, "检查数据缺口", "fixture")
    assert "do-not-leak" not in str(caught.value)
    assert public_config(lab)["calls_today"] == 1
    assert public_config(lab)["history"][0]["status"] == "failed_or_uncertain"
    assert not lab.rows("SELECT * FROM research")


def test_authenticated_backend_roles_and_research_lifecycle(
    lab, monkeypatch
):  # noqa: F811
    initialize_access(lab.root)
    monkeypatch.setenv("CONTEST_AUTH_REQUIRED", "1")
    monkeypatch.setenv("CONTEST_JWT_SECRET", "test-secret-32-characters-at-least")
    monkeypatch.setenv("CONTEST_INTERNAL_TOKEN", "internal-fixture")
    viewer = issue("viewer", "viewer", "test-secret-32-characters-at-least")
    admin = issue("admin", "admin", "test-secret-32-characters-at-least")
    with TestClient(create_app(lab)) as c:
        assert c.get("/api/lab/summary").status_code == 403
        c.headers.update(
            {
                "X-Internal-Token": "internal-fixture",
                "Authorization": "Bearer " + viewer,
            }
        )
        assert c.get("/api/lab/summary").status_code == 200
        assert c.post("/api/lab/research", json={}).status_code == 403
        assert c.get("/api/lab/admin/audit").status_code == 403
        c.headers.update({"Authorization": "Bearer " + admin})
        response = c.post(
            "/api/lab/research",
            json={
                "tool": "quality_audit",
                "hypothesis": "检查数据质量是否足以继续建模",
            },
        )
        rid = response.json()["id"]
        assert c.post("/api/lab/research/" + rid + "/execute").status_code == 400
        assert (
            c.post(
                "/api/lab/research/" + rid + "/decision", json={"status": "approved"}
            ).status_code
            == 200
        )
        from marketlab.research import execute

        result = execute(lab, rid, "fixture")
        assert result["llm_calls"] == 0
        assert c.post("/api/lab/research/" + rid + "/execute").status_code == 400
        assert (
            c.post(
                "/api/lab/research/" + rid + "/decision", json={"status": "accepted"}
            ).status_code
            == 200
        )
        assert (
            c.post(
                "/api/lab/research/" + rid + "/decision", json={"status": "approved"}
            ).status_code
            == 400
        )


def test_pdf_rejects_fake_and_roundtrips_real(lab):  # noqa: F811
    from pypdf import PdfWriter
    import io

    writer = PdfWriter()
    writer.add_blank_page(width=200, height=200)
    file = io.BytesIO()
    writer.write(file)
    with TestClient(create_app(lab)) as c:
        assert (
            c.post(
                "/api/lab/pdf", files={"file": ("fake.pdf", b"not a PDF")}
            ).status_code
            == 400
        )
        result = c.post(
            "/api/lab/pdf",
            files={"file": ("real.pdf", file.getvalue(), "application/pdf")},
        )
        assert result.status_code == 201
        receipt = result.json()
        assert receipt["pages"] == 1 and receipt["publication_verified"] is False
        assert (
            len(c.get("/api/lab/pdf/" + receipt["id"] + "/pages").json()["items"]) == 1
        )
        assert (
            c.get("/api/lab/pdf/" + receipt["id"] + "/original").content
            == file.getvalue()
        )


def test_episode_recall_does_not_claim_alert_after_episode():
    result = warning_metrics([0, 0, 0, 1], [True, True, False, False], cooldown=0)
    assert result["episode_recall"] == 0
    with pytest.raises(ValueError):
        warning_metrics([float("nan")], [True])
    with pytest.raises(ValueError):
        warning_metrics([1, 0], [True])


def test_retention_gap_never_silently_resets():
    from marketlab.offsets import resume_offset

    assert resume_offset(0, 20, None, None) == 0
    assert resume_offset(10, 20, 14, 18) == 15
    assert resume_offset(10, 20, None, 12) == 12
    with pytest.raises(ValueError, match="保留期缺口"):
        resume_offset(10, 20, 4, 15)
    with pytest.raises(ValueError, match="历史已过期"):
        resume_offset(10, 20, None, None)
    with pytest.raises(ValueError):
        resume_offset(10, 20, 25, None)


def test_batch_transaction_commits_once_and_rolls_back_as_unit():
    from marketlab.connections import MysqlPool

    pool = MysqlPool(size=1)

    class Connection:
        open = True
        commits = 0
        rollbacks = 0
        closed = False

        def ping(self, reconnect=False):
            pass

        def rollback(self):
            self.rollbacks += 1

        def commit(self):
            self.commits += 1

        def close(self):
            self.closed = True
            self.open = False

        def cursor(self):
            return "fixture-cursor"

    c = Connection()
    pool.available.put(c)
    with pool.transaction():
        for _ in range(5):
            with pool.connection() as leased:
                assert leased.cursor() == "fixture-cursor"
                leased.commit()
        assert c.commits == 0
    assert c.commits == 1
    assert c.rollbacks == 1  # reset connection before returning to bounded pool
    with pytest.raises(ValueError):
        with pool.transaction():
            with pool.connection() as leased:
                leased.commit()
            raise ValueError("simulated failed domain operation")
    assert c.commits == 1 and c.closed
    assert getattr(pool.local, "connection", None) is None


def test_summary_does_not_ship_large_research_results(lab):  # noqa: F811
    lab.task(
        "fixture",
        "research_tool",
        "completed",
        {"id": "r", "result": {"large": "x" * 200000}},
    )
    with TestClient(create_app(lab)) as client:
        summary = client.get("/api/lab/summary").json()
        assert summary["tasks"][0]["detail"] == {"id": "r"}
        assert len(client.get("/api/lab/summary").content) < 50000
        assert (
            len(
                client.get("/api/lab/tasks/fixture").json()["detail"]["result"]["large"]
            )
            == 200000
        )
        assert client.get("/api/lab/tasks/missing").status_code == 404


def test_columnar_delivery_batches_facts_without_changing_ids():
    from marketlab.distributed import enqueue_ch, ch_facts

    class Cursor:
        def __init__(self):
            self.calls = []

        def execute(self, sql, params):
            self.calls.append((sql, params))

    facts = [
        {"run_id": "run", "id": str(i), "day": "2020-01-01", "payload": "{}"}
        for i in range(39)
    ]
    cursor = Cursor()
    enqueue_ch(cursor, "features", facts)
    assert len(cursor.calls) == 1
    key, table, payload = cursor.calls[0][1]
    assert table == "features" and key.startswith("features:batch:")
    assert ch_facts(payload) == facts
    assert ch_facts(json.dumps(facts[0])) == [facts[0]]
    enqueue_ch(cursor, "features", facts)
    assert cursor.calls[1][1][0] == key
    enqueue_ch(cursor, "features", [])
    assert len(cursor.calls) == 2


def test_guarded_poll_initializes_missing_offsets_and_preserves_gaps():
    from kafka.errors import NoOffsetForPartitionError
    from marketlab.offsets import guarded_poll

    class Consumer:
        calls = 0

        def poll(self, timeout_ms):
            self.calls += 1
            if self.calls == 1:
                raise NoOffsetForPartitionError()
            return {"partition": ["fixture"]}

        def assignment(self):
            return {"partition"}

    class Guard:
        def on_partitions_assigned(self, partitions):
            assert partitions == {"partition"}

    assert guarded_poll(Consumer(), Guard()) == {"partition": ["fixture"]}

    class Gap:
        def on_partitions_assigned(self, partitions):
            raise ValueError("保留期缺口")

    with pytest.raises(ValueError, match="保留期缺口"):
        guarded_poll(Consumer(), Gap())


def test_public_evidence_download_cannot_escape_curated_directory(
    lab, tmp_path, monkeypatch
):
    folder = tmp_path / "evidence"
    folder.mkdir()
    (folder / "accepted.json").write_text('{"passed":true}')
    (folder / "secret-local.json").write_text('{"private":true}')
    (folder / "linked.json").symlink_to(folder / "secret-local.json")
    monkeypatch.setenv("CONTEST_EVIDENCE_DIR", str(folder))
    with TestClient(create_app(lab)) as c:
        assert c.get("/api/lab/reports/accepted.json").json() == {"passed": True}
        assert c.get("/api/lab/reports/secret-local.json").status_code == 404
        assert c.get("/api/lab/reports/linked.json").status_code == 404
        assert c.get("/api/lab/reports/.env").status_code == 404
        assert c.get("/api/lab/reports/%2e%2e%2fsecret-local.json").status_code == 404
