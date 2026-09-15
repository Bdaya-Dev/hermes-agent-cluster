"""#912 fence: answered CARRIER ballots must carry the formal-ballot actuation.

The two ballot writers (measured 2026-09-15: 16 answered owner-ballot-*
carriers; 4 with NO formal actuation — boxes unticked, label never flipped,
registry line never written; 2 whose carrier text named a target no formal
ballot on the linked issue matches) share no data. The gateway relay carries
the QUESTION down and the ANSWER up into the carrier row (tasks.ballot via
POST /tasks/{id}/block + /answer, #894) — and the formal tier
(decision_create/decision_resolve in the bdaya-gitlab pack) records the
authoritative D14/D31... state on GitLab issues. An answer that never
reaches (2) is a lost owner decision.

Wiring under test (the carrier store is the one surface holding both sides):

  * POST /tasks/{id}/block ACCEPTS decision_ref ("group[/sub]/project#iid")
    + optional decision_id; malformed -> 422 at FILE time (the mislink class
    cannot be created silently); absent stays legal (needs-actuation covers
    it) — RED at base: the extra fields are dropped and no directive exists.
  * POST /tasks/{id}/answer returns formal_actuation and persists it on the
    ballot row: state 'formal-resolvable' when a ref is carried,
    'needs-actuation' when not — every answer consumer (relay retry, gateway
    read, brief renderer) sees the obligation. RED at base: no directive.
  * the answer-resume brief renders the formal-actuation instruction next to
    the owner's answer. RED at base: the section never appears.

BallotError stays importable-free at file top; every helper the fence calls
exists at base except the NEW surface, which each test imports INSIDE the
test body (absence fails as the defect, not as an import crash that would
mask the API legs).
"""
import pytest
from fastapi.testclient import TestClient

from hermes_cluster.app import create_app
from hermes_cluster.models import TaskStatus


@pytest.fixture
def client(tmp_path):
    app = create_app(cluster_id="t", node_id="main", node_role="main",
                     db_path=str(tmp_path / "wire.db"))
    with TestClient(app) as c:
        yield c


def _blocked(client, lane_key="owner-ballot-invora-backend-34", decision_ref=None,
             decision_id=None):
    r = client.post("/api/v1/tasks", json={"title": "decide", "lane_key": lane_key})
    assert r.status_code == 200
    tid = r.json()["id"]
    body = {"question": "Migrate to PG16 or stay?", "options": ["migrate", "stay"],
            "class": "technical"}
    if decision_ref is not None:
        body["decision_ref"] = decision_ref
    if decision_id is not None:
        body["decision_id"] = decision_id
    r = client.post(f"/api/v1/tasks/{tid}/block", json=body)
    assert r.status_code == 200, r.text
    return tid, r.json()["ballot"]


def _answered(client, tid, answer="migrate — Friday window", who="telegram-relay:921"):
    r = client.post(f"/api/v1/tasks/{tid}/answer",
                    json={"answer": answer, "answered_by": who})
    assert r.status_code == 200, r.text
    return r.json()


# --------------------------------------------------- block: strict ref intake

def test_block_stores_valid_decision_ref(client):
    tid, b = _blocked(client, decision_ref="invora/backend#34", decision_id="D14")
    assert b.get("decision_ref") == "invora/backend#34"
    assert b.get("decision_id") == "D14"


def test_block_rejects_malformed_decision_ref(client):
    r = client.post("/api/v1/tasks", json={"title": "decide", "lane_key": "x#y"})
    tid = r.json()["id"]
    r = client.post(f"/api/v1/tasks/{tid}/block",
                    json={"question": "q", "options": ["a"],
                          "decision_ref": "backend-34"})  # bare repo: the mislink shape
    assert r.status_code == 422
    assert "decision_ref" in r.text


def test_block_accepts_no_ref_as_open_carrier(client):
    _, b = _blocked(client)
    assert not b.get("decision_ref")


# --------------------------------------------------- answer: the directive

def test_answered_carrier_with_ref_is_formal_resolvable(client):
    tid, _ = _blocked(client, decision_ref="metaphor/bayader/bayader-devops#52",
                      decision_id="D31")
    out = _answered(client, tid)
    fa = out.get("formal_actuation") or {}
    # RED at base: no directive at all (the silent-drop defect restated).
    assert fa, "answered carrier must surface a formal_actuation directive"
    assert fa["state"] == "formal-resolvable", fa
    assert fa["formal_ref"] == {"project": "metaphor/bayader/bayader-devops",
                                "issue_iid": 52,
                                "raw": "metaphor/bayader/bayader-devops#52"}
    assert fa["decision_id"] == "D31"
    assert "migrate" in fa["answer"] and fa["answered_by"] == "telegram-relay:921"
    # and it rides the stored row, not only the response
    ball = client.get(f"/api/v1/tasks/{tid}").json()["ballot"]
    assert ball["formal"]["state"] == "formal-resolvable"


def test_answered_carrier_without_ref_demands_actuation(client):
    tid, _ = _blocked(client)  # historical shape: no formal address
    out = _answered(client, tid)
    fa = out.get("formal_actuation") or {}
    assert fa.get("state") == "needs-actuation"
    assert fa["formal_ref"] is None
    # the directive is loud: it carries the answer itself so the actuation
    # lane never has to re-ask the owner
    assert fa["answer"] and fa["question"]


def test_answer_still_refuses_double_answer(client):
    tid, _ = _blocked(client)
    _answered(client, tid)
    r = client.post(f"/api/v1/tasks/{tid}/answer",
                    json={"answer": "again", "answered_by": "x"})
    assert r.status_code == 409


# ------------------------------------------------- executor: brief rendering

def _brief_with_ballot(tmp_path, ballot):
    from hermes_cluster.core.agent_executor import AgentExecutor, AgentExecutorConfig
    ex = AgentExecutor(
        config=AgentExecutorConfig(enabled=False, poll_interval=60),
        node_id="n", cluster_endpoint="http://127.0.0.1:1", peer_token="")
    p = ex._write_brief("t9", "do work", "", lane_key="L#main", role="author",
                        deliverable_path=str(tmp_path / "r.md"), ballot=ballot)
    return p.read_text()


def test_resumed_brief_carries_formal_actuation_leg(tmp_path):
    from hermes_cluster.core.ballot import build_ballot
    from hermes_cluster.core.ballot_wiring import attach_formal
    b = build_ballot("Migrate?", ["migrate", "stay"], cls="technical",
                     decision_ref="invora/backend#34", decision_id="D14")
    b["answer"] = "migrate"
    b["answered_by"] = "telegram"
    b["answered_at"] = "now"
    attach_formal(b, task_id="t9")
    text = _brief_with_ballot(tmp_path, b)
    assert "Owner's answer" in text  # 894 leg intact
    assert "formal ballot" in text.lower()
    assert "invora/backend#34" in text and "D14" in text
    assert "decision_resolve" in text


def test_resumed_brief_needs_actuation_leg(tmp_path):
    from hermes_cluster.core.ballot import build_ballot
    from hermes_cluster.core.ballot_wiring import attach_formal
    b = build_ballot("Migrate?", ["a", "b"])
    b["answer"] = "a"
    attach_formal(b, task_id="t9")
    text = _brief_with_ballot(tmp_path, b)
    assert "DO NOT RE-ASK" in text.upper()
    assert "needs-actuation" in text


# ------------------------------------------------------- pure-unit pins

def test_decision_id_shape_validated():
    from hermes_cluster.core.ballot_wiring import parse_decision_ref, validate_decision_id
    assert parse_decision_ref("a/b#12") == {"project": "a/b", "issue_iid": 12,
                                            "raw": "a/b#12"}
    assert parse_decision_ref("metaphor/bayader/bayader-devops#52")["project"] \
        == "metaphor/bayader/bayader-devops"
    assert parse_decision_ref(None) is None and parse_decision_ref("") is None
    with pytest.raises(ValueError):
        parse_decision_ref("34")
    with pytest.raises(ValueError):
        parse_decision_ref("a/b#x")
    with pytest.raises(ValueError):
        parse_decision_ref("a/b#12 extra")
    assert validate_decision_id("D14") == "D14"
    with pytest.raises(ValueError):
        validate_decision_id("D 14")


def test_build_ballot_strict_on_ref():
    from hermes_cluster.core.ballot import build_ballot, BallotError
    b = build_ballot("q", ["a"], decision_ref="p/x#7", decision_id="D2")
    assert b["decision_ref"] == "p/x#7" and b["decision_id"] == "D2"
    with pytest.raises(BallotError):
        build_ballot("q", ["a"], decision_ref="not-a-ref")
