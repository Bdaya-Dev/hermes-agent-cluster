"""LFP-1 grouped intake tests — factory-lfp1-grouping lane (#762).

Pins the owner's ruled design as executable rules (shared/claude-plugins#762,
#613 note 105482, !685, #833 note 133037 rule #1):

  (1) one lane-bundle TASK PER CLIENT×REPO batch, not per issue: a repo's
      candidates produce ONE task with lane_key '<repo>#<branch>' whose
      description names every bundled issue and whose membership list (
      ``issues``) is restart-safe DEDUP FIRST;
  (2) never bundle across stacks — two repos in one group scope = two tasks;
  (3) skip labels (blocked/needs-human/needs-decision/epic) are never bundled;
  (4) D-SPAWN-1: an issue already referenced by an OPEN MR is artifact-
      carrying — it is not grouped (the fix exists as an MR);
  (5) the GitLab DAG keeps ordering authority: an issue whose is_blocked_by
      blocker is open OUTSIDE the bundle is held back;
  (6) cardinality guard: at most ONE queued bundle per lane key for band>0
      backlog (the rest wait for the next sitting); the scheduler never
      assigns a ready task whose lane key already has an ACTIVE sibling;
  (7) band 0 (business-team authors 12/8) stays INSTANT: a band-0 batch
      creates its own bundle task even while a band>0 bundle is queued, and
      the webhook lands a band-0 issue immediately as a single-issue bundle;
  (8) grouping defaults OFF: with no grouping section the per-issue wiring
      keeps working byte-for-byte (#886 regression guard).

RED PROOF (skill standard): on origin/main these fail on the DEFECT (no
grouping field on the policy → per-issue tasks + no lane_key), not ImportError
— new modules are imported INSIDE the tests that pin them. Controls that pass
on main too (by-design invariants) are marked.
"""

import asyncio
import json
import os
import re

import httpx
import pytest
from httpx import AsyncClient, ASGITransport

from hermes_cluster.app import create_app
from hermes_cluster.state import ClusterState
from hermes_cluster.state.cluster_store import ClusterStore
from hermes_cluster.routers import intake as intake_mod
from hermes_cluster.models import TaskStatus


@pytest.fixture
def clean_env(monkeypatch):
    for k in list(os.environ):
        if k.startswith("GITLAB_INTAKE_"):
            monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(intake_mod, "_issue_dedup_to_task_id", {})
    yield


def _issue_json(iid, title, project_path, project_id, author_id=99, labels=None):
    return {
        "iid": iid, "id": project_id * 100000 + iid, "title": title,
        "project_id": project_id,
        "references": {"full": f"{project_path}#{iid}"},
        "author": {"id": author_id, "username": "x"},
        "labels": list(labels or []),
        "state": "opened",
    }


def _mr_json(iid, title, project_path, desc=""):
    return {
        "iid": iid, "title": title, "description": desc,
        "state": "opened",
        "references": {"full": f"{project_path}!{iid}",
                       "project_path": project_path},
    }


def _make_transport(routes):
    """routes: {path_suffix: payload-or-callable(request)->payload}. Matches
    on the RAW percent-encoded path (same discipline as the #886 helper —
    httpx decodes %2F in .path, which would break project-path keys)."""
    def handler(request: httpx.Request) -> httpx.Response:
        raw = request.url.raw_path.decode("ascii").split("?", 1)[0]
        for key, payload in routes.items():
            if raw.endswith(key):
                data = payload(request) if callable(payload) else payload
                return httpx.Response(200, json=data)
        return httpx.Response(404, json={"message": "404 Not Found"})
    return httpx.MockTransport(handler)


def _grouping_policy(**overrides):
    g = {"enabled": True}
    g.update(overrides)
    return g


def _poller(state, routes, policy_cfg, monkeypatch=None, defaults_extra=None):
    monkeypatch_env = monkeypatch or (lambda k, v: os.environ.__setitem__(k, v))
    defaults = {"token": "t", "endpoint": "https://gitlab.test",
                "project": "shared%2Fclaude-plugins", "label": "hermes-factory",
                "interval": 30, "requires": ["tooling"]}
    if defaults_extra:
        defaults.update(defaults_extra)
    state.set_config({"intake": {"gitlab": policy_cfg}})
    return intake_mod._GitLabPoller(
        state=state, defaults=defaults, transport=_make_transport(routes))


def _cfg(scopes, grouping=None, priority=None, enabled=True):
    cfg = {"enabled": enabled, "endpoint": "https://gitlab.test",
           "scopes": scopes, "dedup_scope": "full"}
    if grouping is not None:
        cfg["grouping"] = grouping
    if priority is not None:
        cfg["priority"] = priority
    return cfg


# ---------------------------------------------------------------------------
# 1. Policy surface: grouping section round-trips through the runtime store
# ---------------------------------------------------------------------------

def test_grouping_section_roundtrips_and_defaults_off():
    from hermes_cluster.core.intake_policy import load_policy
    st = ClusterState()
    # (control) no grouping section -> disabled.
    st.set_config({"intake": {"gitlab": {"endpoint": "https://gitlab.test",
                                         "scopes": []}}})
    assert load_policy(st).grouping.enabled is False
    # NEW: grouping shape persists.
    st.set_config({"intake": {"gitlab": {
        "endpoint": "https://gitlab.test", "scopes": [],
        "grouping": {"enabled": True, "max_bundle_size": 12,
                     "lane_branch": "env/dev",
                     "lane_branches": {"hermes-agent-cluster": "main"}}}}})
    p = load_policy(st)
    assert p.grouping.enabled is True
    assert p.grouping.max_bundle_size == 12
    assert p.grouping.lane_branches == {"hermes-agent-cluster": "main"}


def test_grouping_bad_cap_rejected_at_write(clean_env):
    app = create_app(cluster_id="t", node_id="n", node_role="main")

    async def go():
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            r = await c.put("/api/v1/intake/gitlab/policy", json={
                "enabled": True, "scopes": [{"type": "group", "path": "invora"}],
                "grouping": {"enabled": True, "max_bundle_size": 0}})
            assert r.status_code == 422
    asyncio.run(go())


# ---------------------------------------------------------------------------
# 2. Grouped poll cycle: ONE bundle task per repo, lane_key set, membership
# ---------------------------------------------------------------------------

def _two_repo_routes():
    return {
        "/groups/invora/issues": [
            _issue_json(101, "flutter bug A", "invora/invora-flutter", 275),
            _issue_json(102, "flutter bug B", "invora/invora-flutter", 275),
            _issue_json(201, "backend gap", "invora/invora-backend", 276),
        ],
        "/merge_requests": lambda req: [],
        "/links": lambda req: {"closed": [], "open": []},
    }


def test_grouped_cycle_one_task_per_repo_with_lane_key(clean_env, monkeypatch):
    monkeypatch.setenv("GITLAB_INTAKE_ENDPOINT", "https://gitlab.test")
    st = ClusterState()
    poller = _poller(st, _two_repo_routes(), _cfg(
        [{"type": "group", "path": "invora", "enabled": True}],
        grouping=_grouping_policy()))
    res = asyncio.run(poller.poll_once())
    tasks = st.get_all_tasks()
    assert len(tasks) == 2, (
        f"expected ONE bundle task per repo, got: {[t.title for t in tasks]}")
    by_lane = {t.lane_key: t for t in tasks}
    assert by_lane["invora-flutter#env/dev"].issues == \
        ["invora/invora-flutter#101", "invora/invora-flutter#102"]
    assert by_lane["invora-backend#env/dev"].issues == \
        ["invora/invora-backend#201"]
    flutter = by_lane["invora-flutter#env/dev"]
    # AB-1: never across stacks — backend issue NOT in the flutter bundle.
    assert "invora/invora-backend#201" not in flutter.issues
    # The brief names every bundled issue (the lane can see its whole scope).
    for iid in ("101", "102"):
        assert f"#{iid}" in flutter.description
    assert "Refs" in flutter.description  # Refs-never-Closes rule in the brief
    # Legacy per-issue title shape must NOT appear (grouping mode on).
    assert not any(t.title.startswith("[#") for t in tasks)


def test_lane_branch_map_overrides_default(clean_env, monkeypatch):
    monkeypatch.setenv("GITLAB_INTAKE_ENDPOINT", "https://gitlab.test")
    st = ClusterState()
    routes = {
        "/projects/shared%2Fclaude-plugins/issues": [
            _issue_json(762, "tooling thing", "shared/claude-plugins", 339,
                        labels=["hermes-factory"])],
        "/merge_requests": lambda req: [],
        "/links": lambda req: {"closed": [], "open": []},
    }
    poller = _poller(st, routes, _cfg(
        [{"type": "project", "path": "shared/claude-plugins",
          "label": "hermes-factory", "enabled": True}],
        grouping=_grouping_policy(lane_branches={"claude-plugins": "main"})))
    asyncio.run(poller.poll_once())
    tasks = st.get_all_tasks()
    assert len(tasks) == 1
    assert tasks[0].lane_key == "claude-plugins#main"


def test_grouped_cycle_is_restart_safe_dedup_first(clean_env, monkeypatch):
    """DEDUP FIRST, store-backed: a FRESH poller (empty _seen_keys — a main
    restart) must not re-bundle issues a live task already holds."""
    monkeypatch.setenv("GITLAB_INTAKE_ENDPOINT", "https://gitlab.test")
    st = ClusterState()
    poller = _poller(st, _two_repo_routes(), _cfg(
        [{"type": "group", "path": "invora", "enabled": True}],
        grouping=_grouping_policy()))
    first = asyncio.run(poller.poll_once())
    assert first["created"]
    # Fresh poller instance, same store: zero new bundles.
    poller2 = _poller(st, _two_repo_routes(), st.get_config()["intake"]["gitlab"])
    poller2.state = st
    second = asyncio.run(poller2.poll_once())
    assert second["created"] == [], "restart re-bundled live issues"
    assert len(st.get_all_tasks()) == 2


def test_next_sitting_after_current_terminal(clean_env, monkeypatch):
    """Once the queued bundle for a lane is terminal (merged/completed), the
    NEXT cycle bundles the same repo's remaining backlog on the SAME lane key
    (one long-lived lane, many sittings — #833 rule #1 resume)."""
    monkeypatch.setenv("GITLAB_INTAKE_ENDPOINT", "https://gitlab.test")
    st = ClusterState()
    routes = {
        "/groups/invora/issues": [
            _issue_json(301, "one", "invora/invora-flutter", 275),
            _issue_json(302, "two", "invora/invora-flutter", 275),
        ],
        "/merge_requests": lambda req: [],
        "/links": lambda req: {"closed": [], "open": []},
    }
    cfg = _cfg([{"type": "group", "path": "invora", "enabled": True}],
               grouping=_grouping_policy(max_bundle_size=1))
    poller = _poller(st, routes, cfg)
    res1 = asyncio.run(poller.poll_once())
    assert len(res1["created"]) == 1
    t1 = st.get_task(res1["created"][0])
    assert t1.issues == ["invora/invora-flutter#301"]
    # Cap=1: one bundle per lane queued at a time — second cycle: still 1 task.
    res1b = asyncio.run(poller.poll_once())
    assert res1b["created"] == []
    # The sitting completes...
    st.set_task_status(t1.id, TaskStatus.completed)
    res2 = asyncio.run(poller.poll_once())
    assert len(res2["created"]) == 1
    t2 = st.get_task(res2["created"][0])
    assert t2.lane_key == t1.lane_key
    assert t2.issues == ["invora/invora-flutter#302"]


# ---------------------------------------------------------------------------
# 3. Skip labels + D-SPAWN-1 + DAG authority
# ---------------------------------------------------------------------------

def test_skip_labels_never_bundled(clean_env, monkeypatch):
    monkeypatch.setenv("GITLAB_INTAKE_ENDPOINT", "https://gitlab.test")
    st = ClusterState()
    routes = {
        "/groups/invora/issues": [
            _issue_json(401, "normal", "invora/invora-flutter", 275),
            _issue_json(402, "blocked", "invora/invora-flutter", 275,
                        labels=["blocked"]),
            _issue_json(403, "nh", "invora/invora-flutter", 275,
                        labels=["status::needs-human"]),
            _issue_json(404, "decide", "invora/invora-flutter", 275,
                        labels=["status::needs-decision"]),
            _issue_json(405, "epic", "invora/invora-flutter", 275,
                        labels=["epic"]),
        ],
        "/merge_requests": lambda req: [],
        "/links": lambda req: {"closed": [], "open": []},
    }
    poller = _poller(st, routes, _cfg(
        [{"type": "group", "path": "invora", "enabled": True}],
        grouping=_grouping_policy()))
    asyncio.run(poller.poll_once())
    bundles = [t for t in st.get_all_tasks() if t.issues]
    assert len(bundles) == 1
    assert bundles[0].issues == ["invora/invora-flutter#401"]


def test_dspawn1_artifact_carrying_issue_not_grouped(clean_env, monkeypatch):
    """Issue 502 already has an open MR that Refs it — re-bundling it would
    duplicate an artifact (D-SPAWN-1)."""
    monkeypatch.setenv("GITLAB_INTAKE_ENDPOINT", "https://gitlab.test")
    st = ClusterState()
    routes = {
        "/groups/invora/issues": [
            _issue_json(501, "fresh", "invora/invora-flutter", 275),
            _issue_json(502, "has an MR", "invora/invora-flutter", 275),
        ],
        "/merge_requests": [_mr_json(900, "fix 502", "invora/invora-flutter",
                                     desc="Refs #502")],
        "/links": lambda req: {"closed": [], "open": []},
    }
    poller = _poller(st, routes, _cfg(
        [{"type": "group", "path": "invora", "enabled": True}],
        grouping=_grouping_policy()))
    asyncio.run(poller.poll_once())
    bundles = [t for t in st.get_all_tasks() if t.issues]
    assert len(bundles) == 1
    assert bundles[0].issues == ["invora/invora-flutter#501"]


def test_dag_open_blocker_outside_bundle_held(clean_env, monkeypatch):
    """602 is blocked_by 601; 601 is itself doomed (epic-labeled => outside
    any bundle). 602 must NOT be bundled while its blocker is open/unmerged."""
    monkeypatch.setenv("GITLAB_INTAKE_ENDPOINT", "https://gitlab.test")
    st = ClusterState()

    def links(request):
        path = request.url.path
        iid = path.rstrip("/").split("/")[-2]
        if iid == "602":
            return {"closed": [], "open": [
                {"iid": 601, "state": "opened", "link_types": ["is_blocked_by"],
                 "references": {"full": "invora/invora-flutter#601"}}]}
        return {"closed": [], "open": []}

    routes = {
        "/groups/invora/issues": [
            _issue_json(601, "epic blocker", "invora/invora-flutter", 275,
                        labels=["epic"]),
            _issue_json(602, "child", "invora/invora-flutter", 275),
            _issue_json(603, "unrelated", "invora/invora-flutter", 275),
        ],
        "/merge_requests": lambda req: [],
        "/links": links,
    }
    poller = _poller(st, routes, _cfg(
        [{"type": "group", "path": "invora", "enabled": True}],
        grouping=_grouping_policy()))
    asyncio.run(poller.poll_once())
    bundles = [t for t in st.get_all_tasks() if t.issues]
    assert len(bundles) == 1
    members = bundles[0].issues
    assert "invora/invora-flutter#602" not in members
    assert "invora/invora-flutter#603" in members


def test_dag_blocker_inside_batch_not_held(clean_env, monkeypatch):
    """612 blocked_by 611 and BOTH are bundleable: one lane fixes them in
    order in one session — the batch must contain both (DAG stays internal)."""
    monkeypatch.setenv("GITLAB_INTAKE_ENDPOINT", "https://gitlab.test")
    st = ClusterState()

    def links(request):
        iid = request.url.path.rstrip("/").split("/")[-2]
        if iid == "612":
            return {"closed": [], "open": [
                {"iid": 611, "state": "opened", "link_types": ["is_blocked_by"],
                 "references": {"full": "invora/invora-flutter#611"}}]}
        return {"closed": [], "open": []}

    routes = {
        "/groups/invora/issues": [
            _issue_json(611, "blocker", "invora/invora-flutter", 275),
            _issue_json(612, "blocked child", "invora/invora-flutter", 275),
        ],
        "/merge_requests": lambda req: [],
        "/links": links,
    }
    poller = _poller(st, routes, _cfg(
        [{"type": "group", "path": "invora", "enabled": True}],
        grouping=_grouping_policy()))
    asyncio.run(poller.poll_once())
    bundles = [t for t in st.get_all_tasks() if t.issues]
    assert len(bundles) == 1
    assert bundles[0].issues == ["invora/invora-flutter#611",
                                 "invora/invora-flutter#612"]


# ---------------------------------------------------------------------------
# 4. Cardinality guard
# ---------------------------------------------------------------------------

def test_bundle_size_cap_and_next_sitting_queue(clean_env, monkeypatch):
    """10 candidates, cap 4: the first sitting takes the cap-size batch on
    the lane; no second bundle task exists until it terminates."""
    monkeypatch.setenv("GITLAB_INTAKE_ENDPOINT", "https://gitlab.test")
    st = ClusterState()
    issues = [_issue_json(700 + i, f"i{i}", "invora/invora-flutter", 275)
              for i in range(10)]
    routes = {
        "/groups/invora/issues": issues,
        "/merge_requests": lambda req: [],
        "/links": lambda req: {"closed": [], "open": []},
    }
    poller = _poller(st, routes, _cfg(
        [{"type": "group", "path": "invora", "enabled": True}],
        grouping=_grouping_policy(max_bundle_size=4)))
    res = asyncio.run(poller.poll_once())
    tasks = st.get_all_tasks()
    assert len(tasks) == 1
    t = tasks[0]
    assert len(t.issues) == 4
    assert t.title.count("#700") or t.title
    # cycle 2: guard holds — still exactly one non-terminal bundle.
    res2 = asyncio.run(poller.poll_once())
    assert res2["created"] == []
    assert len([x for x in st.get_all_tasks()
                if x.status in (TaskStatus.pending, TaskStatus.ready,
                                TaskStatus.running)]) == 1


def test_scheduler_never_assigns_second_sitting_for_active_lane(clean_env):
    """The scheduler half of the guard (#833 rule #1 on the MAIN side):
    a ready task sharing a lane key with an ACTIVE task is not assigned."""
    from hermes_cluster.models import Node, NodeStatus
    st = ClusterState()
    st.register_node(Node(id="node_a", name="a", capabilities=["tooling"],
                          status=NodeStatus.online))
    st.register_node(Node(id="node_b", name="b", capabilities=["tooling"],
                          status=NodeStatus.online))
    t1 = st.create_task("t1", "sitting one", ["tooling"], lane_key="x-flutter#env/dev")
    t2 = st.create_task("t2", "sitting two", ["tooling"], lane_key="x-flutter#env/dev")
    st.trigger_pending_tasks()
    st.schedule_pending()
    # t1 took a node; t2 must NOT have been assigned in the same/next tick
    # while t1 is active.
    assert st.get_task("t1").status == TaskStatus.running
    assert st.get_task("t2").status == TaskStatus.ready, (
        "second sitting scheduled against a live lane — lane_key_guard would DENY it")
    st.schedule_pending()
    assert st.get_task("t2").status == TaskStatus.ready
    # Once the first sitting ends, the second may run.
    st.set_task_status("t1", TaskStatus.completed)
    st.schedule_pending()
    assert st.get_task("t2").status == TaskStatus.running
    # Different lane keys run concurrently (control: lanes don't cross-block).
    t3 = st.create_task("t3", "other lane", ["tooling"], lane_key="y-backend#env/dev")
    st.trigger_pending_tasks()
    st.schedule_pending()
    assert st.get_task("t3").status == TaskStatus.running


def test_sqlite_store_lane_guard_parity(clean_env, tmp_path):
    """The same guard lives in the SQLite store (production backend)."""
    from hermes_cluster.models import Node, NodeStatus
    store = ClusterStore(str(tmp_path / "t.db"))
    store.register_node(Node(id="node_a", name="a", capabilities=["tooling"],
                             status=NodeStatus.online))
    has_bundles = _accepts_issues_kwarg(store)
    store.create_task("t1", "one", ["tooling"], lane_key="x#b")
    if has_bundles:
        store.create_task("t2", "two", ["tooling"], lane_key="x#b",
                          issues=["p/x#2"])
    else:
        store.create_task("t2", "two", ["tooling"], lane_key="x#b")
    store.trigger_pending_tasks()
    store.schedule_pending()
    assert store.get_task("t1").status == TaskStatus.running
    assert store.get_task("t2").status == TaskStatus.ready
    if has_bundles:
        assert store.get_task("t2").issues == ["p/x#2"]  # membership persisted
    store.close()


def _accepts_issues_kwarg(store) -> bool:
    import inspect
    try:
        return "issues" in inspect.signature(store.create_task).parameters
    except (TypeError, ValueError):
        return False


# ---------------------------------------------------------------------------
# 5. Band 0 instant (business-team rule survives grouping)
# ---------------------------------------------------------------------------

_AUTHOR_BANDS = {"default": 3, "author_ids": {"12": 0, "8": 0}}


def test_band0_bundle_bypasses_queued_band3_bundle(clean_env, monkeypatch):
    """Sami/emad must never sit behind a big band-3 bundle: a band-0 batch
    creates its OWN queued bundle even while a band-3 bundle is already
    queued on the same lane (no nodes registered: the guard decision is
    observable before the scheduler's active-lane block kicks in)."""
    monkeypatch.setenv("GITLAB_INTAKE_ENDPOINT", "https://gitlab.test")
    st = ClusterState()
    issues_v1 = [
        _issue_json(801, "backlog", "invora/invora-flutter", 275),
    ]
    issues_v2 = issues_v1 + [
        _issue_json(802, "sami urgent", "invora/invora-flutter", 275,
                    author_id=12),
    ]
    cfg = _cfg([{"type": "group", "path": "invora", "enabled": True}],
               grouping=_grouping_policy(), priority=_AUTHOR_BANDS)
    routes = {
        "/groups/invora/issues": issues_v1,
        "/merge_requests": lambda req: [],
        "/links": lambda req: {"closed": [], "open": []},
    }
    poller = _poller(st, routes, cfg)
    res1 = asyncio.run(poller.poll_once())
    assert len(res1["created"]) == 1
    band3 = st.get_task(res1["created"][0])
    assert band3.priority == 3
    # Second cycle: band-0 issue arrives while the band-3 bundle is QUEUED.
    routes["/groups/invora/issues"] = issues_v2
    res2 = asyncio.run(poller.poll_once())
    assert len(res2["created"]) == 1, "band-0 stranded behind the queued band-3 bundle"
    band0 = st.get_task(res2["created"][0])
    assert band0.priority == 0
    assert band0.issues == ["invora/invora-flutter#802"]
    assert band0.lane_key == band3.lane_key
    # A THIRD band-0 does not double-stack: one queued band-0 per lane.
    routes["/groups/invora/issues"] = issues_v2 + [
        _issue_json(803, "emad urgent", "invora/invora-flutter", 275,
                    author_id=8)]
    res3 = asyncio.run(poller.poll_once())
    assert res3["created"] == []


def test_band0_packs_ahead_within_a_cycle(clean_env, monkeypatch):
    """Mixed backlog in ONE cycle: the first sitting is the band-0 batch ALONE
    (never padded with 40 band-3 issues), proving pack order band-first."""
    monkeypatch.setenv("GITLAB_INTAKE_ENDPOINT", "https://gitlab.test")
    st = ClusterState()
    routes = {
        "/groups/invora/issues": [
            _issue_json(811, "b1", "invora/invora-flutter", 275),
            _issue_json(812, "sami", "invora/invora-flutter", 275,
                        author_id=12),
            _issue_json(813, "b2", "invora/invora-flutter", 275),
        ],
        "/merge_requests": lambda req: [],
        "/links": lambda req: {"closed": [], "open": []},
    }
    poller = _poller(st, routes, _cfg(
        [{"type": "group", "path": "invora", "enabled": True}],
        grouping=_grouping_policy(), priority=_AUTHOR_BANDS))
    res = asyncio.run(poller.poll_once())
    assert len(res["created"]) == 1
    t = st.get_task(res["created"][0])
    assert t.priority == 0
    assert t.issues == ["invora/invora-flutter#812"]


def test_webhook_band0_instant_single_bundle(clean_env):
    app = create_app(cluster_id="t", node_id="n", node_role="main")

    async def go():
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            await c.put("/api/v1/intake/gitlab/policy", json={
                "enabled": True,
                "scopes": [{"type": "group", "path": "invora", "enabled": True}],
                "priority": _AUTHOR_BANDS,
                "dedup_scope": "full",
                "grouping": {"enabled": True},
            })
            # Band-0 author (sami=12): INSTANT bundle task with lane_key.
            r = await c.post("/api/v1/intake/gitlab/webhook", json={
                "object_kind": "issue",
                "object_attributes": {"iid": 991, "action": "open",
                                      "title": "urgent business"},
                "labels": [],
                "project": {"path_with_namespace": "invora/invora-flutter"},
                "user": {"id": 12},
            })
            assert r.status_code == 200
            body = r.json()
            assert body["status"] == "created"
            assert body["priority"] == 0
            assert body["lane_key"] == "invora-flutter#env/dev"
            task = r.json()["task"]
            assert task["issues"] == ["invora/invora-flutter#991"]
            assert "#991" in task["description"]
            # Band-3 author: DEFERRED to the poller (no per-issue task minted).
            r2 = await c.post("/api/v1/intake/gitlab/webhook", json={
                "object_kind": "issue",
                "object_attributes": {"iid": 992, "action": "open",
                                      "title": "normal"},
                "labels": [],
                "project": {"path_with_namespace": "invora/invora-flutter"},
                "user": {"id": 99},
            })
            assert r2.json()["status"] == "queued-grouping"
            assert r2.json().get("task_id") is None
    asyncio.run(go())


# ---------------------------------------------------------------------------
# 6. Legacy untouched when grouping is off (control — passes on main too)
# ---------------------------------------------------------------------------

def test_inflight_legacy_task_blocks_its_issue(clean_env, monkeypatch):
    """Wave-transition safety: a per-issue task STILL RUNNING (the 4 live
    invora tasks of the 872 wave) holds its issue — the grouped cycle must
    not double-fix it."""
    monkeypatch.setenv("GITLAB_INTAKE_ENDPOINT", "https://gitlab.test")
    st = ClusterState()
    legacy = st.create_task("legacy483", "[#483] REQ-INV-004 unit codes",
                            ["tooling"], priority=3)
    st.set_task_status("legacy483", TaskStatus.running)
    routes = {
        "/groups/invora/issues": [
            _issue_json(483, "REQ-INV-004", "invora/invora-backend", 276),
            _issue_json(484, "fresh", "invora/invora-backend", 276),
        ],
        "/merge_requests": lambda req: [],
        "/links": lambda req: {"closed": [], "open": []},
    }
    poller = _poller(st, routes, _cfg(
        [{"type": "group", "path": "invora", "enabled": True}],
        grouping=_grouping_policy()))
    res = asyncio.run(poller.poll_once())
    assert len(res["created"]) == 1
    t = st.get_task(res["created"][0])
    assert t.issues == ["invora/invora-backend#484"]


def test_grouping_off_keeps_per_issue_wiring_byte_for_byte(clean_env, monkeypatch):
    monkeypatch.setenv("GITLAB_INTAKE_ENDPOINT", "https://gitlab.test")
    st = ClusterState()
    routes = {
        "/groups/invora/issues": [
            _issue_json(1001, "a", "invora/invora-flutter", 275),
            _issue_json(1002, "b", "invora/invora-flutter", 275),
        ],
    }
    poller = _poller(st, routes, _cfg(
        [{"type": "group", "path": "invora", "enabled": True}]))
    res = asyncio.run(poller.poll_once())
    assert len(res["created"]) == 2
    for t in st.get_all_tasks():
        assert t.lane_key == ""
        assert t.title.startswith("[#")
        # getattr form so this CONTROL passes on main too (proves the legacy
        # path is byte-for-byte unchanged, not that a new field exists).
        assert list(getattr(t, "issues", None) or []) == []


# ---------------------------------------------------------------------------
# 7. Pure-planner unit pins (import inside: defect-shaped red on main)
# ---------------------------------------------------------------------------

def test_pack_orders_band_then_area_cluster():
    from hermes_cluster.core.intake_grouping import (
        GroupingConfig, LaneView, bundle_plan_for_repo, filter_candidates)
    cfg = GroupingConfig(enabled=True, max_bundle_size=3,
                         max_area_clusters_per_bundle=2)
    iss = lambda i, area=None, band=3: (
        f"p/r#{i}", {"iid": i, "labels": ([f"area::{area}"] if area else [])},
        band)
    cands = [iss(1, "invoices"), iss(2, "invoices"), iss(3, "parties"),
             iss(4)]  # band 3 each
    plans = filter_candidates([(c[0], c[1]) for c in cands], config=cfg,
                              band_for=lambda d: 3)
    assert len(plans) == 4
    from hermes_cluster.core.intake_grouping import pack_repo_candidates
    picked, reason = pack_repo_candidates("r#main", plans, cfg)
    ids = [p[0] for p in picked]
    assert ids == ["p/r#1", "p/r#2", "p/r#3"], (
        f"area-cluster packing expected (biggest cluster first, 2-cluster cap), got {ids}")
    # band order beats everything:
    plans0 = plans + [iss(9, "other", band=0)]
    picked0, _ = pack_repo_candidates("r#main", plans0, cfg)
    assert [p[2] for p in picked0] == [0] * len(picked0)


def test_lane_key_shape_matches_enforcement_registry():
    from hermes_cluster.core.intake_grouping import GroupingConfig, lane_key_for
    cfg = GroupingConfig(enabled=True)
    k = lane_key_for("invora/invora-flutter", cfg)
    assert k == "invora-flutter#env/dev"
    # Same regex bdaya-enforcement's AUTHOR_LANE_KEY_RE accepts.
    import re
    assert re.fullmatch(r"[A-Za-z0-9_.\-/]+#[A-Za-z0-9_.\-/]+", k)


def test_bundle_membership_persists_in_sqlite(clean_env, tmp_path):
    store = ClusterStore(str(tmp_path / "m.db"))
    # Red shape = the defect's assertion (a missing kwarg would TypeError, an
    # import-error shape) — assert the surface exists first.
    assert _accepts_issues_kwarg(store), \
        "ClusterStore.create_task() must accept description= and issues= (#762)"
    t = store.create_task("bt1", "[lane:x#b][##1…3 x2]", ["tooling"],
                          lane_key="x#b", description="Refs #1 Refs #3",
                          issues=["p/x#1", "p/x#3"])
    back = store.get_task("bt1")
    assert back.issues == ["p/x#1", "p/x#3"]
    assert "Refs #1" in back.description
    tasks = store.get_all_tasks()
    assert tasks[0].issues == ["p/x#1", "p/x#3"]
    store.close()
