"""The #909 placement-authority gate must not be walkable via node-id spelling.

RV-1 round 2 on PR#67 returned NEEDS-CHANGES with a working exploit against
`_same_node`, which compared ASYMMETRICALLY:

    a == b or a.removeprefix("node_") == b or b.removeprefix("node_") == a

    _same_node('node_x', 'node_node_x')
      a == b?                        'node_x' == 'node_node_x'  -> False
      a.removeprefix('node_') == b?  'x'      == 'node_node_x'  -> False
      b.removeprefix('node_') == a?  'node_x' == 'node_x'       -> True   <-- bypass

Consequence, in the reviewer's words: "an attacker with token for 'node_x' can
pin lanes to 'node_node_x'". Lane pinning decides WHERE work runs, so this is a
scheduling-integrity hole, not a cosmetic one.

The fix is SYMMETRY, not idempotency. `canonical_node_id` deliberately strips at
most ONE prefix and is deliberately NOT idempotent: greedy stripping would be
idempotent and would collapse `node_node_x` onto `x`, re-opening this very
bypass. See the docstrings in core/lane_affinity.py.
"""

from __future__ import annotations

import pytest

from hermes_cluster.core.lane_affinity import canonical_node_id, same_node
from hermes_cluster.routers.lanes import _same_node


# --- the exploit, verbatim -------------------------------------------------

@pytest.mark.parametrize("impl", [same_node, _same_node],
                         ids=["core.same_node", "routers.lanes._same_node"])
def test_doubled_prefix_is_not_the_same_node(impl):
    """The RV-1 exploit. Both call sites must refuse it."""
    assert impl("node_x", "node_node_x") is False
    assert impl("node_node_x", "node_x") is False, "and symmetrically"


@pytest.mark.parametrize("impl", [same_node, _same_node],
                         ids=["core.same_node", "routers.lanes._same_node"])
def test_bypass_generalises_to_any_name(impl):
    """Not special to 'x' — the shape is <name> vs node_<name> with the
    registry prefix already present on the victim."""
    for name in ("worker", "windows_pc_worker", "a"):
        assert impl(f"node_{name}", f"node_node_{name}") is False


# --- the legitimate case the helper exists for -----------------------------
#
# Measured on the live fleet 2026-09-15: every worker config declares
# `node.id` WITHOUT the prefix, while GET /api/v1/nodes reports it WITH one.
# These are the real pairs; removing normalisation entirely would 403 the
# whole fleet, which is why the fix keeps it and fixes the asymmetry instead.

REAL_FLEET_PAIRS = [
    ("windows_desktop_worker", "node_windows_desktop_worker"),
    ("windows_pc_worker", "node_windows_pc_worker"),
    ("macbook_worker", "node_macbook_worker"),
]


@pytest.mark.parametrize("impl", [same_node, _same_node],
                         ids=["core.same_node", "routers.lanes._same_node"])
@pytest.mark.parametrize("bare,registered", REAL_FLEET_PAIRS)
def test_real_fleet_spellings_still_match(impl, bare, registered):
    assert impl(bare, registered) is True
    assert impl(registered, bare) is True, "must hold in both argument orders"


@pytest.mark.parametrize("impl", [same_node, _same_node],
                         ids=["core.same_node", "routers.lanes._same_node"])
def test_distinct_nodes_never_match(impl):
    for a, _ in REAL_FLEET_PAIRS:
        for b, _ in REAL_FLEET_PAIRS:
            if a == b:
                continue
            assert impl(a, b) is False
            assert impl(f"node_{a}", b) is False


@pytest.mark.parametrize("impl", [same_node, _same_node],
                         ids=["core.same_node", "routers.lanes._same_node"])
def test_empty_is_never_a_match(impl):
    assert impl("", "node_x") is False
    assert impl("node_x", "") is False
    assert impl("", "") is False


# --- the property that makes the bypass impossible by construction ---------

def test_same_node_is_symmetric():
    """Asymmetry WAS the defect. Pin the property, not just its instances."""
    ids = ["x", "node_x", "node_node_x", "windows_pc_worker",
           "node_windows_pc_worker", "", "  node_x  "]
    for a in ids:
        for b in ids:
            assert same_node(a, b) == same_node(b, a), (a, b)


def test_same_node_is_equality_of_canonical_forms():
    """The implementation contract: canonicalise BOTH sides, compare once."""
    ids = ["x", "node_x", "node_node_x", "macbook_worker",
           "node_macbook_worker"]
    for a in ids:
        for b in ids:
            expected = bool(a and b) and (
                canonical_node_id(a) == canonical_node_id(b))
            assert same_node(a, b) is expected, (a, b)


def test_canonical_strips_at_most_one_prefix_and_is_NOT_idempotent():
    """Deliberate. Greedy stripping would be idempotent and would re-open the
    bypass by collapsing node_node_x onto x."""
    assert canonical_node_id("node_x") == "x"
    assert canonical_node_id("node_node_x") == "node_x"
    assert canonical_node_id("x") == "x"
    # the non-idempotency is the point — assert it so nobody "fixes" it
    assert canonical_node_id(canonical_node_id("node_node_x")) == "x"
    assert canonical_node_id("node_node_x") != "x"


def test_canonical_trims_surrounding_whitespace():
    assert canonical_node_id("  node_macbook_worker  ") == "macbook_worker"
    assert same_node(" node_macbook_worker ", "macbook_worker") is True


def test_both_call_sites_share_one_implementation():
    """One function, so the 403 gate and the scheduler pin cannot drift apart.
    Two copies of a security rule is how this defect existed in two places."""
    assert _same_node is same_node or _same_node("node_a", "a") is True
    ids = ["x", "node_x", "node_node_x", "node_windows_pc_worker",
           "windows_pc_worker", ""]
    for a in ids:
        for b in ids:
            assert _same_node(a, b) == same_node(a, b), (a, b)
