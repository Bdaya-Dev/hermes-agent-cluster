"""#929 — the vendored footnote pair must not drift from its source.

``hermes_cluster/core/lane_cost_footnote.py`` and ``lane_cost_report.py`` are
vendored copies of shared/claude-plugins files (the footnote's own module
docstring opens with a VENDORED COPY header block). The executor prefers the
live repo copy when BDAYA_LANE_COST_SCRIPTS is set; this test guards the
vendored fallback. It only runs where a claude-plugins checkout carrying the
#860 files is discoverable (BDAYA_CLAUDE_PLUGINS_ROOT or the sibling lane
dir); otherwise it skips, since the CI container carries neither repo.

The two sanctioned differences from the source are the header block itself
and the corrected footer sentence (the source's shipped claim — "Alibaba
exposes no API usage meter" — is what shared/claude-plugins#929's metering
research disproved; see the MR). Everything else must be byte-equal — the
pricing maths in particular, since a silent divergence there means the two
footnote legs price the same lane differently.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
CORE = HERE.parent / "hermes_cluster" / "core"

# Vendored files open with a header block fused into the original docstring;
# the block ends at the first blank line after the 'VENDORED COPY' opener.
HEADER_RE = re.compile(r'^"""VENDORED COPY[^\n]*\n(?:.*?\n)*?\n', re.DOTALL)

# The single sanctioned footer-sentence swap, once string-continuation joins
# are normalized (the source's line-wrapping differs between the two states).
_SOURCE_FOOTER = ("not authoritative: Alibaba exposes no API usage meter. "
                  "Reporting only, never a gate")
_VEND_FOOTER = ("not authoritative; the authoritative seat figure is the "
                "ModelStudio OpenAPI GetSubscriptionStats "
                "(SeatCredits/SeatRemainingCredits, aliyun CLI "
                "'modelstudio get-subscription-stats'). Reporting only, "
                "never a gate")


def _repo_root() -> Path | None:
    for cand in filter(None, (os.environ.get("BDAYA_CLAUDE_PLUGINS_ROOT"),
                              str(Path.home() / "hermes-lanes" / "claude-plugins-main"))):
        p = Path(cand)
        if ((p / "hermes" / "scripts" / "lane_cost_report.py").is_file()
                and (p / "hermes" / "plugins" / "bdaya-enforcement" /
                     "lane_cost_footnote.py").is_file()):
            return p
    return None


repo = _repo_root()
requires_repo = pytest.mark.skipif(
    repo is None,
    reason=("no claude-plugins checkout with the #860 files on this machine "
            "(a checkout older than the pin legitimately lacks them)"))


def _norm(text: str) -> str:
    joined = re.sub(r'"\s*\n\s*"', "", text)
    return joined.replace(_VEND_FOOTER, "@@FOOTER@@").replace(_SOURCE_FOOTER, "@@FOOTER@@")


@requires_repo
def test_vendored_lane_cost_report_is_byte_equal_to_source():
    src = (repo / "hermes" / "scripts" / "lane_cost_report.py").read_text(encoding="utf-8")
    vend = (CORE / "lane_cost_report.py").read_text(encoding="utf-8")
    assert vend == src, "vendored lane_cost_report.py drifted from hermes/scripts/"


@requires_repo
def test_vendored_footnote_differs_only_by_header_and_footer():
    src = (repo / "hermes" / "plugins" / "bdaya-enforcement" /
           "lane_cost_footnote.py").read_text(encoding="utf-8")
    vend = (CORE / "lane_cost_footnote.py").read_text(encoding="utf-8")

    m = HEADER_RE.match(vend)
    assert m and m.group(0).rstrip().endswith("MR.") or m, (
        "vendored copy must open with the VENDORED COPY header block")
    body = vend[m.end():]
    # The fused docstring: header block ends on a blank line, the original
    # docstring's first line follows WITHOUT its opener (the header IS the
    # opener). Re-attach it for the comparison.
    rest = src[3:] if src.startswith('"""') else src
    assert _norm(body) == _norm(rest), (
        "vendored footnote drifted beyond the header + sanctioned footer edit")
