"""#893 (shim leg): the LOADABLE plugin directory must be a thin shim over
hermes_cluster.plugin — one implementation, no drift.

The bug this locks: PR#42 fixed hermes_cluster/plugin.py (config-resolved
remote endpoint, peer signing, #893 loud-by-default errors) but Hermes LOADS
plugins/hermes-agent-cluster/__init__.py, which remained a full 419-line COPY
of the pre-#893 plugin: its own _api_call/_start_server/handle_* with _base_url
hard-bound to loopback, zero references to hermes_cluster.plugin. Every worker
session therefore still talked to a local auto-started server after the fix
"shipped" (measured on windows_desktop 2026-09-14 04:35Z after a clean
reinstall at main fb92e450: kanban_cluster_status answered the local
{cluster_id: hermes-cluster, node_id: node_main, nodes.total: 0} while the
hosted main answers {cluster_id: bdaya_hermes_cluster, nodes.total: 3}).

RED on a drifted copy, GREEN once __init__.py is a pure re-export shim.

Import-clean against main by design: this file imports NOTHING from the repo
at top level (spec_from_file_location inside the tests, stdlib everywhere), so
every red here is the defect's assertion, never an ImportError.
"""
from __future__ import annotations

import ast
import importlib.util
import re
import sys
import types
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
PLUGIN_DIR = REPO_ROOT / "plugins" / "hermes-agent-cluster"
INIT_PATH = PLUGIN_DIR / "__init__.py"
MODULE_PATH = REPO_ROOT / "hermes_cluster" / "plugin.py"

# The drift-guard forbiddens: an OWN implementation of any of these in the
# loadable shim is the #893 stale-copy class.
OWN_IMPL_NAMES = re.compile(r"^(_api_call|_start_server|handle_.+)$")
LOOPBACK_LITERAL = "127" + ".0.0.1"  # split: this guard must not match itself


def _load_shim():
    """Load plugins/hermes-agent-cluster/__init__.py the way Hermes's
    plugin_validate probe does (spec_from_file_location, stdlib-only)."""
    for name in list(sys.modules):
        if name == "_shim893" or name.startswith("_shim893."):
            del sys.modules[name]
    spec = importlib.util.spec_from_file_location(
        "_shim893", INIT_PATH, submodule_search_locations=[str(PLUGIN_DIR)]
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    module.__package__ = "_shim893"
    module.__path__ = [str(PLUGIN_DIR)]
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        del sys.modules[spec.name]
        raise
    return module


def _make_pkg(name: str, path: Path) -> types.ModuleType:
    pkg = types.ModuleType(name)
    pkg.__path__ = [str(path)]  # type: ignore[attr-defined]
    pkg.__package__ = name
    sys.modules[name] = pkg
    return pkg


@pytest.fixture()
def shim_and_module():
    """(loaded shim, loaded hermes_cluster.plugin) with the repo importable —
    mirroring a fleet install, where hermes_cluster is importable in the
    runtime env (the shim's only dependency)."""
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    saved = {n: m for n, m in sys.modules.items()
             if n == "hermes_cluster" or n.startswith("hermes_cluster.")}
    # Force a fresh, repo-pinned hermes_cluster even if something else (an
    # editable wheel, a prior lane) pre-imported a different copy.
    for n in list(saved):
        del sys.modules[n]
    pkg = _make_pkg("hermes_cluster", REPO_ROOT / "hermes_cluster")
    sub = _make_pkg("hermes_cluster.core", REPO_ROOT / "hermes_cluster" / "core")
    pkg.core = sub  # type: ignore[attr-defined]
    try:
        # Canonical module FIRST: the shim's `import hermes_cluster.plugin`
        # must resolve to this exact object, else identity assertions compare
        # two legitimate loads instead of one implementation.
        pm_spec = importlib.util.spec_from_file_location(
            "hermes_cluster.plugin", MODULE_PATH)
        assert pm_spec is not None and pm_spec.loader is not None
        pm = importlib.util.module_from_spec(pm_spec)
        pm.__package__ = "hermes_cluster"
        sys.modules["hermes_cluster.plugin"] = pm
        pm_spec.loader.exec_module(pm)
        pkg.plugin = pm  # type: ignore[attr-defined]
        shim = _load_shim()
        yield shim, pm
    finally:
        for n in list(sys.modules):
            if n == "hermes_cluster" or n.startswith("hermes_cluster.") or n == "_shim893":
                del sys.modules[n]
        sys.modules.update(saved)


def test_shim_defines_no_own_implementation(shim_and_module):
    """RED on main: the loadable __init__.py still owns _api_call /
    _start_server / handle_* — a second, pre-#893 implementation."""
    shim, _pm = shim_and_module
    tree = ast.parse(INIT_PATH.read_text(encoding="utf-8"))
    offenders = sorted(
        n.name for n in ast.walk(tree)
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
        and OWN_IMPL_NAMES.match(n.name)
    )
    assert not offenders, (
        "plugins/hermes-agent-cluster/__init__.py must be a thin shim over "
        "hermes_cluster.plugin (#893): these functions are defined in the "
        f"loadable plugin dir: {offenders}")


def test_shim_contains_no_loopback_literal(shim_and_module):
    """RED on main: the pre-#893 copy hard-binds _base_url to loopback (4
    literals). The single legitimate loopback lives in hermes_cluster/plugin.py's
    documented single-node fallback — never in the shim."""
    shim, _pm = shim_and_module  # noqa: F841 - fixture side effect: must load
    src = INIT_PATH.read_text(encoding="utf-8")
    hits = [i + 1 for i, line in enumerate(src.splitlines())
            if LOOPBACK_LITERAL in line]
    assert not hits, (
        f"plugins/hermes-agent-cluster/__init__.py contains {LOOPBACK_LITERAL!r} "
        f"at line(s) {hits} — the loadable plugin dir must not own any "
        "endpoint/loopback logic (#893 stale-copy class)")


def test_shim_imports_the_canonical_plugin(shim_and_module):
    """RED on main: zero references to hermes_cluster.plugin anywhere in the
    loadable dir. GREEN: the shim's module-level code re-exports it."""
    shim, _pm = shim_and_module
    src = INIT_PATH.read_text(encoding="utf-8")
    assert re.search(r"\bfrom\s+hermes_cluster\.plugin\b|\bimport\s+hermes_cluster\.plugin\b", src), (
        "shim must import from hermes_cluster.plugin — one implementation")
    import hermes_cluster  # noqa: F401 - the fixture pins sys.modules['hermes_cluster'] to the repo copy


def test_shim_register_is_the_module_register(shim_and_module):
    """One implementation means one register(): identity, not a lookalike."""
    shim, pm = shim_and_module
    assert shim.register is pm.register, (
        "shim register() is not hermes_cluster.plugin.register — a second "
        "registration path can drift (#893)")


def test_shim_registers_the_expected_surface(shim_and_module):
    """register(ctx) through the loadable dir registers the nine
    kanban_cluster_* tools + the two session hooks — the surface plugin.yaml
    declares (the PR#44 correlation test pins the manifest against THIS dir)."""
    shim, _pm = shim_and_module

    class RecordingContext:
        """Mirrors hermes_cli/plugin_validate.py's probe ctx: no get_config,
        getattr-probed by register()."""

        def __init__(self):
            self.tools: list[str] = []
            self.hooks: list[str] = []

        def register_tool(self, *args, **kwargs):
            self.tools.append(str(kwargs.get("name", args[0] if args else "")))

        def register_hook(self, hook_name, *args, **kwargs):
            self.hooks.append(str(hook_name))

    ctx = RecordingContext()
    shim.register(ctx)
    assert sorted(ctx.tools) == [
        "kanban_cluster_complete", "kanban_cluster_config", "kanban_cluster_heartbeat",
        "kanban_cluster_init", "kanban_cluster_join", "kanban_cluster_list",
        "kanban_cluster_nodes", "kanban_cluster_status", "kanban_cluster_submit",
    ]
    assert sorted(ctx.hooks) == ["on_session_end", "on_session_start"]


def test_shim_handlers_are_the_module_handlers(shim_and_module):
    """Identity, not lookalikes: the tool handlers and session hooks the shim
    hands to Hermes ARE hermes_cluster.plugin's — a copy can drift, an import
    cannot."""
    shim, pm = shim_and_module
    for name in ("handle_cluster_submit", "handle_cluster_status",
                 "_on_session_start", "_on_session_end", "_api_call",
                 "_ensure_base_url", "_config_from_hermes_settings"):
        assert getattr(shim, name) is getattr(pm, name), (
            f"shim.{name} is not hermes_cluster.plugin.{name} — the loadable "
            "dir owns a second implementation (#893 stale-copy class)")
