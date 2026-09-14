"""#893: register() and plugin.yaml provides_* must not drift.

`hermes plugins validate` probes register(ctx) against a RecordingContext and diffs the
actually-registered tools/hooks against the manifest's provides_* lists; any undeclared
registration FAILS validation, and discovery gates session loading on that manifest —
so a manifest that drifts from register() means the plugin never loads on a worker at
all (the #893 no-submit-surface class). These tests mirror that correlation so a future
refactor cannot let the two drift: RED on a manifest missing provides_tools/provides_hooks,
GREEN once declared.

Note: the legacy top-level `hooks:` key in plugin.yaml is inert — Hermes only ever reads
`provides_hooks` (hermes_cli/plugins_manifest.py parse: `data.get("provides_hooks", [])`;
hermes_cli/plugin_validate.py _check_capabilities compares against "provides_hooks").
"""
from __future__ import annotations

import importlib.util
import re
from pathlib import Path

PLUGIN_DIR = Path(__file__).resolve().parent.parent / "plugins" / "hermes-agent-cluster"

# Exactly what plugins/hermes-agent-cluster/__init__.py register() registers.
EXPECTED_TOOLS = [
    "kanban_cluster_init",
    "kanban_cluster_join",
    "kanban_cluster_submit",
    "kanban_cluster_list",
    "kanban_cluster_nodes",
    "kanban_cluster_heartbeat",
    "kanban_cluster_complete",
    "kanban_cluster_status",
    "kanban_cluster_config",
]

EXPECTED_HOOKS = ["on_session_start", "on_session_end"]


def _load_plugin_module():
    spec = importlib.util.spec_from_file_location(
        "hermes_agent_cluster_plugin_893", PLUGIN_DIR / "__init__.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _manifest_list(manifest_text: str, key: str):
    """Extract one top-level list from plugin.yaml WITHOUT requiring PyYAML."""
    out: list[str] = []
    in_block = False
    for line in manifest_text.splitlines():
        stripped = line.lstrip()
        if stripped.startswith(f"{key}:"):
            in_block = True
            continue
        if in_block:
            if stripped.startswith("- "):
                name = stripped[2:].strip()
                assert re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", name), f"bad {key} entry {name!r}"
                out.append(name)
                continue
            if stripped and not stripped.startswith("#"):
                break
    return out


class RecordingContext:
    """Mirrors the RecordingContext in hermes_cli/plugin_validate.py's capability probe."""

    def __init__(self):
        self.tools: list[str] = []
        self.hooks: list[str] = []

    def register_tool(self, *args, **kwargs):
        self.tools.append(str(kwargs.get("name", args[0] if args else "")))

    def register_hook(self, hook_name, *args, **kwargs):
        self.hooks.append(str(hook_name))


def _recorded():
    ctx = RecordingContext()
    _load_plugin_module().register(ctx)
    return ctx


def test_register_matches_expected_surface():
    """The pinned baseline: register() really registers these nine tools + two hooks."""
    ctx = _recorded()
    assert sorted(ctx.tools) == sorted(EXPECTED_TOOLS)
    assert sorted(ctx.hooks) == sorted(EXPECTED_HOOKS)


def test_provides_tools_matches_register():
    ctx = _recorded()
    declared = _manifest_list((PLUGIN_DIR / "plugin.yaml").read_text(encoding="utf-8"),
                              "provides_tools")
    assert sorted(declared) == sorted(ctx.tools), (
        f"manifest provides_tools drifted from register(): "
        f"undeclared={sorted(set(ctx.tools) - set(declared))} "
        f"unregistered={sorted(set(declared) - set(ctx.tools))}"
    )


def test_provides_hooks_matches_register():
    ctx = _recorded()
    declared = _manifest_list((PLUGIN_DIR / "plugin.yaml").read_text(encoding="utf-8"),
                              "provides_hooks")
    assert sorted(declared) == sorted(ctx.hooks), (
        f"manifest provides_hooks drifted from register(): "
        f"undeclared={sorted(set(ctx.hooks) - set(declared))} "
        f"unregistered={sorted(set(declared) - set(ctx.hooks))}"
    )


def test_legacy_hooks_key_not_used_for_capabilities():
    """The dead `hooks:` key must not come back as a shadow source of truth:
    discovery reads provides_hooks only, so a manifest with `hooks:` alone fails
    the capability check (#893). Assert the declaration lives under the key Hermes
    actually parses."""
    manifest = (PLUGIN_DIR / "plugin.yaml").read_text(encoding="utf-8")
    assert "\n  - on_session_start" in manifest  # under some top-level list
    assert re.search(r"^provides_hooks:", manifest, re.M), (
        "on_session_start/on_session_end must be declared under provides_hooks; "
        "the legacy top-level `hooks:` key is never read by Hermes "
        "(plugins_manifest.py parses only data.get('provides_hooks'))"
    )
