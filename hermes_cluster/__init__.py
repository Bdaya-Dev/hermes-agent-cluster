"""hermes-cluster — Python backend for Hermes Agent Cluster.

This package replaces the Go backend with a FastAPI-based implementation.
It provides:
  - REST API at /api/v1/*
  - Web Dashboard at /dashboard/*
  - Health check at /health
  - Hermes Agent plugin integration

#893: create_app/ClusterState are resolved LAZILY (PEP 562 __getattr__).
The eager imports here made `import hermes_cluster.core.peer_auth` — a
stdlib-only module the Hermes plugin signs with — pull in fastapi/pydantic
(>10 s, and a hard ImportError where they are absent). The plugin and every
signing helper live entirely in the stdlib half of the tree; the server is
imported only by whoever actually runs it (serve.py imports .app itself).
"""

__version__ = "1.0.0"

_LAZY = {"create_app": ".app", "ClusterState": ".state"}

__all__ = ["create_app", "ClusterState"]


def __getattr__(name):
    module = _LAZY.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib
    value = getattr(importlib.import_module(module, __name__), name)
    globals()[name] = value  # resolve once
    return value


def __dir__():
    return sorted(set(globals()) | set(_LAZY))
