# Capabilities — the fleet's vocabulary

A task's `requires` is matched against the capabilities each node advertises
(`scheduler.node_can_run`). A capability no node advertises can never match, so
the task is accepted and then **queues forever** — silently, with idle workers,
indistinguishable from a task merely waiting its turn.

That happened. Mined from 1394 real tasks on 2026-09-15, these were requested and
served by nobody:

| requested | tasks | what the caller actually meant |
|---|---|---|
| `pc-maint` | 9 | "run maintenance on windows-pc" — the node had been drained by stripping its capabilities |
| `author` | 5 | the task's **role**, not a capability |
| `flutter` | 2 | "build/test Flutter here" — windows-desktop *had* Flutter 3.44 and never advertised it |
| `invora` | 2 | a **repo scope**, not a machine capability |
| `land` | 1 | "merge this MR" — an **action**, gated by a write credential |
| `merge` | 1 | same |

One of those was `bayader-flutter!221`'s landing task: a reviewed client MR sat
unclaimable and never merged while the cluster ran at 4 of 22 slots.

Nobody was guessing carelessly. There was no vocabulary to read.

## The vocabulary

Live truth is always `GET /api/v1/capabilities` — it reports what each node
currently advertises, which capabilities are servable right now, and anything
live tasks are stalled on. This table is the *intent*; the endpoint is the fact.

| capability | means | gated by |
|---|---|---|
| `tooling` | general agent work: repos, editors, shell, the MCP surface | static |
| `native` | runs a native Hermes session on this OS | static |
| `native-win` | needs Windows specifically (paths, PowerShell, Win32 tooling) | static |
| `planning` | long-context planning/triage work | static |
| `review` | fresh-context RV-1 review lanes | static |
| `github-write` | can mint a `bdaya-lane-agent` App token and write to GitHub | probe (`gh-write-doctor.py`) |
| `flutter` | Flutter SDK present and runnable | probe |
| `dotnet` | .NET SDK present and runnable | probe |
| `gcp` | can mint an ADC token as this machine | probe |
| `k8s` | can reach **and is authorized against** the cluster API server | probe |

## What is NOT a capability

Three things kept being written into `requires` that do not belong there. Each
has a real home:

- **A role** — `author`, `reviewer`. That is the task's `role` field. A reviewer
  lane is `role="reviewer", requires=["review"]`.
- **An action** — `merge`, `land`, `approve`. Actions are gated by the
  *credential* that performs them: merging on GitHub needs `github-write`. Name
  what the machine must be able to do, not what you intend to do with it.
- **A repo or client scope** — `invora`, `bayader`. Every node can clone every
  repo; scope belongs in the task's brief, not its scheduling constraint.

A fourth trap, because it cost a live stall: **an intake filter label is not a
capability** (#873). `hermes-factory` is how issues are selected, not something a
worker can do.

## Adding one

1. **Write a probe** in `scripts/capability_probe.py`. It must prove a lane can
   actually *do the thing* on this machine, not that a binary is on `PATH` — a
   binary that cannot authenticate produces exactly the mid-lane failure probes
   exist to prevent. Probes that succeed by minting a credential must pass
   `redact=True`; never print a secret value.
2. **Declare it** in the node's `cluster-worker-*.yaml` under
   `node.capability_probes`, pointing at `capability_probe.py <name>`. The
   connector declares the capability only while the probe exits 0 (#867), and
   re-probes on `interval_s`; a capability lost mid-run is PATCHed off.
3. **Add a row to the table above**, so the next caller can read it instead of
   guessing.

A static capability (no probe) is for things that cannot meaningfully fail —
`native-win` on a Windows box. If it *can* fail, probe it.

## Draining a node

Use `PATCH /api/v1/nodes/{id}/drain` with `{"drained": true}`.

Do **not** drain by stripping capabilities. It fails twice over: a task with an
empty `requires` matches every node whatever it advertises, so the node keeps
taking unconstrained work; and the worker's next re-join rewrites the list from
its own local config, reverting the strip. That revert is what orphaned the nine
`pc-maint` tasks above.

Marking the node offline does not work either — the watchdog flips it back on
the next heartbeat, because liveness is worker-reported and drain is not.
