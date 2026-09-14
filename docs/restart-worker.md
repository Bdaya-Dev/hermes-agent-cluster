# Restarting a Hermes cluster worker (the only sanctioned way) — #899

The 2026-09-14 windows_desktop_worker incident: to apply a config change the
operator stopped the `python -m hermes_cluster.serve` **child** of the
keep-alive wrapper and started the scheduled task. The `:loop` in
`run-worker-desktop.cmd` relaunched the child 5 s later, the scheduled task
started a **second** wrapper, and for ~90 minutes the node id had two
executors: every assigned lane spawned twice, the lane-lock losers marked
tasks `failed`, and 81 lane processes piled up on the box.

Defenses now in the code:

* **serve refuses to double-start** — worker role takes a single-instance
  lock per node id (`hermes_cluster/instance_lock.py`) before binding
  anything. A second instance exits with code **3** and a message naming the
  live twin's pid. The `:loop` wrapper treats that like any crash and backs
  off; the live owner is never disturbed.
* **the main knows who owns a node id** — every join/heartbeat carries a
  per-process instance token. A re-join force-replaces the owner (documented
  choice: a restart legitimately re-joins), and the replaced twin's next
  heartbeat is answered `{"status":"replaced"}`; its connector then
  terminates its own process with code **4**. A zombie executor is now
  self-correcting within one heartbeat interval (~10–30 s), not manual.

## Restart procedure (config change or upgrade)

Windows (scheduled task + wrapper), ONE command — it ends the wrapper FIRST,
so nothing relaunches behind your back:

```bat
schtasks /end /tn "Hermes-Worker-Desktop" && schtasks /run  /tn "Hermes-Worker-Desktop"
```

`schtasks /end` stops the task (wrapper **and** its child, as the task's
process tree); `/run` restarts the wrapper, which starts a fresh serve that
re-joins and takes ownership. No manual kill is ever needed.

POSIX members (launchd/systemd or a shell `while` wrapper): use the service
manager (`launchctl kickstart -k gui/$UID/<label>` / `systemctl restart`), or
`kill` the WRAPPER pid — never just the python child of a `while :; do …;
done` supervisor, which is exactly the `:loop`-child mistake above.

If you are unsure which process is which: the live serve holds
`<data dir>/hermes-worker-<node_id>.lock` containing its pid (default data
dir: `$HERMES_HOME/run`, else `~/.config/bdaya/hermes-cluster-run`;
override with `agent_executor.lock_dir` in the worker YAML). The lock file
self-heals: a crashed holder's lock is stolen by the next start, so a reboot
or kill -9 never wedges the worker.

## Telling the stories apart in the wrapper log

* exit **3** = "another instance already owns this node id" — benign while
  the real worker runs; investigate only if no owner should exist (stale
  lock dir, duplicated task).
* exit **4** = "the main replaced my instance" — this process was the zombie
  twin and stood down on purpose; the newer instance now owns the node.
