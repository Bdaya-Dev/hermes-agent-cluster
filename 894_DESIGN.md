# 894 ballot relay — lane design (author: 894-ballot-relay)

## Notifier choice: POLL TICK, not push. Justification (evidence):
infra-github cluster-config/hermes-gateway/service.yaml: gateway is
`type: ClusterIP`, docstring "the gateway's control surface is Telegram
long-polling (OUTBOUND HTTPS) — it needs no ingress... No HTTPRoute". There is
NO inbound HTTP path from cluster-main to the gateway pod, and the cluster's
hooks deliver to an arbitrary URL (would need an exposed gateway endpoint that
GitOps deliberately refuses to create). So main→gateway push is architecturally
blocked by the estate's own security posture. The gateway ALREADY runs an
in-process periodic thread (poll_probe publisher; the plugin can drive a
background tick like the alert CronJob does). PICK: a background poll tick in
the plugin that reads pending ballots and renders them. Matches how the fleet
already gets work done in this image.

## Ballot record (on the task row, JSON TEXT column `ballot`):
{question, options:[...], class:"technical"|"product" (default technical per
owner note 138210), asked_at, lane_key, answer, answered_at, answered_by}

## Fork (hermes-agent-cluster) — branch feat/894-ballot-relay:
1. models: Task.ballot: Optional[dict]; BlockTaskRequest, AnswerTaskRequest.
2. core/ballot.py: shared validate_ballot / build_ballot (used by router+stores).
3. state (3 backends: __init__, cluster_store, postgres_store):
   - `block_task_for_ballot(task_id, ballot)`: guarded flip
     running->blocked + store ballot (mirrors the existing blocked flip;
     releases the live lease so the freed lane can be re-dispatched).
   - `answer_ballot(task_id, answer, answered_by)`: store answer on ballot,
     flip blocked->ready (reuses unblock semantics but ->ready so lane
     affinity re-dispatches; NOT ->pending like legacy unblock).
   - `get_pending_ballots()`: tasks with status blocked + ballot.answer None.
   - schema + _migrate_schema: ADD COLUMN ballot TEXT.
4. routers/tasks.py:
   - POST /{id}/block  (peer-gated by middleware; runs headless-lane escalation)
   - POST /{id}/answer (peer-gated, same trust as submit/cancel)
   - GET  /{id}/ballot and GET /ballots/pending (read for the relay)
5. plugin.py (LOCAL client): + kanban_cluster_block, kanban_cluster_answer tool
   (a lane can post/answer directly too).
6. executor (agent_executor.py):
   - hermes reap: if <task>.escalation.json sidecar present at rc==0 ->
     outcome "blocked": POST /block with the ballot; DO NOT touch/drop the
     lane session row (so it resumes). New _report_blocked + _read_escalation.
   - resume-with-answer: _write_brief, when task.ballot.answer is set, writes
     the answer as the delivery message (the SAME session resumes; --resume
     path already exists for lane_key).
7. tests_v3/test_ballot_relay_894.py: RED/GREEN — block flips+attaches,
   answer unblocks+stores, pending read, terminal/409 guards, class default,
   escalation sidecar -> blocked (not done), resume renders answer.

## Image (hermes-gateway-image) — plugin hermes-cluster-remote v1.3:
- gate: reuse the SAME 3-leg action gate + peer HMAC signing already in the
  module (_signed_headers / action_gate).
- kanban_cluster_ballots (READ tool: GET /api/v1/ballots/pending).
- kanban_cluster_answer WRITE tool (owner types/answers in chat) behind gate.
- NOTIFIER: background tick thread (poll_interval config) reads pending
  ballots, renders each ONCE via the native Telegram adapter send_clarify
  (register a clarify_gateway entry keyed by task id + `cl:` callback) and on
  tap posts to /answer. Dedup by ballot asked_at so a task isn't re-pinged.
- Routing (owner note 138210): class product -> business group chat id,
  technical -> owner DM chat id; chat ids from GitOps CONFIG (never env); the
  answer path accepts replies from either chat, gated by allowed_users only.

## Security:
- /block and /answer are NOT in PUBLIC_PATHS -> when peer auth is enabled they
  require a valid peer HMAC signature (same middleware path as submit/cancel).
  Tests assert 401 unsigned / 200 signed.
- The image relay signs with the gateway's peer token (HERMES_CLUSTER_*).
- 3-leg action gate still bounds who can trigger an ANSWER from chat.
