# Task handoff playbook

## TUI commands

- `/queue AGENT TASK` — queue a task for the named agent.
- `/tasks` — list tasks and their current status.
- `/task ID` — inspect a task and any recorded result.
- `/reconcile ID` — recover a completed agent receipt for a task awaiting review. This is not a command to blindly rerun work.

## Ambiguous outcomes

A timeout or worker exit does not prove that a tool had no effect. Ambiguous work stays in review until its receipt and effects can be checked. Do not automatically replay it. Reconciliation can recover a completed receipt; if no reliable receipt exists, a human must decide whether to requeue or cancel after checking effects.

## Conceptual local receipt simulation

Use `examples/handoff_fixture.py` with disposable local SQLite storage; the following describes behavior, not a runnable API recipe:

1. Store note `Review Moon handoff` under idempotency key `playbook-note-v1`. The fixture stores the harmless note and its success receipt atomically.
2. Simulate losing the caller's acknowledgment, then close and reopen the fixture against the same database.
3. Look up the key: its durable receipt demonstrates the stored effect survived reopening.
4. Repeat the same key and note: receive the original receipt without another effect.
5. Repeat the key with different text: receive a conflict; the original note is unchanged.

For a queued task, inspect `/task ID` and reconcile a recoverable completed receipt with `/reconcile ID`. A fixture receipt proves only that local note operation, not completion of unrelated task steps. Keep any remaining ambiguity in review.
