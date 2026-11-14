# Task handoff playbook

## TUI commands

- `/queue AGENT TASK` — queue a task for the named agent.
- `/queue-check AGENT CHECK | TASK` — queue a task with a check to perform if its outcome becomes uncertain.
- `/tasks` — list tasks and their current status.
- `/task ID` — inspect a task and any recorded result.
- `/children ID` — find tasks delegated from this task.
- `/review [PARENT]` — list tasks with uncertain outcomes. Supply a parent ID to see only its children.
- `/timeline ID` — see recorded claims, tool calls, replies, and review decisions across attempts.
- `/reconcile ID` — recover a completed agent receipt for a task awaiting review.
- `/resolve ID REVISION ACTION NOTE` — record an explicit resolution after checking the task effects.

For a queued task with zero attempts, use `/resolve ID REVISION cancel NOTE` to withdraw it before delivery. If an agent claims it first, read the new state before taking further action.

## Review a run

Find the parent task with `/tasks`, then open `/task ID` and `/children ID`. Use `/timeline ID` on the relevant child. For example, if Moon asks Finch to extract Markdown headings, the timeline can show that the headings tool returned. Finch's recorded reply contains the heading text. When a task needs review, check its durable receipt with `/reconcile ID` and verify any remaining effects before recording a resolution.

## Ambiguous outcomes

A timeout or worker exit does not prove that a tool had no effect. Ambiguous work stays in review until its receipt and effects can be checked. Do not automatically replay it. Reconciliation can recover a completed receipt; if no reliable receipt exists, record independently verified completion with `verified`, requeue when a retry is warranted, or cancel the task.

## Recovery instruction recorded at submission

For a note task, attach a check that names the effect key `playbook-note-v1`, the expected note `Review Moon handoff`, and the local fixture database. Confirm the stored receipt matches both the key and note. That receipt verifies one accepted note effect. If the receipt is missing, keep the task in review while you verify the database identity and check the stored note. Record an explicit resolution after checking the evidence. An approved retry uses the original effect key and note.

## Conceptual local receipt simulation

Use `examples/handoff_fixture.py` with disposable local SQLite storage; the following describes behavior, not a runnable API recipe:

1. Store note `Review Moon handoff` under idempotency key `playbook-note-v1`. The fixture stores the harmless note and its success receipt atomically.
2. Simulate losing the caller's acknowledgment, then close and reopen the fixture against the same database.
3. Look up the key: its durable receipt demonstrates the stored effect survived reopening.
4. Repeat the same key and note: receive the original receipt without another effect.
5. Repeat the key with different text: receive a conflict; the original note is unchanged.

For a queued task, inspect `/task ID` and reconcile a recoverable completed receipt with `/reconcile ID`. A fixture receipt proves only that local note operation, not completion of unrelated task steps. Keep any remaining ambiguity in review.
