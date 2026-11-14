# Task handoff playbook

## TUI commands

- `/queue AGENT TASK` — queue a task for the named agent.
- `/queue-check AGENT CHECK | TASK` — queue a task with a check to perform if its outcome becomes uncertain.
- `/tasks` — list tasks and their current status.
- `/handoff` — see work needing review, work in progress, queued tasks, and recent replies. Each entry shows the last recorded activity, the active recipient if registered, and whether there is a recovery check or linked evidence.
- `/task ID` — inspect a task and any recorded result.
- `/children ID` — find tasks delegated from this task.
- `/review [PARENT]` — list tasks with uncertain outcomes. Supply a parent ID to see only its children.
- `/timeline ID` — see recorded claims, tool calls, replies, and review decisions across attempts.
- `/reconcile ID` — recover a completed agent receipt for a task awaiting review.
- `/resolve ID REVISION ACTION NOTE` — record an explicit resolution after checking the task effects.

For a queued task with zero attempts, use `/resolve ID REVISION cancel NOTE` to withdraw it before delivery. If an agent claims it first, read the new state before taking further action.

## Review a run

Find the parent task with `/tasks`, then open `/task ID` and `/children ID`. Use `/timeline ID` on the relevant child. For example, if Moon asks Finch to extract Markdown headings, the timeline can show that the headings tool returned. Finch's recorded reply contains the heading text. When a task needs review, check its durable receipt with `/reconcile ID` and verify any remaining effects before recording a resolution.

For a visual check, follow the `/evidence OWNER KEY` reference shown by `/task ID`. The evidence view gives the observation and its screenshot digest. Use `/save-image OWNER KEY PATH` to inspect the screenshot locally. For example, a screenshot showing `Saved: Semantic handoff` can verify what the fixture displayed. Record that observation with `/resolve ID REVISION verified NOTE` when the task only asked for a visible confirmation.

## Ambiguous outcomes

A timeout or worker exit does not prove that a tool had no effect. Ambiguous work stays in review until its receipt and effects can be checked. Do not automatically replay it. Reconciliation can recover a completed receipt; if no reliable receipt exists, record independently verified completion with `verified`, requeue when a retry is warranted, or cancel the task.

A requeue note is included in the recipient's next delivery. Use it to say what you verified and which actions still need to run.

## Recover a note after a lost acknowledgment

Suppose Tide has write access to the disposable fixture at `/tmp/glassmoon-handoff/notes.sqlite`. Record the absolute database path, expected text, and effect key when assigning the work.

```text
/queue-check tide In /tmp/glassmoon-handoff/notes.sqlite, check the receipt for key playbook-note-v1 and note Review Moon handoff. | Use the local HandoffFixture to store Review Moon handoff under key playbook-note-v1 in /tmp/glassmoon-handoff/notes.sqlite.
```

1. Open `/task ID` after the lease expires. Read the saved check and note the revision. `/timeline ID` can confirm that delivery reached a tool call before the interruption.
2. Run `/reconcile ID`. A saved, completed agent reply can settle the delivery. If the agent only saved a start receipt, or there is no receipt, check the fixture before making a decision.
3. Open the exact database from the check in read-only mode. Verify that its receipt matches the key and note.

   ```sh
   uv run --locked python - <<'PY'
   from contextlib import closing
   import sqlite3

   path = 'file:/tmp/glassmoon-handoff/notes.sqlite?mode=ro'
   with closing(sqlite3.connect(path, uri=True)) as db:
       rows = db.execute(
           'SELECT receipt_id, note FROM notes WHERE idempotency_key=?',
           ('playbook-note-v1',),
       ).fetchall()
   assert len(rows) == 1 and rows[0][1] == 'Review Moon handoff', rows
   print(f'Confirmed fixture receipt {rows[0][0]}')
   PY
   ```

4. Reopen `/task ID` for its current revision. If the task is still in review and that note was its only requested effect, use `/resolve ID REVISION verified Confirmed fixture receipt N and matching note`. If the database cannot be opened or the receipt differs, leave the task in review while investigating. An approved retry uses the original effect key and note.

From the repository, the following test simulates losing the acknowledgment after the fixture commits. It checks for one stored effect, closes the mailbox task from review, and confirms there is no second delivery.

```sh
uv run --locked python -m unittest discover -s tests -p test_mailbox.py -k verified_external_effect
```
