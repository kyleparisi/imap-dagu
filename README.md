# imap-dagu

A small Go daemon that watches one IMAP INBOX using IDLE and enqueues a Dagu
workflow for each new UID. No webhook server, extra queue, or AI in the listener.
Requires Linux or macOS and an IMAP server supporting IDLE over TLS.

## Run

```sh
go build -o imap-dagu ./cmd/imap-dagu
cp imap-dagu.example.toml imap-dagu.toml
# Edit the account, absolute state path, IMAP settings and Dagu destination.
# Supply IMAP_PASSWORD through your existing secret manager or service environment.
./imap-dagu -config imap-dagu.toml -check
./imap-dagu -config imap-dagu.toml
```

The check authenticates, verifies the server advertises IDLE, and opens INBOX
read-only. It does not enqueue jobs or create state. The daemon never sends
mail, changes flags, downloads message bodies, or uses unread status as a cursor.
There is no silent fallback to polling when IDLE is unavailable.

Run under the same OS user and Dagu home as your scheduler, or grant the
service user access to the same Dagu directories. Copy the example workflow
into that Dagu home's `dags` directory, enable queues in Dagu's `config.yaml`,
and run the Dagu scheduler with the same `DAGU_HOME` environment variable
(e.g. `DAGU_HOME=/var/lib/dagu dagu scheduler`). See `deploy/imap-dagu@.service` for an adaptable
systemd example; its user and paths must match your installation. State and
Dagu directories must exist and be writable by that user before starting it.
Do not run this listener on the Mac merely because the email client lives there;
use an always-on host with access to Dagu.

```yaml
queues:
  enabled: true
```

The configured DAG is a name (letters, digits, hyphens and underscores), not
a YAML file path. The listener inherits Dagu's environment and settings;
`dagu.home` explicitly overrides the home when set. Dagu 2.21.0 is tested.

## Event contract

Each job receives `EVENT_ID`, `ACCOUNT`, `MAILBOX` (`INBOX`), `UIDVALIDITY`, and
`UID` as named Dagu parameters. The run ID is `EVENT_ID`, a stable ID
retaining 192 bits of SHA-256 based on the unique account name, folder, UIDVALIDITY, and UID. Assign a unique
account name to each mailbox; do not reuse it for a different mailbox or server.
An IMAP UID is **not necessarily a Himalaya envelope ID**. Consumers must resolve
or fetch the UID correctly instead of blindly passing it to `himalaya message read`.
Messages can be moved or deleted between detection and processing. This utility
queues references, not durable copies of message bodies.

## Reliability and boundaries

- On first start, skip existing messages using the server's UIDNEXT snapshot.
  Set `backfill = true` before first start to enqueue existing messages instead.
  Editing `backfill` after initialization has no effect.
- Search by UID immediately on connection and after notifications. Notifications
  are hints, so coalesced arrivals are enumerated together. Every ten minutes
  leave IDLE and reconcile once as a safety net for missed events or dead peers.
- Reconnect with bounded exponential backoff and catch up from the saved cursor.
  Transient queue failures retain the cursor and are retried, including when no
  new mail arrives. Ambiguous Dagu records stop for operator attention instead.
- Save the cursor with file sync, atomic rename, and directory sync only after
  Dagu returns a successful enqueue or history proves the run has started. A process lock prevents two
  listeners from sharing the same state directory. Put state on local disk.
- A retry checks the deterministic Dagu run ID. History showing a started or
  completed run prevents a second job after interrupted cursor persistence.
  History saying only `queued` does **not** prove queue acceptance: Dagu 2.21.0
  writes the status before the queue item. The listener stops with its cursor
  intact on this ambiguity, incomplete attempts, canceled/rejected runs, or
  unknown run statuses. See recovery below. Keep Dagu history longer than the
  maximum listener downtime/recovery window. Workflows must still make side
  effects idempotent.
- SIGTERM/SIGINT lets an in-flight enqueue and its recovery check finish, with
  independent 30-second limits, before saving the accepted cursor and exiting.
  Dagu children use a separate process group so terminal Ctrl-C cannot bypass
  this shutdown handling.
  The example systemd unit signals only the listener initially (`KillMode=mixed`)
  and allows 90 seconds to stop. A hard kill, host crash or command timeout can
  still leave Dagu's multi-file enqueue incomplete; ambiguous recovery requires
  operator attention instead of assuming delivery.
- A UIDVALIDITY change, corrupt state, or source/DAG change stops the process
  rather than silently skipping or replaying messages. Stop the service, retain
  the old state for inspection, choose a fresh state directory and first-start
  policy, then restart. Namespace resets may require consumer deduplication.
- One account, INBOX only. No folder events, flag events, SMTP, web UI, plugin
  framework, or integration with Ganclaw internals. Run another instance for a
  second account. Email authentication uses an environment variable, not a
  shell command in the listener config. After reading the configured password,
  the listener removes that variable from its environment so Dagu children do
  not inherit it. Keep real config and secrets private.
- Dagu jobs can outlive this listener. Queue acceptance does not mean the job
  succeeded. Monitor failures and retries through Dagu.

## Recovery after interrupted enqueue

An operator-only error exits with code **78**. The example unit uses
`RestartPreventExitStatus=78` so it stays stopped instead of logging in every
10 seconds. Logs include the event/run ID; the failed UID remains pending in
`state.json`. **Keep the listener state and cursor intact.**

1. Stop the listener. In the same Dagu home, inspect the exact run ID using
   `dagu history email-received --run-id <event-id> --format json`, the Dagu UI,
   and queue membership. History alone cannot distinguish a queued job from a
   queued status whose queue item was never written.
2. If the job is queued or running, let the scheduler process it. Once it has
   started or finished, restart the listener; it recognizes the existing run.
3. If the attempt is incomplete and definitely has no queued or active job,
   back up that exact Dagu run and use the installed Dagu version's supported
   recovery tools to repair/requeue it or remove **only that orphan attempt**.
   Then restart the listener so it can retry the same UID and run ID. An attempt
   may have `dag.json` but no status, making history empty while enqueue reports
   `already exists`. Do not clear all DAG history to work around one orphan.
4. If the run was canceled or rejected, decide whether to reprocess it before
   repairing that specific run. Never advance the cursor merely to silence the
   error. Failed workflow execution is handled by Dagu retries, not mail replay.

This deliberately favors a visible pause over losing an event or running it
twice after an unconfirmed enqueue. Normal successful enqueue does not wait for
the workflow to run. The checks and tests target Dagu 2.21.0. Its [enqueue implementation](https://github.com/dagucloud/dagu/blob/v2.21.0/internal/intake/queue.go)
writes only queued status before publication; queue errors attempt rollback
instead of creating a failed-run status. Hard termination can bypass rollback.

## Development

```sh
gofmt -w cmd internal
go test -race -timeout 45s ./...
go vet ./...
# Optional: installed Dagu, isolated temporary home; no scheduler or real mail.
IMAP_DAGU_INTEGRATION=1 go test -timeout 45s ./internal/dagu
```

Tests cover first-start policy, catch-up and UID ordering, queue failure recovery,
stable IDs, exclusive locking, invalid state, UIDVALIDITY changes, IMAP arrival
notifications, read-only selection, cancellation and BYE during IDLE, stale
command deadlines during long catch-up, graceful enqueue shutdown, and real
Dagu partial-attempt detection. The integration tests use isolated temporary
homes and intentionally remove only fixture queue/status files.
