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
and run the Dagu scheduler. See `deploy/imap-dagu@.service` for an adaptable
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
`UID` as named Dagu parameters. The run ID is `EVENT_ID`, a stable ID retaining 192 bits of SHA-256
based on the unique account name, folder, UIDVALIDITY, and UID. Assign a unique
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
  Queue failures retain the cursor and are retried, including when no new mail arrives.
- Save the cursor with file sync, atomic rename, and directory sync only after
  Dagu confirms a run exists or accepts enqueue. A process lock prevents two
  listeners from sharing the same state directory. Put state on local disk.
- A retry checks the deterministic Dagu run ID, preventing a second job after
  a crash between enqueue and state save. This relies on Dagu retaining the run
  record. Keep Dagu history longer than your maximum listener downtime/recovery
  window. At-least-once processing still requires idempotent workflow side effects.
- A UIDVALIDITY change, corrupt state, or source/DAG change stops the process
  rather than silently skipping or replaying messages. Stop the service, retain
  the old state for inspection, choose a fresh state directory and first-start
  policy, then restart. Namespace resets may require consumer deduplication.
- One account, INBOX only. No folder events, flag events, SMTP, web UI, plugin
  framework, or integration with Ganclaw internals. Run another instance for a
  second account. Email authentication uses an environment variable, not a
  shell command in the listener config. Keep real config and secrets private.
- Dagu jobs can outlive this listener. Queue acceptance does not mean the job
  succeeded. Monitor failures and retries through Dagu.

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
notifications, read-only selection, and real Dagu duplicate-run behavior.
