# Angelic Angel: durable collection branch

A Rust CLI for receiving Twitter/X Web Push notifications through Mozilla AutoPush.
This fork adds a durable outbox, bounded retries, explicit notification-type filtering,
and safer credential/log handling.

**Experimental draft.** Compilation and runtime checks have not yet been performed.
Read [the verification checklist](docs/VERIFICATION.md) before any real-account trial.
No server, account connection, subscription, or background service is created merely
by checking out this repository.

[日本語の運用手順](docs/OPERATIONS.ja.md)

## Data flow

Registered AutoPush session → decrypt → explicit type allowlist → durable local outbox
→ upstream ACK → independent HTTPS webhook delivery

The outbox is synced before acknowledging an accepted notification. Transient delivery
failures retry; permanent failures and exhausted attempts remain as dead letters.
A crash after downstream acceptance can still cause a duplicate: the receiver must
honor the stable `Idempotency-Key` header.

This collects notifications that X chooses to emit. It is not an exhaustive stream,
historical archive, or guarantee that every post/full text will be received.
Treat all notification contents and URLs as untrusted data, never agent instructions.

## Safety and operating model

- Run only in an explicitly approved, isolated execution environment with reviewed
  egress, storage, and service supervision. This program does **not** enforce network
  location, a VPN kill switch, or IP anonymity.
- `listen` reuses an existing registration. An invalid UAID stops the process instead
  of generating keys, creating a new subscription, or calling X automatically.
- Registration and unregistration are explicit account-changing commands.
- Use a secret manager / protected mounted credential file. Config and pending/dead-letter
  payloads are **not encrypted by this application**. Use encrypted storage and access
  controls where needed.
- Existing config files must be private regular Unix files; new configs use atomic
  mode-0600 writes. Status/debug output never displays cookie prefixes, keys, payloads,
  endpoints, or raw HTTP/WebSocket bodies.
- Cookie command-line arguments have been removed. `init` uses hidden interactive input.
  Never paste cookies into chat, shell arguments, source files, or an issue.
- A listener-only config may omit `[twitter]` after registration. Push keys/session
  material remain sensitive. Re-registration requires an explicit, separate operation.
- Only HTTPS webhooks are accepted. Redirects and environment proxy discovery are disabled.
  The complete destination is bound to the outbox with a private salted fingerprint;
  a destination change requires a fresh outbox.
- Keep config, queue files, credentials, and real notifications out of this public repository.

## Commands

Rust edition 2024 is required. After approving and reviewing execution in the intended
environment, build/test with the commands in [VERIFICATION.md](docs/VERIFICATION.md).
No test is expected to need real X cookies or a public network endpoint.

```sh
# Owner-run credential setup in an existing private directory
angelic-angel --config /protected/config.toml init

# Explicitly creates a Mozilla/X push registration
angelic-angel --config /protected/config.toml register

# Example schema only. Verify the actual type path and values before a real trial.
WEBHOOK_ENDPOINT=https://receiver.example/notifications \
  angelic-angel --config /protected/listener.toml listen \
  --outbox /protected/outbox --type-pointer /type --allow-type tweet

# Redacted registration/config status
angelic-angel --config /protected/listener.toml status

# Durable counts while stopped; an active/stale lock fails closed
angelic-angel queue-status --outbox /protected/outbox
```

The `/type=tweet` example is synthetic, not a claim about the current X notification
schema. The type pointer and exact allowlist are required. Missing/non-string/disallowed
types are intentionally discarded and acknowledged, with a countable redacted log event.
This prevents unknown notification classes from being forwarded by default.

## Durability, delivery, and recovery

- Local filesystem with atomic rename and file/directory fsync is required
- New queue directories use 0700; records use 0600; unsafe paths fail closed
- Deduplication key: AutoPush channel ID + version, retained across restarts
- Defaults: 256 KiB payloads, 10,000 retained records, 12 delivery attempts
- Retry: transport failures, 408/425/429/5xx; exponential jitter up to 15 minutes
- Retry-After: integer seconds or IMF-fixdate; a server delay is a lower bound
- Other non-2xx statuses, including redirects, become dead letters
- Connect/request timeouts: 10/30 seconds
- SIGINT/SIGTERM stop acquisition and await durable operations
- Live redacted queue counts are logged every 30 seconds
- Delivered payloads are cleared; dedup metadata and dead-letter payloads remain
- Disk/retention management is manual. A full/broken queue stops intake without ACK
- A hard crash can leave a lock file. Verify the old process is gone before recovery;
  never remove a live process's lock

See [operations](docs/OPERATIONS.ja.md) for trial, monitoring, and recovery details.

## License and origin

MIT. Based on [sh1ma/Angelic-Angel](https://github.com/sh1ma/Angelic-Angel),
upstream commit `169a098e2025cc6e41a50fc8d521c483e21d9b6d`.
