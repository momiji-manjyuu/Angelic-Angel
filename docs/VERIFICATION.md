# Verification status and preflight

## Recorded status for this draft

- Source acquired from pinned upstream commit 169a098e2025cc6e41a50fc8d521c483e21d9b6d
- Source edits and static review only
- No Rust compiler, Cargo, or rustfmt was available in the editing cloud workspace
- No upstream code, unit test, dependency build script, or application command was executed
- No dependency download, installation, real X request, subscription registration, or live webhook request ran
- No CI workflow has been added or enabled

Do not describe the tests below as passed. Test definitions are preparation, not evidence of correctness.

## Before execution

Use an approved isolated environment. Review the pinned source and dependencies, approve
execution of this third-party project, and install Rust only from an approved official source.
Never attach production credentials to the test process. The tests use synthetic data,
temporary files, and localhost-only HTTP mocks.

## Commands to run once approved

```sh
cargo fmt --all -- --check
cargo check --locked --all-targets
cargo test --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
```

The sha2 crate was already present in the upstream lockfile; its new direct dependency
entry was added statically. Check that Cargo accepts/regenerates the same lockfile before
claiming locked builds pass.

Fix any formatting, compile, test, or lint failures, rerun all checks against the final commit,
and record exact results and toolchain version. Do not run init/register/listen/unregister
as part of these checks.

## Expected coverage

- Durable enqueue, recovery, duplicate channel/version detection and terminal dedup
- Failed/uncertain filesystem commits, corrupt/symlink/nonprivate storage, bounded capacity
- Private created permissions and unchanged ancestor permissions
- Successful 2xx delivery; 302/400 dead letters; 503 and Retry-After retries
- Stable Idempotency-Key across retries/restarts; attempt exhaustion; timeout/cancellation
- Destination binding and rejection of mismatched or unbound nonempty queues
- Explicit notification type allowlist including missing/wrong-type values
- Redacted Debug/errors; private atomic config save/load; invalid configuration handling

## Additional integration checks still required

1. A mocked AutoPush exchange must prove no ACK is sent before durable enqueue completes
2. Storage failure must cause process failure without any Delivered/NotDelivered ACK
3. Duplicate notifications received during pong wait must still reach durable dedup
4. SIGINT and SIGTERM while fsync or HTTPS delivery is active must preserve recoverability
5. Invalid UAID must produce no register call, new key generation, or X API call
6. A blocked/slow receiver must not block push receive/ping handling
7. Verify actual X notification schema in an explicitly approved brief real-account trial
8. Verify infrastructure egress, DNS/IPv6 behavior, disk retention, and service supervisor
   separately; this application cannot prove those properties

Do not merge or deploy until these gaps and any discovered failures have been addressed.
