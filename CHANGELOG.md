## 0.1.5 (unreleased)
- Treat explicit `:sync`, `{:sync, metadata}`, and `sync: true` callback returns as strict durability boundaries. Built-in backends first exhaust their bounded transient retry policy; if the write still fails, the DurableServer exits with a structured `{:sync_failed, reason}` fatal-exit reason before acknowledging the callback. Automatic and periodic sync remain best effort for transient failures, while storage conflicts remain fatal.
- Honor the caller-supplied `ensure_started_child/3` timeout while waiting for a live storage owner to finish Group registration, and preserve the caller's remaining overall deadline when sticky placement falls back to a local start instead of applying fresh fixed 5-second waits.
- Prune stale heartbeat cache entries whose node heartbeat objects are absent from a complete storage listing, preventing stopped nodes from remaining in `get_cluster_nodes/1` indefinitely while avoiding pruning after partial or failed listings.
- Add `:heartbeat_future_skew_tolerance_ms` supervisor option (default: `5_000`). Node heartbeats stamped further than this into the future are ignored for liveness decisions instead of being treated as always-fresh, and local heartbeat/watchdog deadlines now use monotonic time so wall-clock (NTP) adjustments no longer stretch or shrink safety windows.
- Reject child keys in the reserved internal `__nodes/` namespace: `start_child/3`, `ensure_started_child/3`, and `rehome_child/3` now raise `ArgumentError` for keys that would collide with node heartbeat storage.
- Enforce `max_children` limits atomically with local capacity reservations so concurrent starts can no longer exceed total or per-module limits. Integer `max_children` is enforced through the same path and returns `{:error, {:capacity_limit, :max_children_total}}` instead of leaking DynamicSupervisor's raw `{:error, :max_children}`; integer values must now be positive (`0` or negative raises `ArgumentError`).
- Terminate servers using `auto_sync: true` when a storage CAS conflict shows the object was replaced by another owner, matching explicit and periodic sync behavior. Previously the stale process logged the conflict and kept serving requests.
- Accept documented callback returns that previously crashed the process: integer timeout actions (e.g. `{:reply, reply, state, 5_000}`) and `{:stop, {:shutdown, :normal}, ...}` stops, which persist `:stopped_graceful` while preserving the exit reason.
- Return `{:error, reason}` from `terminate_and_delete_child/2,3` when the storage delete fails instead of `:ok`, and only delete storage after an owned `:deleting` tombstone is CAS-written so a stale process cannot delete an object claimed by a newer owner.
- Fence crash-time metadata writes to the dying owner so a stale crashing process cannot overwrite a newer owner's status or crash history.
- Release the storage prefix claim when a supervisor stops or fails to start. Supervisors can now be restarted with the same `:prefix` in the same VM; previously the claim leaked until VM restart.
- Bound graceful shutdown with a new `graceful_shutdown_total_timeout_ms` deadline (default: `55_000`) shared by discovery shutdown and all children, while preserving `graceful_shutdown_timeout_ms` as each child's persistence window, and warn when children fail final persistence during shutdown.
- Cap remote placement ERPC calls and remote child timeouts by the caller's remaining deadline instead of issuing fresh fixed per-node timeouts after the budget expired.
- Enable Req's transient retries for heartbeat writes within the heartbeat deadline instead of crashing the supervisor tree on the first retryable HTTP or transport error.
- Recover from discovery task crashes by logging, clearing discovery state, and rescheduling instead of restarting the whole supervisor tree.
- Make mirror fallback promotion create-only (`try_claim`) so promotion can no longer overwrite an object concurrently created in the preferred backend.
- Retry waiting `ensure_started_child/3` callers immediately when the singleflight leader finishes before the waiter registers instead of sleeping until the full timeout, and never run the guarded operation twice when it raises `ArgumentError`.
- Harden persisted metadata decoding with bounded sizes, `:safe` external-term decoding, and per-field validation. Corrupt or unsafe metadata is rejected with `{:error, %ArgumentError{}}` from storage reads instead of being admitted as partially typed data; legacy binary node references remain accepted.
- Parse IAM XML responses with DTDs disabled and a 1 MiB limit to prevent external-entity and entity-expansion attacks from a malicious endpoint.
- Redact secrets from logging: IAM CreateAccessKey bodies (which contain `SecretAccessKey`) are no longer logged, supervisor startup logs omit backend state and `init_info`, and backend errors no longer echo raw options.
- Update Mint to 1.9.3 for the HTTP/1 chunk-size parser security fix (CVE-2026-59249).

## 0.1.4 (2026-06-04)
- Reject explicit local `start_child/3` and `ensure_started_child/3` attempts before spawning a child when the local supervisor is draining.
- Add optional `handle_sync/3` callback invoked inline after successful user state writes.

## 0.1.3 (2026-06-03)
- Return `{:error, {:unreachable, pid}}` from `ensure_started_child/2` when storage shows a live owner but Group metadata is unavailable.
- Avoid an extra storage read on restart paths by reusing preloaded object data.
- Reserve restart ownership during `rehome_child/3` so LifecycleManager cannot claim the child between shutdown and replacement placement.
- Singleflight concurrent `rehome_child/3` calls per key/module to avoid redundant shutdown/start writes.
- Let callers waiting on an active restart claim make one final takeover retry before timing out, so expired orphaned rehome attempts do not require a second external call.
- Handle `:ignore` from user `init/2` in the local `ensure_started_child/3` path instead of raising `CaseClauseError`.

## 0.1.2 (2026-05-14)
- Fix orphan claim logic allowing dueling LifecycleManager claim

## 0.1.1 (2026-04-29) 🚀
- Initial release!
