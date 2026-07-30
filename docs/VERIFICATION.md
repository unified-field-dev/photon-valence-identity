# photon-valence-identity verification

Re-run after code or doc changes. Valence `IdentityFactory` bridge plus local Photon
handler executor — covered by unit + integration tests below.

## Environment

```bash
export CARGO_BUILD_JOBS=1
export CARGO_TARGET_DIR=target-photon-valence-identity
```

## Unit + integration (CI)

```bash
cargo fmt --all --check
cargo clippy --all-targets -- -D warnings
cargo test
```

### TEST_MAP

| Behavior | Level | Happy | Sad | Notes |
|----------|-------|-------|-----|-------|
| `ValenceIdentityFactory::new` / `reconstruct` | unit | System actor → non-empty `Actor::label` | invalid JSON / invalid actor shape / inner `ValenceFactory` fail → `IdentityError::InvalidActor` (+ message) | identity bridge |
| `ProcessValenceFactory::new` / `arc` / `build` | unit | System actor builds `Valence` | — | opaque JSON accepted (shape checks live in identity bridge) |
| `system_valence` / `set_process_valence_factory` | unit + integ | after install / `build_photon_runtime` → usable `Valence` | unset factory → error contains `"not installed"` | process `OnceLock` |
| `HandlerDescriptor::matches_event` | unit | topic + optional key filter match | topic / key mismatch → false | descriptor filter |
| `HandlerRegistry` register / lookup | unit | register → `handlers_for_event` / `handlers_for_topic_key` | empty registry / key mismatch → empty | inventory-backed table |
| `is_durable_descriptor` | unit | `Durable` → true | `Ephemeral` → false | checkpoint gate |
| `initial_subscription_after_seq` | unit | — | ephemeral-only / durable missing checkpoint → `None` | resume `after_seq` |
| `ExecutorHandle::empty` / `abort` | unit | empty abort is idempotent noop | — | drop/abort safety |
| `start_executor` + identity dispatch | integ | publish → handler invocation / typed subscribe | valence build fail → DLQ `identity_build` (error message); handler error → DLQ `handler_error` (no drain) | `integration_tests` + `instrumentation_operations` |
| `build_photon_runtime` | integ | installs factory; `system_valence` works | — | one-call host wiring |
| Photon pub/sub (mem) | integ | publish id / multi-sub / keyed filter / after_seq replay | — | mem backend glue used by executor |

## Notes

- Tests may `unwrap`/`expect`; production paths map failures to typed Photon /
  identity / `anyhow` errors (no ordinary-path unwrap).
- Sad-path assertions check typed variants / DLQ reason fields and message
  content, (stronger than `is_err()` alone).
