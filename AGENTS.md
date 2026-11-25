# Repository Guidelines

## Structure

Root `src/main.rs` builds the `zero` binary and is limited to process initialization, tracing, error exit, and application executor invocation. `src/application/` owns command execution split into run, inspect/status, control IPC, and TUN control responsibilities. Reusable crates live under `crates/`. External protocol implementations live under `protocols/`.

### Crates (dependency bottom-up)

- `zero-traits` — `#![no_std]` runtime-neutral abstractions: protocol traits (`TcpTunnelProtocol`, `UdpRelayProtocol`, `UdpPacketPath`, `DatagramCodec`, etc.), `TransportKind`, `InboundTransport`, address types, async socket/listener traits. No runtime dependency.
- `zero-core` — runtime-neutral domain primitives, request/packet facts and component identities, including `Session`, `Address`, `Error`. Depends on `zero-traits` and remains `no_std`.
- `zero-api` — interface contract types (snapshots, commands, events). Independent.
- `zero-config` — config ADTs (`RuntimeConfig`, `InboundConfig`, `InboundProtocolConfig`, `OutboundProtocolConfig`, `RouteConfig`, etc.), structural/reference validation, and delegation to validation-only protocol APIs for protocol-private values. Depends on `zero-api`, `zero-router`, and the lightweight validation surfaces of relevant protocol crates.
- `zero-router` — rule set matching and route decisions. Depends on `zero-core`.
- `zero-engine` — the control kernel: engine plan, target resolution, session registry, stats, event log, hooks, groups (urltest/selector), outbound health. Depends on `zero-api`, `zero-config`, `zero-core`, `zero-router`.
- `zero-logging` — non-blocking structured logging with event bridge. Depends on `zero-config`.
- `zero-dns` — configurable DNS resolver, caching, fake-IP, routing. Depends on `zero-config`, `zero-platform-tokio`, `zero-traits`.
- `zero-platform-tokio` — tokio runtime implementation of the traits abstractions (`TokioSocket`, `TokioListener`, `TokioDatagramSocket`, `TcpRelayStream`, `ClientStream`, `TransportConnector`). Public runtime collaboration contracts live in `contract`; this crate depends on `zero-traits`, `zero-core` and `zero-error`, never config, engine, proxy or concrete protocols. Core/traits stay `no_std`.
- `zero-transport` — concrete carrier implementations (QUIC, TLS, WebSocket, gRPC, H2, HTTP-Upgrade, split-HTTP, REALITY), neutral I/O wrappers, and the high-level Protocol Runner SDK (`zero_transport::runner`). Protocol-private transport plans remain protocol-owned. It must not import or project `ResolvedLeafOutbound` or name concrete proxy protocol crates; engine-leaf projection belongs to owning proxy adapters. Feature-gated per transport. Depends on `zero-core`, `zero-error`, `zero-platform-tokio` and `zero-traits`, never config, engine, proxy or concrete protocols.
- `zero-tls-legacy-crypto` (`crates/tls-legacy-crypto`) — internal legacy TLS 1.2 CBC/SHA-1 cipher-suite crypto provider used by `rustls` fingerprint profiles.
- `zero-runtime` — common component catalog, resource generations, lifecycle/task ownership, confirmed shared-resource replacement, prepared routing/configuration resources, source persistence/recovery and configuration-operation ownership. Depends on config/core/platform/traits; optional `raw-ip-runtime` also uses the neutral `zero-tun` original provider. Never depends on proxy or concrete protocol runtimes in production. Proxy injects its actual factory registry and publication-receipt type.
- `zero-proxy` — the orchestration layer: `Proxy`, `ProxyHandle`, `RunningProxy`, reload/reconcile, `ProtocolRegistry` capability dispatch, protocol-local inbound listener entrypoints, `serve_inbound` unified TCP pipeline, TCP/UDP dispatch, upstream connect helpers. Depends on runtime/config/core/dns/engine/platform/traits/transport/tun/stack.
- `zero-connector` — controller-neutral event dispatcher and outbound sink delivery (`event-dispatcher`, `sink-jsonl`, `connector` features). Registers webhooks via `api.event_sinks` (`tag`, `url`, `events`, optional `source_id`), sends the `zero.event.v1` envelope, and owns retry/outbox/dead-letter state. It must not prescribe panel/node/user/billing dialects or a second configuration API.
- `zero-grpc` — gRPC control plane adapter (`grpc-api` feature). Optional `proxy-control` consumes the same `ProxyHandle` and `Engine` for borrowed control views without creating a second Host or executor.
- `zero-ffi` — C-compatible FFI bindings.
- `zero-tun` — platform-agnostic TUN device abstraction.
- `zero-stack` — user-space network stack (pure packet parsing/building in `packet.rs`, ICMP Echo translation in `echo_translation`, async TCP termination + UDP forwarding in sibling modules).
- `zero-ztls` — generic TLS 1.3 client with custom ClientHello (extracted from REALITY).

### Protocols

External protocol implementations under `protocols/`: `socks5`, `http`, `vless`, `hysteria2`, `shadowsocks`, `trojan`, `vmess`, `mieru`, `wireguard`. Runtime-neutral surfaces use `zero-core` and `zero-traits` plus protocol crypto. Optional `runtime` features may consume `zero-platform-tokio` contracts and generic `zero-transport` carriers/runners without depending on `zero-config`, `zero-engine`, `zero-proxy` or `zero-runtime`; validation-only consumers must not enable `runtime` modules.

Protocol package versions track the explicitly selected upstream implementation version one-to-one; they do not inherit the Zero product version. Compare feature completeness against that exact upstream tag and immutable commit, keeping lockfile versions, reference docs, interop tooling and CI aligned. RFC-only protocols have no upstream implementation release to mirror.

### Key paths

- `AGENTS.md` — concise engineering guidelines, red lines, and Protocol Runner SDK guide (kept under 24 KB so agent system prompts never truncate it).
- `release/runtime-host-migration-ledger.md` — detailed `RuntimeHost`, original-receipt, and per-protocol ownership/migration ledger.
- `release/breaking-changes.md` — versioned release compatibility ledger.
- `release/resource-configuration.md` — runtime resource-budget configuration audit.
- `docs/` — optional local notes, ignored by Git; never a build or CI dependency.
- `examples/` — example configs, versioned.
- `tests/` — binary-level integration and architecture boundary tests (`tests/workspace_architecture.rs`).
- `crates/proxy/tests/` — proxy-level integration tests (protocol interop, session observability).

## Third-party vendor boundary (red line)

- `vendor/` is strictly frozen to a monotonically shrinking allowlist enforced by `tests/workspace_architecture.rs` (`vendor_directory_and_crates_io_patches_are_frozen_to_shrinking_allowlist`).
- **NEVER** add new crates to `vendor/` or `[patch.crates-io]` to satisfy `RuntimeHost` lifecycle/close receipts, cancellation semantics, memory accounting, or new protocol integration. Adapt third-party resource boundaries externally in `zero-platform-tokio` or `zero-transport::runner` instead.
- **NEVER** place first-party crates under `vendor/`; internal workspace crates belong under `crates/*` or `protocols/*`.
- Whenever an existing vendored dependency is retired, shrink `ALLOWED_VENDOR_DIRS` in `tests/workspace_architecture.rs` and `[patch.crates-io]` in root `Cargo.toml` in the same commit.

## Protocol Runner SDK (`zero-transport::runner`)

To keep protocol crates focused on wire handshakes, framing, and cryptography — and maintain high velocity when integrating new protocols (e.g. TUIC, Juicity, AnyTLS, ShadowTLS, MASQUE, Naive, SSH) — protocols **must** compose `zero_transport::runner` primitives instead of hand-writing `SessionOwner`, `Weak` role fences, 3-way cancellation `select!`, `MetadataPoolOwner`, `OwnedEndpointOwner`, or `CloseReport` state machines:

- **`StatelessOutboundFactory<P, D>`** (`runner/stateless.rs` + `runner/carrier.rs` + `runner/call.rs`):
  - For per-connection outbound protocols (`StreamProtocolHandshake` + optional `DatagramProtocolHandshake`, e.g. Trojan, Shadowsocks, SOCKS5, HTTP, AnyTLS, ShadowTLS).
  - Owns `SessionOwner` admission, dial-policy verification, `GuardedCall` cancellation/deadline racing, `StatelessTransportStack` wrapping (including taskless inline gRPC via `connect_grpc_inline_with_settings` when `task_limit` is `None`), `StreamLoan` (`ExclusiveIo`) carrier lending, and `DatagramLoan` lending.
- **`PooledRoleFactory<P, D>`** (`runner/pooled_role.rs`):
  - For stream-multiplexed outbound roles (`PooledRoleProtocol` + optional `PooledDatagramProtocol`, e.g. Trojan MUX, VMess MUX, VLESS MUX, SSH).
  - Enforces exact `pool_owner` `ComponentInstanceId` binding, retains only `Weak<Options>` and `Weak<Ready<BoundLeaf>>` in cached role handles so revoked roles never pin retired private resources, reports `UpstreamTransport::observe_carrier_origin`, and bridges `StreamLoan` / `DatagramLoan`.
- **`QuicPool<P>`, `QuicPoolFactory<P>`, `QuicPoolRoleFactory<P, D>`** (`runner/quic_pool/`):
  - For QUIC/UDP physical connection pools (`QuicPoolProtocol` + `QuicPoolRoleProtocol` + optional `QuicPoolDatagramProtocol`, e.g. Hysteria2, TUIC, Juicity, MASQUE).
  - Encapsulates `ControlledMetadataCarrier` + `MetadataPoolOwner` registration before fallible CA file reads, `PacketSocket` + `PacketSegment` + `OwnedEndpointOwner` binding, single-flight scoped setup, multi-candidate fallback, DNS/network generation transition fencing, LRU idle reaping, optional outer UDP port hopping, and ordered `CloseReport` / `TaskScope` ACK retirement.
- **`StandardTcpInboundFactory<P>`** (`runner/inbound.rs`):
  - For standard TCP/TLS inbound sources (`StandardTcpInboundProtocol`, e.g. Trojan, VMess, AnyTLS, ShadowTLS).
  - Stages cold credentials in a single-shot mutex so cold validation/drop never mutates live user stores, activates once during host `build()` (after `StopOld`), hashes process-keyed static identities, provides `InboundSourceLimits` / work-limit / TLS file-limit builders, and wires `tcp::source` + `tls::source::wrap`.
- **`LifecycleChain`** (`runner/lifecycle.rs`):
  - Composes multiple `ResourceLifecycle` owners with forward-order `start`, reverse-order `revoke` and `shutdown`, and retry-idempotent receipt retention across partial close deadlines.

## Features & Build

Default build is `--features full,status-api`. Optional features:
- `socks5`, `http`, `mixed`, `vless`, `hysteria2`, `shadowsocks`, `trojan`, `vmess`, `mieru`, `dns`
- `status-api` — runtime control endpoint and selector switching
- `event-dispatcher`, `sink-jsonl`, `connector` — kernel event delivery (independent of `status-api` / `grpc-api`)
- `grpc-api` — gRPC control plane adapter
- `host-network-stats` — read-only host interface statistics on supported platforms

If a config references an uncompiled protocol, it fails early with a clear error.

## Commands

Always use workspace-wide commands by default:
- `cargo fmt --all`
- `cargo check --workspace`
- `RUST_MIN_STACK=16777216 cargo test --workspace --all-features` (full test suite; matches CI feature coverage and 16 MiB test-thread stack)
- `cargo clippy --workspace --all-targets --features full`
- `py scripts/check-test-layout.py` (verifies explicit integration test target registration)
- `cargo build --release`
- `cargo run -- run <config>` — run proxy with given config
- `cargo run -- status [--json] <config>` — show current status

Run a single test: `cargo test <test_name>`

If you change protocol behavior, config parsing, routing, or runtime wiring, run the relevant protocol/proxy/architecture tests and full suite checks.

## Style

- Use `rustfmt` defaults and Conventional Commits (`feat(...)`, `fix(...)`, `refactor(...)`, etc.).
- Keep module/function names `snake_case`, types `CamelCase`, workspace crates `zero-*`.
- Prefer ASCII, keep directory names short, and split `.rs` source files around 300 lines.

## Tests

- Tests live in sibling `tests/` directories (registered in explicit `tests/suites/*.rs` targets when applicable), not inline in production logic files.
- Name tests after the behavior they test; always add/update tests when changing config shape, protocol handling, routing, runtime behavior, or logging.
- Listener fixtures retain the actual bound socket until its worker takes ownership; a shared port-allocation mutex does not reserve endpoints against the OS. Invoke the same host `request_stop` and full observer as production before awaiting native peer retirement.
- Cleanup regressions consume the original native controller's close receipt and verify its transferred view is retired; post-close port availability alone is not proof of handle closure.
- Cancellation regressions establish that the awaited operation is still pending at an explicit I/O suspension point; retain real child spawning, ownership and reaping assertions, and never fabricate successful probes.

## Core Architecture & Ownership Boundaries

Detailed historical edge cases and per-protocol invariants are archived in `release/runtime-host-migration-ledger.md`. The foundational invariants enforced by `tests/workspace_architecture.rs` are:

- **Layering & Responsibility**:
  - `src/main.rs` delegates command execution to `src/application/{control,tun,inspect,run}.rs` and never constructs IPC requests or parses protocols.
  - `crates/config` owns serde ADTs and structural/reference validation, delegating protocol-private validation (UUIDs, ciphers, keys, REALITY fields, credentials) to validation-only features of `protocols/*`. Never duplicate protocol validators or enable protocol `runtime` features from `zero-config`. Mieru shared config ADTs live in `protocols/mieru/config` (`mieru-config`) to avoid feature cycles.
  - `crates/router` owns rule-set matching and route decisions.
  - `crates/engine` owns the control kernel (`runtime.rs` facade delegating to `runtime/{configuration,policy,observability,diagnostics,session,route}.rs`), `direct` / `block` route semantics, groups, stats, and health. Pure route evaluation lives in `engine/src/runtime/route.rs`; `zero-proxy` consumes it and owns async DNS execution without duplicating bypass/mode rules.
  - `crates/stack` keeps pure packet parsing/building in `packet.rs` and bounded ICMP Echo correlation in `echo_translation`; async TCP/UDP connection lifecycles consume `packet.rs` without recreating IP parsing. WireGuard peer demux, auth, allowed-IP checks, and keys stay in `protocols/wireguard` (never dispatch peers by outer UDP source address).
  - `crates/proxy` owns orchestration (`Proxy`, `ProtocolRegistry`, `ProtocolInventory`, `serve_inbound`, TCP/UDP/packet-flow dispatch). `ResolvedLeafOutbound` projection stops strictly at `crates/proxy/src/inventory` and `protocol_registry/registry/outbound` preparation boundaries and never leaks into `adapters/*`, `zero-transport`, or `zero-runtime`.
- **`RuntimeHost` & Lifecycle**:
  - One `zero_runtime::RuntimeHost<A>` owns the component catalog, task owner, serial apply lock (`ProxyHandle::apply_config_and_wait`), and pending acknowledgements. Weak dispatch references must never retain the host or its pools.
  - Replacement strictly follows `StopOld`-before-`BuildNew`: fence/revoke affected owners and transitive consumers, confirm protocol, `TaskScope`, transferred-view, and original native close receipts before building/binding replacements (including port moves), and recover from the last-known-good snapshot on candidate bind failure.
  - Component factories declare explicit `task_limit` (`None` grants zero tasks), install persistent resource owners before awaiting `start`, and never treat `Drop`, timeout, or abort as native close proof.
  - Port conflict detection is authoritative in config validation (`DuplicateInboundListen`); bind-time errors represent external port occupation only.
  - Protocol UDP surfaces live under each protocol's explicit `udp` module (`<protocol>::udp::*`), not protocol crate roots. Protocol-private config strings are unpacked by thin proxy adapters into validated protocol constructors, never parsed inline by generic proxy runtime code.
