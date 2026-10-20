# Controlled gRPC pool library

`NativePoolOptions::from_pool` binds the caller's existing `GrpcPool`, the full
TLS/gRPC/dial settings, and independent explicit nonzero physical-carrier and
logical-stream limits. `NativePoolFactory` is roleless and declares
`BackgroundCarrier` plus one possible scoped task per physical carrier. One complete cold recipe is retained in that actual pool state; a checked
process-local opaque recipe ID cannot be reused or mistaken for a host identity.
Cold canonical equality cannot silently conflate conflicting full settings or
budgets; live matches additionally compare full values and actual state identity.
There are no production defaults or registrations. The initial provider requires a
literal endpoint and an exact empty-prefix background binding. CA files,
DNS-backed ECH and unsupported TLS backends require their real resource owners.

Build remains cold. Start atomically claims the actual pool before enabling its
finite carrier owner. A pool that was entered by a legacy opener cannot be
adopted, even if that attempt failed; failed candidate shutdown must leave its
legacy cache alone. Native and legacy calls never run parallel executors over
the same pool. Retirement is sticky for that actual pool generation.

Each native reservation installs its opaque adoption handle in a persistent
record before synchronous socket preparation or an await. The original control
is installed by the native provider. Preparation preserves TLS authentication,
ALPN/application settings and original H2 options, then atomically registers the
same tagged H2 driver in the declared TaskScope. That one driver polls the actual
H2/keepalive future and bounded logical read/write futures. No per-logical-stream
Tokio tasks are created. The legacy executor uses the same prepared futures.
Keepalive timeout and ping errors retain their original error objects.

Each demand supplies a fresh cancellation token, deadline and full binding.
These are admission/preparation inputs; the pool never retains the first
business request's context or Session. An admitted logical stream retains its
ordinary half-close and grpc-status behavior. gRPC carrier establishment makes
no protocol destination-acceptance or remote-delivery claim.

Pending, live and retiring logical streams share the explicit logical limit.
Normal admissions release only after the application handle is gone and both
actual workers have returned. Abrupt worker destruction needs the matching
physical-close receipt. Physical records release only after the real Scope
consumes the registered task exit and the same original native control confirms
closure. Body completion, cancellation, Drop, abort or a guessed count cannot
replace either proof. Acquisition with an unobserved result remains Unknown and
occupies its finite physical slot. Source wrappers retain the same actual
scoped failure Arc. Protocol cleanup does not duplicate Scope task IDs.

This is a library owner cut. Production qualification for resourceful gRPC/H2
remains gated pending host resource planning and confirmed replacement. Relay
prefixes, hostname DNS and their per-demand/background grants are not supported
by this direct library provider. Tests use actual loopback TCP, TLS, H2 and the
original gRPC framing; only the parent integration tree runs Cargo validation.
