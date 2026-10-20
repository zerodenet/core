# Exact carrier declaration registration

The canonical `ControlledOutboundStream` declaration specifies the complete
endpoint, upstream binding, demand class and an explicit nonzero physical limit.
It introduces no configuration, FFI field, numeric default or production factory
activation. A physical limit counts live, retiring and Unknown originals, not
healthy lifetime opens. BackgroundCarrier does not grant this capability.

The host constructs `HostResources::new_for_instance` with the actual component
generation and the actual hosted TaskScope's weak identity before building the
protocol. The older component-ID-only constructor rejects these declarations.

During lifecycle start, the protocol supplies its original declaration ordinal,
its sole CarrierOwner's weak registration, and the actual TaskRegistrar through
`ResourceAccess::register_controlled_carrier`. The provider checks the full actual
instance, exact declared cap, original ordinal, same scope and current start
phase. One actual owner cannot cover two declarations, and a declaration cannot
replace its original owner. No second native pool, owner or task map is created.

The pending original registration is installed before calling `bind_declared`.
No host resource or cleanup mutex encloses this call or native/scope observers.
The platform binds its actual cold owner once to the complete descriptor, actual
scope and the provider's original Arc cancellation fence. Rebinding with a
foreign fence is rejected even for otherwise identical descriptions. The host
rechecks phase and binding identity before publishing. A post-bind phase failure
cancels the original host fence outside all resource locks and retains the pending
registration and first original source. Platform preparation/open and its sole
driver check that same fence; cancellation never refunds an Unknown original.

After actual Slot start success and current host/Engine business admission have
been checked by the caller, `registered_carrier` looks up the published binding
using the actual CarrierGrant, not just equal endpoint facts. It checks complete
endpoint/binding/demand equality, actual owner liveness, exact host fence and scope
admission, then rechecks host phase. This lookup grants no session, fabricates no
start success and supplies no transferred-user receipt. Its caller must retain
those separate actual proofs. The native opener's existing Service-only `Some`
rejection remains until the separate Business integration supplies real authority.

Evidence preserves every canonical declaration ordinal. An unregistered carrier
is Unaccounted. Expired Weak ownership, an empty slot list and task completion are
never native closure. Only the actual owner's private generation receipt supplies
`native_generation_closed`. Actual acquisition errors and native close source
Arcs remain bounded and sticky independently of closure, without retaining full
component recipes or resource history. Snapshots are not atomic across owners;
a pending snapshot can require a retry after another branch finishes.

External protocol/transfer users are currently explicitly Unaccounted. Even after
real driver join, explicit native close and peer EOF, this provider's complete
coverage remains false and `shutdown_io` adds an actual unfinished obligation.
It neither drives a duplicate physical shutdown nor counts the actual owner's
native receipt twice. The protocol's sole persistent owner must perform shutdown;
resource evidence and user/task completion remain separate. This is a library
precondition, not M4 or ALL17 qualification.

## Verification handoff

No Cargo command was run by the resources worker. Manual rustfmt and
`git diff --check` are the worker's only executed checks. The parent owns all
compilation, focused runtime tests and broader validation.

Required predecessor commits are the canonical declaration, ScopeIdentity,
carrier registration and complete bind/revocation enforcement. The provider's
regressions live under `component/tests/resource_carrier*` and cover exact
ordinal/instance/cap/scope/phase, foreign prebinding with original source retained,
owner reuse/Weak loss, actual grant lookup and real loopback driver join/native
EOF separated from missing users. Suggested focused parent commands:

```sh
cargo test --locked -p zero-proxy --all-features --lib resource_carrier
cargo test --locked -p zero-proxy --all-features --lib resource_coverage
cargo test --locked -p zero-proxy --all-features --lib resource_files
cargo test --locked -p zero-platform-tokio --all-features --lib controlled_carrier
```

Actual Slot constructor wiring, business demand execution, protocol options and
Factory start registration, complete transferred-user/transport closure and the
accepted-TCP host task/user graph remain separate integration obligations. The
production resource gate remains closed.
