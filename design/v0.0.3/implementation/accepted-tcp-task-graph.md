# Same accepted TCP entry, task joins and native closure

This library opt-in extends the existing ControlledTcpListenerOwner and its
original bounded accepted-entry table. It creates no manager, global task map,
actor, extra task, FD clone, configuration field or numeric default. Both the
accepted-original cap and each entry's task cap are explicit NonZero inputs.
The host must separately reserve their checked total in the actual Scope before
binding. Bind-attempt, UDP-peer and existing Scope-one budgets grant no children.
Production activation remains closed until those real caller budgets and all
selected handler/transport users have been integrated.

`new_with_accepted_graph` freezes the original Registrar's weak identity and
WeakTaskRegistrar. `AcceptedTcp::spawn_handler` requires that same actual Scope;
descendants register only through the same entry's cached weak authority. Every
actual TaskId and join witness is installed before the one admission gate permits
the original task future to poll. The entry retains no strong Scope, protocol
future or hidden JoinSet. Cancelled admission and panics retain the native owner.
Foreign registrar, exhausted explicit task limit and escaped `into_parts` paths
cannot fabricate a task graph. An explicitly selected legacy user must call
`mark_unaccounted`; an unported driver cannot silently inherit a completed graph.

The unique accepted byte view records actual view return independently of tasks.
Its Drop requests/releases view obligations, never TaskJoined or native closure.
A close request seals the graph and cancels the same original entry, then waits
for the original Scope's real join consumer. Only after registered users have
joined and the unique byte view has returned does the owner explicitly close its
same original TCP control. An opaque AcceptedTcpClosed matches only that same
entry. Native-only AcceptedTcpNativeClosed stays separate and cannot prove task
or transfer completion. Interrupted waits preserve the owner, joins and counts.

The owning AcceptedTcpTaskReceiver is weak access to this same entry table.
It selects the already installed witness and reads its genuine consumed exit;
an arbitrary ID, completed body, Drop or Scope disappearance supplies no receipt.
Before locally accepting an Auxiliary client rejection it retains the actual
TaskJoined, including original ID, class and error Arc, in a bounded first-fault
record. Hosted local error delivery does not acknowledge the host's independent
non-cloneable receipt or refund Scope credit. Critical exits remain critical.
Ordinary Scope delivery retains its existing semantics. Joined task entries can
then be reclaimed, so the per-entry cap is concurrent/retiring occupancy rather
than a lifetime spawn count. The first caller failure survives accepted-slot
reuse, independently of native faults and physical closure.

`AcceptedTcpTasks::caller` is the only constructor for opaque AcceptedTcpCaller.
Call it inside the actual task's first protocol poll, after the admission gate;
its actual ID/witness is already installed then. The caller holds weak entry
identity and the exact original witness, with no witness setter or strong Scope
upgrade. check_open checks the original cancel, registration and sealed state.
The witness remains readable after cancellation for genuine join observation.
It is ingress identity, not permission to access an unrelated outbound Scope.
Runtime admission and Engine current-plan validation remain separate authority.
MUX and unported paths must not copy a primary caller identity onto other users.

The source regressions exercise real loopback I/O, pending cancellation before
Scope join, peer EOF only after explicit original close, foreign Scope rejection,
explicit task capacity, one native slot reused for 65 connections, 65 joined
child tasks within one live connection, and an invalid client whose original
error is retained while that same listener accepts a healthy next client.
They preserve the prior native-only accepted tests. These additions are not a
claim that the production handler, multiplexers and all transport tasks have
already migrated. Their owning host receiver and runtime integration follow in
a separate dependent cut; missing selected users remain Unaccounted.

Worker checks: manual rustfmt and git diff --check only. No Cargo or test runtime
was executed by this worker. Parent-only suggested commands:

```sh
cargo test --locked -p zero-platform-tokio --all-features --lib controlled_listener::accepted
cargo test --locked -p zero-platform-tokio --all-features --lib contract::scope
cargo clippy --locked -p zero-platform-tokio --all-features --all-targets -- -D warnings
```

## Actual host optional path

The canonical TcpListener now has optional explicit AcceptedTcpLimits. All old
callers pass None; no configuration/FFI field or production numeric default was
added. The actual pre-bind reservation checks multiplication and addition of the
child task ceiling before constructing its original Scope. When Some is present,
its original NativeTcpGrant constructs the same controlled owner with that Scope's
weak authority and installs the weak task receiver before bind. The general
HostResources provider lacks this actual handler Scope and explicitly rejects
Some rather than silently issuing an unbounded accepted view.

The owned listener loop accepts only this controlled view, registers each
handler in the actual Scope, installs the opaque caller at its first poll, and
waits for the genuine join plus owning-receiver delivery before driving the same
native control. Its bounded FuturesUnordered holds wait futures, not new tasks
or another native table. The registry's persistent entry still owns everything
if this outer future is cancelled. Real exit delivery retains the first original
client TaskJoined independently from fatal listener/resource faults. Auxiliary
client rejection does not stop the listener; critical or unrelated exits retain
the original fatal semantics. Joined healthy handler IDs do not build history.

The plain-handler regression uses the actual registry reservation, explicit
canonical cap, NativeTcpGrant, original Scope observer and owned listener loop.
It sends an invalid real byte, confirms original accepted socket EOF, then sends
a valid byte on a new connection to the same listener. Shutdown separately
asserts three real joins, listener/child native receipt counts, exact retained
client-error Arc/class, no fatal listener failure and actual port rebinding.

Generic protocol dispatch remains conservatively Unaccounted because it may
create transport/mux users before returning a runtime context. This optional
path does not declare an arbitrary generic future to be single-task. The audited
plain fixture supplies an explicit PrimaryOnly selection; every selected
production transport, multiplexer and other derived user still needs migration
to the same entry's task authority before its gap can be cleared. This is not
final production activation, default calibration, or ALL17 qualification.

Additional parent-only focus:

```sh
cargo test --locked -p zero-proxy --all-features --lib rejected_plain_client_retains_actual_source_without_stopping_same_owned_listener
cargo test --locked -p zero-proxy --all-features --lib runtime::inbound_owners
cargo test --locked -p zero-proxy --all-features --lib runtime::listener_loop
```

The accepted handler now installs its existing weak AcceptedTcpTasks directly
in the private inbound route runtime at first poll. Caller identity remains
separate and cannot upgrade into a registrar. The generic logged operation still
selects Unaccounted. Its same implementation accepts a crate-private explicit
user plan; only the actual fixed-target Direct ordinary TCP source selects the
audited single-primary chain. UDP combinations retain Unaccounted. This selects
no capacity/default and does not prove closure itself: the original byte view,
actual owning receiver delivery and explicit original native close remain
mandatory. New source regression uses the actual Direct factory/registry/driver,
a real bound-not-listening upstream refusal, a subsequent healthy exchange and
pending IO, actual original-source retention and native address reuse before
whole-owner terminal confirmation. It is source-only until root runtime tests.
TLS/WS selected protocol branches and H2/MUX drivers still need actual task
propagation before their detached constructors; this cut does not classify them
from generic R or a completed handshake.
