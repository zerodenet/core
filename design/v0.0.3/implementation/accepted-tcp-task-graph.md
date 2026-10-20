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
