# Native plugin lifecycle ownership

Status: native production wiring implemented in source on 2026-11-16; not
compiled, tested or runtime-qualified. Overall completion is not asserted.

Keep the configured native plugin command, arguments, environment, readiness,
TCP/UDP behavior and existing capabilities. This refactor does not require a VM,
a different operating-system executable, Apple Developer enrollment or Endpoint
Security authorization. Windows and Linux retain their existing native backends.

The migration must retain the actual Child before readiness can suspend, track
its real termination/reaping outcome, and supervise work in the original Host
Scope. Stream/Datagram view return, original socket closure, accepted work and
Scope joins remain independent obligations. Interrupted readiness or cleanup
must retain those actual owners; cancellation, timeout and Drop are requests,
not successful close receipts.

Resource ownership and whole-system process containment have different scopes.
Reaping the actual direct child must not be reported as proof that arbitrary
self-daemonizing descendants have exited. Preserve that honest limitation while
migrating the existing native behavior. Do not fabricate ProcessTreeClosed,
weaken existing receipt types, or activate an unsupported provider just by
changing a catalog gate. Each platform keeps its actual native proof type; a shared host interface
does not make a direct-child receipt equivalent to a contained-tree receipt.

The platform `owned_process::unix_child` backend now stores the real Child
synchronously before readiness, retains cancelled waits and typed failures, and
returns ProcessChildClosed only after actual reaping, derived endpoint cleanup
and the original actor join. Existing Linux and Windows backends still return
their distinct ProcessTreeClosed; macOS never constructs or aliases that type.

The existing ResourceAccess/ComponentFactory/ProcessEndpointHandoff pipeline now
uses this backend for native macOS SIP003/SIP003U inbound and outbound operation.
Standard Stream/Datagram borrowing and native DNS detours use those same current
process generations and original controllers. Windows/Linux retain their native
backends. Executable protocol claimers, SS socket TCP/UDP executors, the private
Datagram resume manager and recursive compatibility relay/stream facade were
removed after their supported native callers selected real standard factories.

Process budget configuration now counts macOS plugin children and TUN helper
graphs in one actual root. It includes pending and retiring entries and requires
native closure plus original Scope acknowledgement before reuse. No new config
shape, VM, binary mapping or Apple authorization requirement is introduced.

Regression sources cover pending actual Child waits, terminal spawn failures,
original receipts, concurrent monitor/cleanup waits, shared N1 helper/plugin
admission, native SIP cross-protocol
borrowing, TCP half-close, UDP, MUX and native DNS detours. Only source/format
checks are performed in this batch; compilation, test execution and low-resource
runtime qualification remain outstanding. The broader resource-configuration
audit is not marked complete. TEMP-WG runtime acceptance stays deferred.

Source inventory found uses outside config for all 112 effective runtime option
getters. This locates consumers only; it does not qualify every allocation,
cancellation, retirement and configuration transaction. The unified Process
root and retired compatibility executors above are concrete changes, while
full resource-domain closure remains open.
