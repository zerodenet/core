# Controlled native PacketSocket parts

`PacketSocketReceiver::from_granted(grant, egress_control)` consumes an already
owned `GrantedDatagramListener` and returns one non-Clone receiver and one mutable
`PacketSocketSender`. The host retains the original listener Control and
ControlledDatagramOwner. No descriptor, worker, payload copy, receive queue or
additional send budget is created by this conversion.

The sender uses the same Entry/native descriptor, writer mutex, retirement fence
and explicit `max_pending_sends` semaphore as every TX clone from that grant.
Admission charges waiting and in-flight calls until their borrowed payload is
released. Capacity rejects a new call; cancellation returns only its permit.
Dropping a sender or receiver does not revoke or close the listener. The original
host Control wakes all views and obtains the actual native close receipt. An
unconfirmed original remains charged, with its original source, under the existing
Unix/Windows close rules. A rebind or Arc count is not a close receipt.

Receive preserves the existing PacketSocket payload/source-address contract;
send reuses its actual interface ancillary implementation (including Linux
per-message SO_MARK and Darwin's temporary IPv4 interface scope). There is no
public native FD/socket getter or callback accepting the native socket. This API
does not implement receive ECN, GRO, GSO, destination/interface metadata or Quinn
pollers. No metadata request is accepted and silently reduced to a plain packet.

## Selection boundary

The controlled sender shares the original selector's snapshot and no-probe
algorithm. Loopback, wildcard family markers, and a configured physical interface
without active TUN state need no native route probe. Generation is checked again
at actual native submission after readiness suspension. Route decisions requiring
the old native route probe return Unsupported before that probe is called. They
must not use a different route or legacy socket as fallback.

The global/thread-local legacy route probe is not changed here. Its original
sockets require separate declarations, persistent actual ownership and explicit
closure evidence before that path can be admitted to a controlled listener. Its
FDs are not covered by this listener's close receipt. Non-Linux socket marks also
return Unsupported in this new backend because it has no actual platform
leak-guard receipt to verify; the legacy mark identity semantics are unchanged.
There are no new default limits or production consumers.

## Darwin restoration failure

A restoration error wins over the send result, as in the original implementation.
`PacketScopeRestoreFailure` retains both original outcomes: `submitted_bytes()`
means successful local syscall submission, not remote delivery;
`send_error()` and `restoration_error()` expose the original failures. Its outer
io::Error kind is Other even if restoration returned WouldBlock; replay is unsafe.

The controlled sender persists this failure in the original Entry before any
external observer callback and wakes/revokes every view. Subsequent I/O cannot
silently reuse the uncertain native interface policy. The host reads the same
source through `Control::packet_failure()`, including after actual close. This
policy fault is separate from FD close confirmation: native close can succeed
while the original policy error still needs reporting. A successful send followed
by restoration failure is counted as one actual submission plus a policy error.

## Validation

Sibling tests cover actual IPv4/IPv6 native round trips and zero-length packets,
observer counts, retained original source port, shared plain/ancillary TX admission,
actual pending native I/O and writer waits, caller cancellation, native oversize
errors, interface CMSG errors without unmarked replay, generation changes and
host Control close with old views retained. Darwin-only coverage additionally
combines an actual successful zero-length syscall with an actual failed native
scope restore; structured error classification is tested independently.

The exact no-probe negative case creates only one native listener and closes it
explicitly. After compilation, run it serially under a native syscall trace:

```sh
strace -f -e trace=socket,bind,connect,close -o packet-no-probe.raw \
  "$PLATFORM_TEST_BINARY" --exact \
  controlled_datagram::listener::packet::tests::egress::required_native_probe_is_unsupported_without_new_native_attempt \
  --test-threads=1
```

Check the actual original UDP creation/bind and matching successful close; there
must be no route-probe socket/connect. The owner's reservation assertion by itself
does not prove absence of unrelated native FDs. Tests, native traces and Windows
cross-compilation are pending the root task's serial validation; cross-compilation
is not native Windows execution. This library does not activate WireGuard or
change its tasks, history, rollback, grants or listener preparation model.
