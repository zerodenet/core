# Accepted TCP through the existing socket facade

TokioSocket now stores either its ordinary Tokio native stream or a boxed unique
AcceptedTcpStream. The latter is the exact byte view returned by the listener's
persistent accepted entry. Adaptation performs no native create, clone, rebind,
getter or control extraction. The original plain socket retained inside that
control never contains this outer accepted facade, avoiding a retention cycle.

The existing socket/ClientStream/AsyncSocket and Tokio I/O interfaces remain the
protocol entry point. Local/peer and effective prelude addresses are preserved;
observation is applied once at the outer socket boundary. Accepted peek borrows
the original through the existing short SocketLease, uses its fixed weak relay
and cancellation token, and performs native I/O without holding the state mutex.
It consumes no protocol bytes and creates no task or background receiver.

Rust raw conversion is fallible: `TokioSocket::into_inner` returns the original
facade intact when it contains an accepted view. `into_controlled` and
`ControlledTcpStream::new` similarly reject an already controlled facade, rather
than constructing a second owner. Ordinary native fixtures explicitly unwrap
success while retaining their original descriptor, nodelay and policy assertions.
The Hysteria2 website accepts its existing generic I/O directly instead of
extracting a raw stream. No wire/config/FFI/default budget is changed.

Crate-private native-only operations explicitly reject accepted storage. A view
Drop/cancellation still cannot issue a native receipt; only the same persistent
listener entry's concrete control can close its original. Effective address and
cached metadata success is not a live-native or closure proof.

Two additional real loopback regression sources verify bytes, nonconsuming peek,
observation counts, same metadata, both forbidden conversions retaining the actual
view, pending-peek cancellation, Drop without peer EOF, and explicit same-entry
native close followed by real peer EOF. The previous accepted/control regressions
are retained. No Cargo or runtime tests were run by this worker; only rustfmt and
`git diff --check` were executed. Parent validation must include the platform
library tests, original controlled_tcp integration tests and Windows cross-check.

This is the concrete I/O migration seam for the next same-Scope handler and
transport-user graph integration. It does not register handler tasks, establish a
new accepted budget, prove detached transport drivers joined, or activate an
unqualified production path. The actual accepted/provider users remain
Unaccounted until those obligations are wired to the same entry.
