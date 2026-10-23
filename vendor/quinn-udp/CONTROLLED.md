# Controlled native integration

This directory retains the crates.io `quinn-udp` **0.5.16** package, its original
Cargo metadata, features, licenses and ancillary algorithms. Original `.crate`
SHA-256: `af66907df18639dcf4db56ca65490cabc4b27a97dbadd96f2926cca73298f016`.
The root patch selects this exact version; no Quinn runtime/protocol dependency
is added to the platform crate.

The opt-in additions are `controlled::{NativeProbes, ProbeKind, SendOutcome}`,
`UdpSocketState::new_with_owned_probes` and `try_send_outcome`. The caller must
install every original main/probe socket in a persistent native owner before
calling these methods. The helper borrows that original and never creates,
clones, registers or closes sockets in these opt-in methods. Capability tests
use the same native options and successful values as the pinned implementation.
A probe capacity, acquisition or unconfirmed-close failure is an error, never a
fabricated unsupported capability. Windows resolves WSARecvMsg on the supplied
provider's original SOCKET rather than creating its legacy query socket.

Existing `new`, `send`, `try_send` and `recv` remain available with their pinned
behavior. In particular legacy `send` logs/drops non-WouldBlock errors. The new
outcome method exposes actual native errors and the successfully submitted
byte/datagram prefix (including Apple partial batches). It does not acknowledge
remote delivery. Its explicit ECN request is not silently stripped after a
native EINVAL. No native capability, budget default or production activation
is introduced by these hooks.

The platform wrapper exposes its own neutral metadata and retains all ownership,
probe closure, reservation and poller accounting. It must not export the native
borrow, raw handle, `UdpSockRef`, or an arbitrary socket callback to consumers.
