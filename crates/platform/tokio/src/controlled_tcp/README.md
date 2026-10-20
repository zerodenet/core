# Controlled TCP synchronous ownership

The public stream/control and PendingTcpConnect APIs are unchanged. One original
socket is retained; no descriptor clone, background worker or alternate socket
manager is introduced. Ordinary TokioSocket behavior is unchanged.

State linearizes a byte/connect poll against explicit close when it takes the
original Socket. A synchronous SocketLease marks polling and an unconfirmed
sentinel before that take, retaining the last-known local/peer/interface snapshot
from that instant. Getters during a borrow return that snapshot; a pre-connect
snapshot does not claim that a later connect already succeeded. A concurrent or
reentrant close requests cancellation and returns unfinished, never Closed or a
refund. The borrow crosses no await. Its Drop restores the same original object
(including Windows consumed-registration-unknown state) even when polling unwinds.

Polling itself also runs outside the state mutex: a transferred Tokio socket may
already cache an arbitrary old caller RawWaker, whose replacement can run custom
Drop. Two fixed relays retain only weak waiter targets. External Waker clone,
replacement, Drop and wake happen outside state/target locks. A logical guard
coalesces native/coop wakes until the original socket is restored. Relays own no
Shared, caller future, TaskScope or runtime, and allocate no task/queue/history.

The actual original observer lives in Shared. The socket contains only a fixed
internal capture, preserving the existing platform-specific received/sent/error
callbacks, including explicit zero and Pending producing no event. Each serialized
poll drains that one event slot exactly once, restores ownership, then invokes the
external observer without state/observer locks. Its old Arc is likewise dropped
outside those locks. Observer panic leaves the original socket recoverable.

Explicit close has a different linearization: state installs a consumed-unknown
sentinel and closing marker before taking the original. Deregistration and native
close run outside the state mutex. Only an observed success sets closed, before
any external observer destructor. A destructor panic cannot erase that known
native disposition. An unobserved close panic keeps the same Unknown source and
never produces a receipt, even if OS Drop happened to remove the fd. Unix never
reconstructs or retries a possibly reused descriptor. Windows only retries a
known-open original SOCKET; the current retained retry handle is separate from
the first sticky original error Arc. Closing success does not clear that fault.

These are native descriptor facts. They do not prove TLS close_notify, remote
business delivery, users having left, component replacement, or production grants.
