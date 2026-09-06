# Configuration validation and runtime state

`zero validate CONFIG` checks configuration structure, protocol values and compiled
protocol support, the engine target plan, file-backed routing resources, DNS
routing, and DNS backend/address-pool construction. Relative files are resolved
from the original configuration directory.

Validation does not construct an engine, acquire a Fake-IP persistence lease,
read or repair principal quota state, bind a listener, or start TUN. It can run
while the production kernel owns those resources. Success establishes that the
configuration is valid for this build; it does not promise network reachability,
available ports, permissions, or compatible persisted runtime data. Persistent
Connector/quota inspection remains the separate `connector state` command.

The previous CLI implementation constructed `Engine` and `Proxy`, which acquired
the running kernel's Fake-IP lease. An installer correctly validating before
stopping the old kernel therefore failed with `already owned by another Zero
process`. Removing the lock or stopping the production process is not a fix.

Clients probing older kernels should set `ZERO_DNS_STATE_DIR` only on the probe
child to a private temporary directory, retaining it until the process is reaped.
This isolates legacy Fake-IP persistence without moving the configuration and
breaking relative resources. It is a compatibility measure for Fake-IP, not a
general guarantee that older kernels have side-effect-free validation.

`tests/validate_isolation.rs` covers a live kernel holding the same Fake-IP lease,
occupied listener ports, untouched quota state, relative route files, and invalid
DNS/routing configuration. The platform CI runs it on Linux, macOS, and Windows.
