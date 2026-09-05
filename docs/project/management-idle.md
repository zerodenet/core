# Management-only runtime

Zero accepts an empty inbound list with no configured TUN as a management-only
runtime. The application's private IPC server remains available; the proxy
orchestration loop waits for configuration changes or shutdown without creating
an inbound listener. No HTTP status API or TUN is enabled implicitly.

A controller can apply the first inbound using the existing reconciled
`config.apply` transaction. A bind failure leaves the last known good empty
configuration available for retry. Removing the final inbound returns to the
same idle state. Existing listeners and URL-test tasks still fail the runtime
when they terminate unexpectedly; empty startup is not a reason to weaken that
supervision.

This supports desktop first launch before a proxy profile is imported. The
previous `NoInbounds` startup guard allowed IPC to appear briefly and then
terminated the process, despite the configuration passing validation.

Coverage: `tests/management_lifecycle.rs` exercises real IPC, first-listener
SOCKS5 negotiation, return to idle and parent-lifetime EOF. The proxy test
`crates/proxy/tests/management_idle.rs` covers a failed first bind followed by a
successful retry and shutdown.
