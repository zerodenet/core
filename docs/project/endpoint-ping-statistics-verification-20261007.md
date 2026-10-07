# Endpoint local Echo and idle Packet observation - 2026-10-07

## Live findings

The client's application version is not the running kernel version. Read-only
IPC and the installed executable confirmed `v0.0.3-dev.202610060104` / `740424a`,
with two canonical resources, `endpoint:company` and `endpoint:personal`.
Both were running with inbound and outbound directions enabled.

The displayed tuple is active Stream Flow / active Datagram Flow / active
Packet route, not cumulative bytes. A SOCKS CONNECT through the installed
kernel to `192.168.1.235:5432` succeeded without sending database commands.
Company's Stream count changed from 0 to 1 while that socket was open and
returned to 0 after closure. Personal was unaffected. Company already had
nonzero, independently measured inner and outer traffic. Neither resource
had a Packet activity observation at idle; its count was null. The installed
kernel did not recognize the newer `packet_routes` query.

A separate SOCKS CONNECT to the known personal service `16.10.1.2:8000`
succeeded; personal's Stream count changed from 0 to 1 while company remained
at 0. No HTTP application request or configuration change was made.

Company's authenticated peer allows `10.10.0.0/24` and `192.168.0.0/23`;
personal's first two peers have authenticated outer addresses. The known
offline third personal peer remains outside this acceptance scope.

The running kernel and active host TUN were left untouched. These observations
do not constitute acceptance of a newly installed kernel or an external
device's ping.

## Fixes and ownership

- `zero-config` projects canonical `protocol.addresses` into the inbound role.
  Optional legacy inbound addresses preserve old forwarding-only configurations.
  An explicit linked inbound address list must agree with its outbound list.
- `protocols/wireguard` validates addresses through its existing interface
  validation and supplies exact local-IP ownership. AllowedIPs are not a local
  address assignment. Peer authentication and source validation remain protocol
  responsibilities.
- The neutral Raw-IP inbound operation admits local Echo only after protocol
  validation and inbound-direction authorization, ahead of forwarding policy.
  Established outbound replies keep their existing return path. Local requests
  are not swallowed by an unrelated native return channel.
- Carrier revision filtering applies only to queued proxied datagrams. A peer
  watch update must not discard the first live datagram from the direct UDP
  socket. Direct packets are authenticated against current device state; stale
  proxy queues retain their revision and peer checks. Local Echo reload and
  direction tests exercise the first post-update packet without a retry.
- `zero-stack::packet` constructs checksum-valid IPv4/IPv6 Echo replies with
  the original identity and payload. No ICMP Flow plane, WireGuard-specific
  Echo correlation, OS route changes or implicit host service exposure is added.
- Raw-IP meter preparation explicitly declares Packet activity observation.
  An observed idle device reports 0; actual native/translated conversation leases
  increase/decrease the count. Derived Flow packets and local delivery do not
  create forwarding leases. Unobserved legacy resource metrics stay null.

Clients continue to query `endpoints` / `endpoint` and `traffic` using the
existing contracts. Discover `traffic_packet_route_idle_observation_v1` and
`raw_ip_local_echo_v1` in `capabilities.features`; use the separately declared
`packet_route_management_v1` for path queries/control. A zero tuple can coexist
with accumulated bytes or a successful local ping.

## Verification

New cases join the existing `proxy_wireguard`, `stack_packet`, `wireguard_model`
and configuration suites. No new test executable or second statistics system
is introduced. Regression scope covers authenticated IPv4/IPv6 local delivery,
exact address ownership, address reload, forged source rejection, inbound
revocation, active TCP/UDP versus native Packet accounting, lease release and
shared resource deduplication.

The first run used the automatic host fixture, which selected `10.0.0.1` on
active `utun5` (`route get 192.0.2.1`). Host business round trips timed out and
one test generated a duplicate AllowedIPs fixture. Setting the existing
`ZERO_TEST_HOST_IPV4=192.168.0.101` to the actual en0 address, routed locally
through lo0, preserved TUN and made all 25 endpoint-control cases pass.
Initial compilation/test fixture mistakes were corrected before qualification.

Completed gates:

- `proxy_wireguard`: 13 passed, including all three new local-delivery cases.
- `wireguard_inbound`: 5 passed, 1 existing live-ICMP case ignored.
- `endpoint_contracts`: 25 passed with the explicit real-host fixture.
- `stack_packet`: 45 passed; `wireguard_model`: 19 passed.
- `cargo check --workspace --all-features --jobs 4`: exit 0.
- `cargo clippy --workspace --all-targets --all-features --jobs 4`: exit 0,
  no warnings.
- `cargo fmt --all -- --check` and `git diff --check`: exit 0.

Full workspace qualification completed with the existing wrapper:

```sh
ZERO_TEST_HOST_IPV4=192.168.0.101 bash scripts/test-workspace.sh --jobs 4
```

The wrapper runs `RUST_MIN_STACK=16777216 cargo test --workspace --all-features
--no-fail-fast --jobs 4`, including unit, integration and documentation tests.
Start: `2026-10-07T08:01:42Z`; end: `2026-10-07T09:43:46Z`; elapsed: 6124 seconds
(102 minutes 4 seconds); exit: **101**. The original full run recorded **2391
passed, 8 failed, 155 ignored** across 146 result summaries. This is not a
passing full gate. Count result lines, not repeated CI error annotations.

Failure handling:

- One capability-list assertion omitted the two new feature declarations.
  The assertion was updated under the same `raw-ip-runtime` feature gate.
  The complete `proxy_control` target was rerun: **34 passed, 0 failed**.
  Final formatting and workspace all-target/all-feature Clippy checks passed
  again after that test-only change.
- Six Proxy unit failures resolve `localhost` through host DNS and expect
  loopback routing/egress. Read-only `dscacheutil -q host -a name localhost`
  returned `198.18.0.44`, matching the failing direct connection's remote
  address instead of `127.0.0.1`. Four route assertions therefore selected
  fallback/reject rules; two dial assertions observed the Fake-IP address
  or unavailable physical egress.
- One Transport unit test (`https_origin_negotiates_http2_and_streams_response`)
  also connects to `https://localhost` while its server listens on
  `127.0.0.1`; its connection was reset under the same host DNS interference.
  These seven environment-affected tests remain unqualified; host DNS/TUN was
  not changed to hide them.

In the full run, `proxy_wireguard` passed all 13 cases, `wireguard_inbound`
passed 5 with 1 existing ignored case, `endpoint_contracts` passed 25,
`stack_packet` passed 45, and runtime architecture boundaries passed all 188.
Ignored tests require external reference binaries, live ICMP sockets,
privileged/isolated hosts or other explicitly declared acceptance conditions;
ignored documentation examples are also counted. In particular all four
`wireguard_go_interop` cases were ignored because the pinned reference helper
was absent. They are not official interoperability acceptance.

The slow full run included extended executable/documentation startup delays.
Host diagnostics showed high load and disk I/O; the precise cause was not
established. No Rust test, running proxy or host TUN was terminated. Auxiliary
process sampling was stopped rather than leaving diagnostic work running.

Logs are under `/Volumes/tool/tmp/zero-endpoint-ping-stats-20261007/`;
`full.log`, `control-final.log`, `check-final.log`, `clippy-final.log` and
`fmt.log` record the results above.

## External acceptance

After installing a build with these features, an authenticated peer whose
inner source is allowed can ping the exact assigned endpoint IP while inbound
is enabled. Disabling inbound rejects new local Echo requests. The endpoint
inner/outer byte and packet counters should grow; local Echo alone leaves
activity at `0 / 0 / 0`. A real forwarded native Packet conversation should
appear in `packet_routes` and increase the appropriate active route counts.
Installing the build and sending that real external ping are separate from
the local encrypted integration tests.
