# WireGuard network test failure audit (2026-10-13)

## Conclusion and scope

The eight failures in the Connector workspace gate share one host-local Direct
TCP prerequisite. They do not represent eight independent WireGuard defects.
The installed core is `0.0.3-dev.202610102233` (`71b3db1`), which predates the
macOS PF correction in `ef2b275b`. Its retained strict-route policy still affects
native host-local TCP while newer test processes run.

The current source already contains the required fix:
`zero-tun::route::leak::macos::policy_rules` emits
`pass out quick on lo0 all no state`. The policy and regression-test source
hashes match the previously qualified correction. No additional WireGuard,
Direct dialing, Flow adapter or packet-stack workaround is needed for these
failures. PF rule construction remains in the platform TUN route layer.

## Reproduced failures

The existing all-feature debug test images were replayed serially with
`ZERO_TEST_HOST_IPV4=192.168.0.102` and the installed TUN retained. No test image
was recompiled, assertion relaxed, target changed to loopback or failure skipped.

| Target | Case | Failure with retained old policy | Equivalent source correction |
| --- | --- | --- | --- |
| endpoint_contracts | directions::live_inbound_contraction_preserves_outbound_replies_and_ends_inbound_business | TCP payload timeout | Passed, including direction contraction assertions |
| endpoint_contracts | listening::passive_peer_learns_authenticated_address_and_live_outbound_revocation_preserves_inbound | TCP payload timeout | Passed, including passive peer and live revocation assertions |
| endpoint_contracts | payload::stopping_a_ends_its_existing_business_while_b_keeps_its_tcp_flow | TCP payload timeout | Passed, including resource isolation assertions |
| proxy_wireguard | wireguard_network_recovery::wireguard_tcp_udp_recover_after_network_changes_without_config_reload | TCP payload timeout | Passed, including successive network-generation recovery |
| proxy_wireguard | wireguard_traffic::inbound::bidirectional_resources_attribute_inbound_peer_flow_and_packet_roles | TCP payload timeout | Passed, including active peer accounting and reset |
| wireguard_inbound | linked_wireguard_endpoints_share_socket_and_peer_state_in_both_directions | Direct connect timeout, then client EOF | Passed, including shared listener/peer state and reload |
| wireguard_inbound | wireguard_outer_udp_uses_socks5_to_shadowsocks_relay_group | Direct connect timeout, then client EOF | Passed, including encrypted UDP through the relay chain |
| wireguard_inbound | zero_wireguard_inbound_routes_tcp_and_udp_from_zero_outbound | Direct connect timeout, then client EOF | Passed, including TCP/UDP forwarding and reconciliation |

## Causal verification

1. Independent Python sockets, without Zero or WireGuard, timed out when
   connecting to listeners on `192.168.0.102` and `192.168.34.14`. Both targets
   are assigned host addresses; the local route uses `lo0`. A loopback-source
   control succeeded, but that source substitution was not used for acceptance.
2. The unmodified test images reproduced exactly the same eight failures.
3. A temporary, independent `com.apple/zero_0_tcp_20261013` PF anchor applied only
   the same output-only stateless loopback exemption as the committed fix.
   Native TCP succeeded with the original physical source addresses on both
   interfaces. The same test images then passed all eight cases.
4. Removing only the validation anchor reproduced both native TCP timeouts.
   Cleanup confirmed that the validation rules were absent, the installed core
   instance was unchanged, and the original owner anchor and main PF ruleset
   hashes were unchanged. No host route, global PF state, persistent policy or
   installed core was changed.

| Target | Retained old policy: passed / failed / ignored | Corrected validation policy: passed / failed / ignored |
| --- | --- | --- |
| endpoint_contracts | 22 / 3 / 0 | 25 / 0 / 0 |
| proxy_wireguard | 11 / 2 / 0 | 13 / 0 / 0 |
| wireguard_inbound | 3 / 3 / 1 | 6 / 0 / 1 |

The five macOS PF policy unit tests also passed, including the regression that
requires exactly one stateless output-loopback exemption while preserving the
remaining exceptions and protected-route blocks.

## Qualification boundary

This closes the cause investigation of these eight failures and confirms the
existing source correction using 44 passing integration cases and five policy
unit cases. The ignored live-ICMP test remains separate qualification. This is
a focused replay, not a new complete workspace gate or production acceptance.
The earlier full-gate failure record remains valid for its original environment.

The installed pre-fix core and its original policy remain active after cleanup,
so uncorrected local runs can still reproduce these failures. Runtime adoption
requires a separately authorized installation/restart. This task did not build
a release, switch the active core or introduce runtime source changes.

Private evidence (logs, binary hashes, native controls and cleanup confirmation):
`/Users/higanbana/.codex/tmp/zero-wg-eight-audit-20261013/assessment.json` and
`lease.json` in the same directory. The starting source HEAD was `1086b3d2`.
