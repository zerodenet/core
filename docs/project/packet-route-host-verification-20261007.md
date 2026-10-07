# Packet route and host L3 verification (2026-10-07)

## Implemented kernel scope

- Neutral device incarnation facts advance endpoint generation on actual
  replacement; retained linked protocol/I/O resources keep generation when
  direction-scoped client stacks are withdrawn.
- The existing physical-egress reconciler projects preparing, retrying,
  recovered and superseded through endpoint snapshots and state events.
- TUN and protocol raw-IP ingress share one execution-identity allocator;
  separate counters cannot collide in a shared return map.
- Executed native/translated Packet conversations own instance-bound leases.
  Query/list/close use existing Zero API transports and Admin authorization.
  Close cancels the bounded response pump, wakes an idle owner and confirms
  both releases. Concurrent close is rejected; acknowledgement timeout retains
  eventual completion/event delivery.
- Direct can consume a dedicated inherited Linux/macOS TUN descriptor through
  the existing neutral Packet operation. Preparation is inactive until commit;
  replaced readers stop before activation. The host supplies router addresses,
  forwarding, routes and optional NAT/firewall policy.
- Fragmented host returns preserve delivery and mark incomplete role RX bytes
  and packet metrics unavailable through the existing observer.
- Existing traffic observers/leases are reused; Flow usage/quota accounting
  and statistics periods are not reset by path close or device recovery.

## Local test coverage

New coverage is wired into existing test aggregators, without adding a test
binary. It covers:

- Same-ID key replacement and generation isolation; retained-resource direction
  changes; recovery facts and generation changes after three actual local
  WireGuard TCP/UDP payload rounds with egress epoch invalidation.
- Instance/config conflicts, duplicate close, pagination, owning lease and
  response receipts, and real IPC query/acknowledged close.
- Per-conversation response isolation, provisional admission rollback, idle
  owner wakeup and cancellation with a blocked response consumer.
- Neutral host executor bidirectional I/O, source/hop preservation, non-TCP/UDP
  IP forwarding, local ICMP error sources, staged cancellation and cancellation
  of a blocked writer while another path can continue.
- Actual descriptor-adoption rejection for non-TUN descriptors while preserving
  the host's original descriptor; explicit configuration and ingress collision.

The host executor uses an injected local I/O device in these tests. This tests
execution and cancellation, not operating-system forwarding or external
connectivity. Descriptor rejection tests invoke actual platform validation.

## Qualification

Final gate logs and machine-readable timestamps/results:
`/Volumes/tool/tmp/zero-packet-final-gates-20261007/`.

The full suite ran to completion with the user's existing TUN enabled:
09:52:40-11:17:07 CST (5067 seconds). It covered 146 targets:
2375 passed, 19 failed, 155 ignored. This is **not** a green full-suite result.

After that run, only three test expectations/fixtures were corrected:

- The event catalog includes `packet_route.closed`.
- Direct capability expectations reflect the compiled platform backend.
- Statistics reset rejects old generation after a listen-port rebuild or
  restart; a route-only edit preserves generation and permits period reset.

The six affected whole targets were then rerun, preserving the same workspace
all-feature graph and 16 MiB test-thread stack, with
`ZERO_TEST_HOST_IPV4=192.168.0.101`. The TUN remained enabled. All **198 tests
passed, zero failed, one ignored**. This includes actual three-round WireGuard
TCP/UDP recovery, generation changes, passive-peer direction contraction,
shared endpoint roles, roaming, outer relay and real IPC acknowledgement.
Correction run: 11:28:59-11:30:59 CST (120 seconds).
Logs: `/Volumes/tool/tmp/zero-packet-correction-gates-20261007/`.
The full suite was not repeated after these test-only corrections.

| Check | Result |
| --- | --- |
| Workspace format and integration layout | Pass, including after corrections |
| `cargo check --workspace --jobs 2` | Pass |
| Strict workspace/all-target/all-feature Clippy | Pass, repeated after corrections |
| Full unit/integration/doctest gate | Completed; original failures retained above |
| Six affected whole targets | 198 passed, 0 failed, 1 ignored |
| Core with full/WireGuard/status API and no Connector | Pass |
| Linux x86_64 `zero-tun` cross-check | Pass; compilation only |
| `git diff --check` | Pass |

Full-gate source fingerprint (before equals after):
`4a122f6994d886c3e1e54bcb710c42e0a02168367224a3cd47d8746b5acc4daf`.
Correction-gate source fingerprint (before equals after):
`3b626f1b5eb13be11e0893ce2209946f412dad04747365f4ef824eb61c09c2a1`.
Production source fingerprint stayed unchanged across the test corrections:
`aa72eee4998f1a6ff9a5b61bc1e2dd52f89dbebba5d1ce7d07cf2361320a2736`.

### Environment failures still unresolved

Seven failures in the completed full gate remain environment-limited, rather
than being relabeled as passing. System lookup of `localhost` returned
`198.18.0.44`; the host route to that address used `utun5`. Consequently:

- Shared route trace and resolved-IP bypass tests chose the Fake-IP route.
- TCP and UDP ingress resolved-IP recheck tests did not receive loopback IPs.
- Trusted IPv4 fallback tests observed the Fake-IP or
  `tun_ipv4_egress_unavailable`.
- The HTTPS/H2 local-origin test requested `https://localhost:PORT` while its
  server listened on `127.0.0.1`; the intercepted connection was reset.

Nine WireGuard failures were separately explained and removed by the explicit
host fixture: automatic route-based discovery selected TUN address `10.0.0.1`,
which also collided with test tunnel prefixes. The physical interface address
was verified locally before the successful rerun. No client/TUN settings were
changed and no failed test was skipped for that rerun.

Testing also showed process startup delays before the first test-harness line.
A read-only sample showed an incompletely loaded test image and approximately
12 KiB footprint; host swap usage was about 5.5 GiB. These observations do not
establish a runtime performance regression or a definitive OS root cause.

## Remaining acceptance

- Real dedicated host TUN forwarding, host routing/NAT interaction and external
  return-path acceptance are not exercised by the local injected-device tests.
- Physical external network outages, long duration operation, other platform
  backends and platform CI remain separate qualification.
- The descriptor backend is implemented for Linux/macOS only; Windows/mobile
  adoption is unavailable. Strict Packet requires a live backend binding.
- Flow conversion and legacy Direct Echo retain their existing lifecycles;
  they are not new independently managed PacketRoute rows.
- Default storage remains bounded. Closed conversation tombstones expire after
  the existing idle timeout; path close is not a persistent routing policy.
- This work does not alter host network settings or claim WireGuard M5 complete.

Client contract: [Packet route and host L3 control v1](packet-route-host-control-v1.md).
