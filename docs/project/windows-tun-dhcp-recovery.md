# Windows strict routing and DHCP recovery

The Windows strict-route WFP policy permits outbound DHCPv4 UDP 68 -> 67
and DHCPv6 UDP 546 -> 547. Both local and remote ports and the UDP protocol
must match. The exception covers initial broadcast/multicast discovery and
unicast lease renewal, including a newly connected physical adapter whose
gateway and DNS servers are not known yet. It does not permit arbitrary LAN
broadcasts, DNS, TCP, or DHCP server traffic.

These two rules are installed in the existing Zero-owned WFP transaction and
included in the expected filter inventory. Reconciliation repairs older
policies missing them; normal cleanup removes them along with all other
Zero-owned filters. No Windows Firewall profile or third-party filter changes
are needed. The policy only filters outbound authorization, so it does not
introduce an inbound exception.

The triggering Windows report showed repeated Ethernet DHCP failures while
TUN was active and Internet detection recovering seconds after TUN stopped.
A WFP capture confirmed Zero blocked an ordinary LAN broadcast. It did not
retain the original DHCP packet's filter ID: the capture is supporting
evidence, not proof that every historical DHCP failure had the same cause.

Regression coverage checks the generated WFP conditions, rejection of
non-DHCP traffic, address-family binding, and inventory migration. Hosted
Windows zero-tun tests and privileged TUN jobs validate the change without
replacing the user's running binary. Final target-machine acceptance should
capture DHCP acquisition/renewal with TUN active during Wi-Fi/Ethernet
transitions, followed by normal proxy traffic and strict-route blocking checks.
