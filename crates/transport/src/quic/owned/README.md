# Opt-in owned Quinn libraries

The metadata adapter consumes the actual platform owner and its same-original
unique RX, TX and control grant. Cross-owner numeric identity equality is not
accepted. On composition failure both native ownership and views are returned.
Only the abstract Quinn UDP view escapes; there is no FD, raw socket, RX clone
or caller-native closure. The host retains actual native control and original
main/probe close ledger in MetadataSocketOwner.

RX takes/restores the unique receiver under a short mutex. The outer mutex is
released before native polling, caller wakers or observer callbacks, including
panic/unwind. Concurrent invalid RX polling is rejected rather than acquiring
another receiver. Write pollers consume the platform's caller-supplied finite
quota; failure retains its original source in a failed poller, with no fake
readiness or fallback native socket. No tasks/queues are added by this adapter.

Metadata success consumes the actual prefix once, including Apple's partial
batch result. It never resubmits the full request. Actual native non-WouldBlock
OS errors keep pinned Quinn's nonfatal-drop disposition. Original neutral I/O
observation records no submitted bytes for those errors. One bounded diagnostic
slot retains the first original error Arc and a checked aggregate count with an
explicit overflow indicator, not an infinite error-source history. Locally
rejected admission, metadata, retirement and helper Unsupported errors remain
errors; they cannot silently become plain UDP or legacy fallback success.

This library does not provide the canonical host native grant, accepted-user
budget, FinalMask/port-hop worker ownership, or production activation. Native
close reports are separate from actual endpoint/connection driver joins and
application failure acceptance. Keeping or dropping an abstract view does not
constitute any of these receipts. Existing constructors remain unchanged.
