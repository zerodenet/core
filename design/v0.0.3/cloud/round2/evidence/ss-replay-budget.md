# SS TCP2022 salt accounting

The ordinary `ShadowsocksAdapter` owns one TCP2022 salt budget shared by its
outbound history and all inbound profiles. `runtime.ss_tcp_replay_max_salts`
defaults to 500000 entries for that entire actual adapter, allocated on demand.
This is not 500000 per profile and is not a budget for all Shadowsocks security
state. Existing UDP session/replay windows and legacy rotating Bloom filters
retain their separate rules and remain separate production budget work.

Each accepted TCP2022 salt reserves one entry before insertion into its original
exact history. All policy/capacity views of that history share membership and one
maintenance owner. Existing configured local capacity groups remain bounded by
`StateLimits.tcp_replay_capacity`; omitting that local field cannot bypass the
adapter total. TCP2022 duplicate checks remain mandatory under every policy;
legacy Default/Ignore/Detect/Reject behavior is preserved.

A failed handshake, stream/view return, recipe replacement, worker shutdown or
profile retirement does not refund valid history. The budget strongly retains
the original salt book, with a weak back-reference, until each entry's unchanged
61-second security window expires. Pressure never evicts a valid salt. Expired
entries are physically removed from that same book before refund, and empty
unreferenced books can then retire. There is no copied membership cache.

A cold candidate validates a proposed total without changing current admission.
The real replay owner installs it only after transferring/joining the original
legacy worker, registering the actual Scope task and establishing readiness.
A total below still-valid occupancy is rejected. A runtime-only total change
must replace actual affected source/consumer owners before new admission.
Independent adapter states have different canonical identities; temporary cap
wrappers are not resource identity.

Raw protocol-library `ReplaySaltPool::new`, `ShadowsocksTransportState::default`
and `ShadowsocksInboundProfileStore::default` retain genuine standalone,
unbounded responsibility. Ordinary Proxy constructs finite shared-budget state;
it does not select those standalone defaults to bypass production accounting.
Qualifying legacy transport/process callers and their worker-retirement barriers
is tracked separately and must use that same adapter state.
