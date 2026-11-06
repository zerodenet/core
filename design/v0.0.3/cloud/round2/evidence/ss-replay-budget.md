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

## UDP continuation: five actual protocol-state units

The same ordinary `ShadowsocksAdapter` owns one `UdpReplayBudget`, shared by its
inbound profile store and outbound transport state. This is distinct from its
TCP2022 salt budget and from native socket, worker, packet and byte budgets.
Production defaults are kernel policy choices, not upstream defaults:

| Config field | Actual counted unit | Default | Release condition |
| --- | --- | --- | --- |
| `ss_udp_replay_max_windows` | One original 1024-byte replay bitmap for a credential/receiver family and wire session | 8192 | Original valid expiry and all same-source peer retirement tokens released |
| `ss_udp_max_senders` | One actual randomized sender ID and monotonically increasing nonce counter | 8192 | Original flow disappears, or original response sender expires after all its peer tokens retire |
| `ss_udp_max_associations` | One responder namespace + credential + wire session/legacy endpoint identity and roaming endpoint record | 8192 | Actual record expiry with no live same-generation peer token, or original responder destruction |
| `ss_udp_max_return_bindings` | One existing proxy-session return-target binding | 65536 | Actual binding expiry after its original peer is no longer live, or original owner destruction |
| `ss_udp_max_authenticated_codecs` | One actually AEAD-authenticated cached inbound codec | 4096 | Actual codec destruction, including existing idle cleanup after its bindings/peer users retire |

Only the receiver bitmap payload at its default ceiling is 8 MiB. This is not an
RSS limit; records, codecs, packets, cryptographic buffers and other owners have
separate costs. A return binding is not an association: one association may use
many targets. Plain and stream ciphers are not counted as AEAD-authenticated
codecs. The native Source uses its existing authenticated association for
replies; its native target queues are not renamed protocol return bindings.

The original receiver and response-sender books are shared by policy/capacity
views. Changing a view cannot reset accepted packet bits, sender ID or nonce
counter. The adapter strongly retains original books; books and their charges
hold weak backreferences, avoiding an ownership cycle. No valid history is
copied, evicted for pressure, or discarded on profile removal. The existing
300-second default and inclusive 61-second minimum replay protection remain.
Shorter views cannot shorten an already protected original deadline.

A real Source passes the already selected `AuthenticatedAssociation` identity
and the same `DatagramPeerRetirement` token into the protocol's existing
association, receiver and response sender. This is observation of the original
peer, not a new receipt, registry or permission. Tokens are deduplicated by same
original registration. Roaming/multiple peers preserve every still-live token;
Drop, socket idleness and a last-peer overwrite do not release this pin.
Unauthenticated trials are not cached as authenticated codecs, and empty trial
book metadata is reclaimed only when no actual codec still references it.

Cold candidate projection validates retained occupancy without installing new
limits. Actual Source/Stream option identities include their original UDP
budget identity and proposed limits. The existing component lifecycle installs
the proposal only at actual start, behind the shared Source/Users StopOld
barrier. Removing all SS consumers still validates retained debt. Configuration
publication, a successful bind, a peer view returning, and security-budget
refund are separate facts.

The legacy SIP003 outbound-only process owner still needs B's actual owner
migration before it can install custom UDP caps from its cold options. Its
adapter default uses the finite same budget; there is no `onReload` live-cap
swap. This is an implementation dependency, not environment qualification.
Native/worker/queued-byte budgets, non-AEAD Bloom state, and the whole kernel
combined budget remain separately tracked in the continuation owner table.

## Legacy Bloom bitmap byte owner

`ss_legacy_replay_max_bytes` is the whole actual Adapter's fixed bitmap payload
budget, with a new kernel policy default of 64 MiB. It is not an RSS limit or an
old/upstream default. `bloomfilter=1.0.14` uses `BitVec<u32>`: server500000 at1e-6
requires3,594,400 bytes for its two original filters; client5000 at1e-15 requires
89,864 bytes. The actual crate's recommended bitmap byte count is rounded to
u32 payload blocks before charging. Hashers, structs and allocator overhead are
separate costs.

The existing original `LegacyReplay` book receives one charge before either
bitmap allocation, including pending/failed initialization. Policy aliases
share that exact membership/rotation book and charge. The Adapter retains the
original books; charge backreferences are weak. A consumer dropping, a profile
removal, a cap shrink or pressure does not erase valid Bloom membership. There
is no timestamp TTL for this legacy algorithm. Existing fixed capacities,
false-positive policies, and natural two-generation clear/reuse remain intact.
No extra maintenance thread or Scope task is created for these pure bitmaps.

Default/Ignore/Detect/Reject retain their actual separate rules. Ignored inbound
salts and Ignore generation do not allocate a filter. Other original generation
paths still allocate and consult their original membership. Finite bitmap
admission can reject a new book; it does not resize a filter or shorten history.

Cold candidate validation checks this retained byte debt even if every SS
consumer is removed. The inert `SsSecurityBudgetLimits` projection now compares
seven separate units. Same-Adapter Source and native Stream options include the
actual Bloom budget identity and byte proposal; their existing actual start
installs it behind the same Source/Users StopOld barrier. Standalone constructors
retain their explicit library ownership. The real Plugin owner must consume the
weak same-budget proposal before child/probe IO, and its SS2022 framing must
share the existing canonical ReplayFactory with the requested TCP salt total;
a UDP-only proposal is not all-SIP security-budget coverage.

Necessary protocol regressions use the actual fixed server/client allocations,
shared policy aliases, retiring histories, rejected allocation/shrink, cold
validation versus start installation, and the unchanged original client5000
natural rotation. The production Source/combined-outbound barrier tests still
require actual wire, Scope and native receipt qualification; a declaration or
successful constructor is not that evidence.
