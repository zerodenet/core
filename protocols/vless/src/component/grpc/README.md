# Explicit managed gRPC library recipe

`PreparedGrpc::from_options_refs` preserves protocol-owned validation and the
existing `VlessOutboundLeaf` transport preparation. It obtains the actual pool
already selected by the original `share_xhttp_pool` key, including source
context, full TLS/gRPC profiles and other transport identity. It neither formats
a second key nor allocates a second pool. The runtime must actually retain that
pool: an uncached legacy overflow fallback is rejected for this native recipe.
Only ordinary literal direct gRPC, optionally with its supported TLS profile, is
admitted. MUX/XUDP, Vision, REALITY, encryption, CA file/DNS-ECH resource gaps and
other carriers retain their existing paths. Caller limits are explicit, with no
production defaults.

The host cold allocator canonicalizes `pool_options()` with the roleless
`PoolFactory`, then binds the resulting real ComponentId into the Stream recipe.
Build checks its one actual dependency; start checks the matching live resource
generation and complete recipe. The original runtime reload retires the actual
pool, so retained recipes and consumers reject it. Dropping the runtime before
owner construction leaves no alternative pool to acquire. Shared-resource
preparation failure still invalidates the host's whole cold pass.

A Stream connect passes its current full binding and finite deadline/cancellation
to the shared owner. The persistent carrier uses the real owner's background
origin without a business Session. VLESS request submission, deferred response
validation, observer facts, protocol bytes and Vision handling are shared with
the existing ordinary Stream implementation; gRPC carrier readiness is never
reported as VLESS peer acceptance. The gRPC owner alone owns its physical tasks
and sockets. Closing a role does not close the shared pool.

The protocol library tests perform real TCP and TLS/H2/gRPC wire exchanges. A
standard Service Stream and a second actual UDP wire exchange use one original
pool, one registered physical driver and one matching native-close receipt.
The UDP case exercises the existing carrier and protocol wire; it does not
register or claim a completed native Datagram role. The proxy test only proves
cold canonical allocator/graph behavior. Production `native_input` and bare
`Options` still exclude gRPC/H2 until real host replacement/resource admission
supports this owner. Relay prefix ownership remains separate and unsupported by
this direct provider.
