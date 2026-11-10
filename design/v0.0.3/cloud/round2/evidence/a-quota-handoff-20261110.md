# A quota source / Scope storage handoff — WIP, do not activate

Saved at the user's environment-close request. Base is
`502ec395e2a939ce50c2552ad7f10b6a104e2020`; all existing file preimages also equal
`7401588095b824cf899cee4c7c6578c6a38c689b` byte-for-byte. Latest 27 source files
were copied exactly from `a-quota-source-owned-v1-mutable/after`. No merge into
main, Cargo, tests, rustc, semantic repair or new environment during handoff.

This is an incomplete source snapshot, not a buildable/qualified change.
Earlier ordinary source persistence19, initial document, Logger, Direct DNS,
SOCKS Multiple, and withdrawn nondefault/Source-projection proposals are not
replayed. M already applied SOCKS Multiple10 at `062d218d00b8424261787ee34d338a88f93f8bd6`.

## Saved work

- Optional quota storage limit with omitted None/default16MiB and Source-owned
  counter born before read, later adopted into the existing Host root.
- Same-inode bounded seek/read, constant-stack facts pass and second-pass guard;
  original serde remains the decoder. Whitespace does not become a file-size
  cap. Reader8192, token scratch, decoded rows/strings and duplicate table have
  separate conservative storage accounting.
- Paid quota Storage/Weak and book owners, live HashMap/string admission,
  tombstone/grow peak handling, snapshot/fixed encoded backing promises and
  data-before-credit handback. Persistent and memory-only quota are distinct.
- Draft Source execution recipe, original genuine std thread/join/panic/native
  cleanup, finite Hosted blocking actor and inline driver. Explicit Host mode
  forbids missing-registrar/Tokio fallback to std. Work/cause storage has no
  intended strong Source/Scope back edge.
- Existing SDK successful-header exclusive GC visibility change (implementation
  unchanged) and a readonly actual Hosted offer layout. M approved these only
  with the complete eventual quota consumer, not as standalone APIs.

## Not complete / not verified

- `source/quota/execution.rs` declares **shutdown.rs, which is not yet written**.
  `receive_task_failure` / `receive_driver_failure` and shutdown/status/receiver
  methods are not wired. Scoped/genuine actor integration needs full review.
- Engine quota still has its original std persistence wrapper; it has not yet
  been replaced with the new Source writer. Actual Proxy/main progress,
  shutdown, old-std-to-Hosted transfer and held registration ACK are incomplete.
- Proxy Source-to-Host adoption and actual accepted limit candidate publication
  are not connected. Growth, rollback and publish outside Host/catalog/Engine
  locks remain required. Host.rs was not copied or edited.
- New config field is not yet in owner_budgets' full field table/contract test.
  New quota tests, existing fixture signature migrations and Source constructor
  visibility/static checks remain unfinished. No running qualification exists.
- Exact IO panic/poison cleanup and independent late/native diagnostics require
  review; retain original Box/Arc and Unknown, never turn cancellation/poison/
  scope counters into positive NativeClosed or ScopeACK.
- Preserve the original 256MiB-whitespace/192MiB-RLIMIT/180s quota oracle and
  streaming map semantics. Do not apply file_read_max_bytes to the whole quota
  file, evict valid balances, make a second book/Host/runtime, or add watchers.

## Integrating later

Do not merge this WIP wholesale into production. Forward from its exact base,
keep M's later Source19 closed-counter guard, Journal/counter additions,
Host/ApplyOwner/Catalog changes (`a6502f36e` and predecessors), and the already
applied SOCKS Multiple fixes. Shared files remain M's sole serial integration.
Likely overlap: runtime options/defaults/budgets/validation/table,
HostOriginalBudgets, Source/configuration exports, Engine quota/runtime, SDK
HostedScope and join metadata. No manifests or lockfiles were changed.

DNS stays open: B F8 Browser42 is frozen but not yet qualified; F7 appearance,
Mieru whole/mixed pool and applicable platform/SIP paths remain specific donor
work. Original WG outer2 stays unchanged; SOCKS Multiple producer is in M,
actual same-input WG regression still pending. Only after all legitimate
callers migrate may inert ProxyDnsOutboundConnector/Session0/exchange installs
be deleted. DoQ detour is reserved/rejected; non-Direct nondefault dial remains
rejected by the original validator. Do not restore the withdrawn projections.
