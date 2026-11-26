# macOS TUN route recovery

`TUN_ROUTE_RECOVERY_REQUIRED` means that a retained journal cannot prove route
ownership: an old record lacks its original interface index/capture next hop,
or a persisted acquisition intent has no confirmed command outcome. The error
includes the journal path, prefix, recorded interface, observed rows and family.
The existing TUN error/status channel carries this diagnostic.

1. Preserve the journal and stop repeated startup attempts. Stop the previous
   tunnel/VPN through its original controller if it still exists. Do not replace
   an old index or gateway with the current interface's values.
2. Inspect `/usr/sbin/netstat -rnW -f inet` (or `inet6` for IPv6). Compare the
   reported prefix, interface and gateway. A matching name or prefix alone does
   not establish ownership; another VPN or administrator may own that route.
3. If a route still exists and its owner cannot be independently established,
   retain the journal and escalate to the system administrator. Do not delete
   the journal or issue a blanket prefix deletion to bypass the guard. Route
   removal is appropriate only through its known original owner or after
   independent administrator verification; this diagnostic grants no deletion
   authority.
4. After the original owner/system has actually retired the route, retry normal
   TUN start or the existing TUN network recovery command. The kernel reads the
   route table again and clears only the now-absent unknown entries. It never
   upgrades an unknown old index/gateway into ownership. A failed read or any
   matching non-neighbor row preserves the entry and reports the same action.

New macOS Peer/Capture additions persist and sync a pending intent before the
system operation, then confirm installation only after successful completion.
A failed/interrupted operation remains pending. Pending entries are not eligible
for automatic route deletion. Confirmed routes still require their original
identity checks and post-removal readback. Do not downgrade to a binary unaware
of pending intents while such records remain unresolved.

This recovery concerns route metadata only. It does not substitute for original
TUN/helper/monitor/controller closure or Scope joins. Apple authorization is
tracked separately from route-journal recovery.
