# Direct-access route exceptions

`route.bypass` is an optional list of existing routing conditions. A match
returns `direct` before rule/global mode selection. An empty list preserves the
previous mode behavior. The contract is advertised as `route_bypass_v1`.

```json
{
  "route": {
    "bypass": [
      {"type": "ip", "values": ["192.168.0.0/16"]},
      {"type": "domain_regex", "values": ["^.*\\.internal\\.example$"]}
    ],
    "rules": [],
    "final": {"type": "direct"}
  }
}
```

Exceptions are configuration-owned and compiled with each runtime snapshot.
They use the existing condition validation and trusted resolved-address boundary;
IP conditions request address resolution even in global mode or after an ordinary
domain rule matched, so a private resolved address can still take precedence.
Old snapshots
retain their own exception set across staged reloads and rollback. A bypass match
does not claim an index in the ordinary `route.rules` list.

This is a routing decision, not an instruction to rewrite host interface routes.
Controllers may separately project IP networks into TUN route exclusions and
native proxy settings. Domain rules require a known destination hostname; they
cannot infer hidden names from encrypted client DNS or ECH.

Route evaluation and the need for real destination IPs are computed together in
`zero-engine` (`runtime/route.rs`) from one mode read and one runtime snapshot.
A matched bypass is final and requests no routing DNS lookup, even when the same
configuration also contains IP conditions. Ordinary domain matches and a direct
fallback must still allow unresolved IP rules to run where applicable. Proxy
only executes requested DNS work. Direct connection establishment retains its
separate direct-role DNS lookup when an address is needed for dialing.
