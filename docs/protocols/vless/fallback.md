# VLESS fallback

`fallback.rules` selects a native stream endpoint after a VLESS request is rejected.
TLS is terminated first; selectors use the negotiated SNI/ALPN and an HTTP request
path without its query. Each consumed prefix byte is replayed exactly once.

```json
"fallback": {
  "rules": [
    {"destination": {"type": "tcp", "server": "127.0.0.1", "port": 8080}},
    {"name": "example.test", "alpn": "http/1.1", "path": "/app",
     "destination": {"type": "unix", "path": "/run/site.sock"}, "proxy_protocol": 2}
  ]
}
```

Empty selectors provide defaults. Name selection uses the longest matching
substring, then ALPN and exact path selection follow the fixed Xray v26.3.27
inheritance order. Last duplicate rule wins. Inheritance materializes ALPN/path
defaults before global name defaults; a new name-specific ALPN does not
automatically gain paths added to its default ALPN afterwards. Configure shared
ALPN defaults explicitly when needed. A missing matching/default rule rejects
the connection rather than selecting an unrelated target.

`proxy_protocol` is 0 (none), 1 or 2. The preamble precedes replayed application
bytes and carries the original source and listener addresses when available.
Unix destinations are supported on Unix platforms; relative paths resolve from
the configuration directory. VLESS Encryption and fallback cannot be combined.

The existing `fallback: {server, port, alpn}` single-target form remains available,
including its earlier raw TLS ALPN handoff behavior. It cannot be combined with
`rules`; use rules for official-style post-TLS name/ALPN/path selection.

Protocol-owned selection returns a neutral `FallbackRoute`. Generic runtime owns
dialing and relay lifecycle; shared transport owns PROXY wire formatting. Neither
the proxy adapter nor configuration validation parses VLESS request bytes.
