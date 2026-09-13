# Browser Dialer

Browser Dialer delegates outbound WebSocket and XHTTP packet-up requests to a
real browser. It is a data carrier, configured through Zero's existing runtime
configuration. TLS fingerprint templates do not provide this capability.

For a VLESS outbound, place `browser_dialer` inside `ws` or `split_http`:

```json
{
  "type": "vless",
  "server": "relay.example",
  "port": 443,
  "id": "11111111-2222-3333-4444-555555555555",
  "tls": {},
  "ws": {
    "path": "/browser",
    "browser_dialer": { "listen": "127.0.0.1:16888", "idle_capacity": 8 }
  }
}
```

The first connection attempt starts a loopback listener and logs its page URL
with `Browser Dialer ready`. Open that page in a browser and keep it open while
using the outbound. The page registers authenticated local WebSocket channels;
the registration token is not included in the logged URL. The browser opens
the remote connection and applies its normal certificate and web-origin rules.
The kernel does not open a replacement native connection when the browser is
unavailable. Task establishment has a bounded timeout.

Identical listener settings share one runtime-owned listener. Conflicting
settings on the same address, inbound port conflicts, public listen addresses,
and unsupported transport/security combinations fail configuration validation.
Prepared request disposal retains the listener; reload and runtime disposal
retire it. Active data streams observe cancellation when polled.

XHTTP supports the browser GET download plus packet uploads. Stream-up,
stream-one, separate download settings, custom TLS identity, REALITY, FinalMask,
custom carrier headers and relay combinations that cannot preserve the selected
path are rejected. Browser-imposed restrictions also apply to cookies, CORS and
forbidden request headers.

Configuration and protocol-simulation tests are separate from execution of the
HTML page in a real browser. The page URL is the local manual verification entry
point; protocol-simulation tests alone do not establish real-browser acceptance.

The opt-in `browser_dialer_real` integration test exercises the shipped page in
an actual browser against local WebSocket and HTTP origins. Set
`ZERO_BROWSER_DIALER_READY_FILE` to a fresh temporary path, run the ignored test,
and open the emitted `page_url`. The test verifies WebSocket round trips, Fetch
streaming downloads and acknowledged POST uploads. This local browser acceptance
passed in the Codex in-app browser on 2026-09-13; it is distinct from production
qualification and from the simulated control-channel tests.
