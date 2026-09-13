# Authenticated HTTP/2 application settings

Base h2 0.4.14, commit `e2826c54601a2afd5083e496a6e021408cc2a11f`,
from the published crate; its original license is retained.

`client::Builder::peer_application_settings` accepts authenticated ALPS bytes.
It validates frame boundaries, forbidden frames, SETTINGS values, server push
rules and ACCEPT_CH framing before writing the HTTP/2 preface. Unknown extension
frames are ignored. ACCEPT_CH hints do not alter proxy request headers.

Initial peer settings are applied to streams, HPACK and frame-size state before
returning a sender. They generate no ACK. Ordinary SETTINGS still require an ACK
and update the same state; omitted values do not erase previous ALPS settings.
No server behavior or public protocol baseline is changed.

Regression source: `crates/transport/tests/h2_alps.rs`. Not executed in this
implementation-only follow-up.
