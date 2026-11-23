# HTTP/2 ALPS configuration forwarding

Base hyper 1.9.0, commit `0d6c7d5469baa09e2fb127ee3758a79b3271a4f0`,
from the published crate; its original license is retained.

`client::conn::http2::Builder::peer_application_settings` forwards authenticated
settings to the patched h2 builder. XHTTP uses it before starting a shared
HTTP/2 connection. Decoding, state changes and ACK semantics are owned by h2.

`ext::CloseOnReadEof` lets a streaming HTTP/1 response opt into checking the
accepted connection's read EOF. XHTTP applies it to downloads while preserving
the existing half-close handling for upload acknowledgements.
