# HTTP/3 carrier extension

Upstream h3-quinn 0.0.10, MIT license retained. Adds a neutral externally
classified bidirectional stream source and replay prefix. Ordinary clients
and servers retain upstream behavior. The source must terminate by yielding
a QUIC connection error, like the upstream accept stream. Hysteria2 frame
recognition, authentication and web policy stay in the protocol crate.

The two existing `RecvStream::new` and `SendStream::new` constructors are
public so a sealed caller can move an already tracked original into the
unchanged upstream read-chunk/write-buffer adapters. They neither clone nor
return a raw stream and introduce no executor, socket, policy or default.

Pinned source: `https://static.crates.io/crates/h3-quinn/h3-quinn-0.0.10.crate`,
SHA-256 `8b2e732c8d91a74731663ac8479ab505042fbf547b9a207213ab7fbcbfc4f8b4`.
The package's `.cargo_vcs_info.json` identifies upstream commit
`2dc3412bdf6083451920d5bfd7a9484d054c1859`, path `h3-quinn`, in
`https://github.com/hyperium/h3`. The package version and MIT license remain
unchanged. No dependency or lockfile change is required by this visibility seam.
