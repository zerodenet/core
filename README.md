# Zero

A network proxy kernel written in Rust.

Run it as a local gateway, an edge node, or a server. Combine the protocols you need — SOCKS5, HTTP CONNECT, VLESS, Hysteria2, Shadowsocks, Trojan, mieru, TUN — drive it over HTTP, IPC, or CLI, and control traffic with rule-based routing and outbound groups.

## Quick start

```shell
cargo build --release
cargo run -- run examples/v0.0.1/basic.json
cargo run -- status --json examples/v0.0.1/basic.json
```

## Documentation

- [Zero documentation](https://docs.zerodenet.org/projects/core/)
- [Quick start](https://docs.zerodenet.org/projects/core/guides/quickstart)
- [Configuration](https://docs.zerodenet.org/projects/core/configuration/)
- [Control plane API](https://docs.zerodenet.org/projects/core/control-plane/)
- [Internal architecture notes](docs/project/architecture.md)
- [Examples](examples/)

## License

MPL-2.0 — see [LICENSE](LICENSE).
