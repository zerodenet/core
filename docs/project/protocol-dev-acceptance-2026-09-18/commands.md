# Reproduction commands

Run from `/Volumes/tool/rust/zero`. These commands use isolated paths and do
not install system-global tools. Replace the temporary directory if it no
longer exists. The commits are deliberately fixed; do not substitute newer
tags when reproducing this acceptance.

## Common environment

```bash
export TMPDIR=/private/tmp
export RUST_MIN_STACK=16777216
export ACCEPT_TOOLS=/private/tmp/zero-dev-acceptance-tools-run1
mkdir -p "$ACCEPT_TOOLS"
```

## Reference preparation and identity checks

### Xray v26.3.27

The repository helper downloads and verifies the official release and exact
commit. It also verifies the VLESS package/lock baseline.

```bash
python3 scripts/prepare-xhttp-interop.py \
  --output "$ACCEPT_TOOLS/xray-v26.3.27"
export XRAY_BIN="$ACCEPT_TOOLS/xray-v26.3.27/xray"
"$XRAY_BIN" version
```

### shadowsocks-rust v1.21.2

```bash
python3 scripts/prepare-ss-interop.py \
  --output "$ACCEPT_TOOLS/ss-1.21.2"
export SS_RUST_BIN_DIR="$ACCEPT_TOOLS/ss-1.21.2"
```

### Mieru v3.33.0

```bash
python3 scripts/prepare-mieru-interop.py --check-baseline
python3 scripts/prepare-mieru-interop.py \
  --output "$ACCEPT_TOOLS/mieru-v3.33.0"
export MIERU_REFERENCE_BIN="$ACCEPT_TOOLS/mieru-v3.33.0"
```

### Hysteria app/v2.12.2

```bash
git clone --filter=blob:none --no-checkout \
  https://github.com/HyNetwork/hysteria.git \
  "$ACCEPT_TOOLS/hysteria-v2.12.2-src"
git -C "$ACCEPT_TOOLS/hysteria-v2.12.2-src" checkout --detach \
  619a6f856b69fb7ee6a7a379e810e68b84004605
test "$(git -C "$ACCEPT_TOOLS/hysteria-v2.12.2-src" rev-parse HEAD)" = \
  619a6f856b69fb7ee6a7a379e810e68b84004605
go -C "$ACCEPT_TOOLS/hysteria-v2.12.2-src" build \
  -o "$ACCEPT_TOOLS/hysteria-v2.12.2" ./app
export HY2_BIN="$ACCEPT_TOOLS/hysteria-v2.12.2"
```

### Trojan-Go v0.10.6

The `full` build tag is required by the official application feature set used
by the fixture.

```bash
git clone --filter=blob:none --no-checkout \
  https://github.com/p4gefau1t/trojan-go.git \
  "$ACCEPT_TOOLS/trojan-go-v0.10.6-src"
git -C "$ACCEPT_TOOLS/trojan-go-v0.10.6-src" checkout --detach \
  2dc60f52e79ff8b910e78e444f1e80678e936450
test "$(git -C "$ACCEPT_TOOLS/trojan-go-v0.10.6-src" rev-parse HEAD)" = \
  2dc60f52e79ff8b910e78e444f1e80678e936450
go -C "$ACCEPT_TOOLS/trojan-go-v0.10.6-src" build -tags full \
  -o "$ACCEPT_TOOLS/trojan-go-v0.10.6-full" .
export TROJAN_GO_BIN="$ACCEPT_TOOLS/trojan-go-v0.10.6-full"
```

### Supplementary sing-box v1.13.14

```bash
git clone --filter=blob:none --no-checkout \
  https://github.com/SagerNet/sing-box.git \
  "$ACCEPT_TOOLS/sing-box-v1.13.14-src"
git -C "$ACCEPT_TOOLS/sing-box-v1.13.14-src" checkout --detach \
  25a600db24f7680ad9806ce5427bd0ab8afe1114
test "$(git -C "$ACCEPT_TOOLS/sing-box-v1.13.14-src" rev-parse HEAD)" = \
  25a600db24f7680ad9806ce5427bd0ab8afe1114
go -C "$ACCEPT_TOOLS/sing-box-v1.13.14-src" build -tags with_quic \
  -o "$ACCEPT_TOOLS/sing-box-v1.13.14-quic" ./cmd/sing-box
export SING_BOX_BIN="$ACCEPT_TOOLS/sing-box-v1.13.14-quic"
"$SING_BOX_BIN" version
```

### Tag and package checks

```bash
git ls-remote --tags https://github.com/XTLS/Xray-core.git \
  refs/tags/v25.3.1 refs/tags/v26.3.27
git ls-remote --tags https://github.com/HyNetwork/hysteria.git \
  refs/tags/app/v2.12.2
git ls-remote --tags https://github.com/p4gefau1t/trojan-go.git \
  'refs/tags/v0.10.6*'
git ls-remote --tags https://github.com/shadowsocks/shadowsocks-rust.git \
  'refs/tags/v1.21.2*'
git ls-remote --tags https://github.com/enfein/mieru.git \
  'refs/tags/v3.33.0*'
```

## Local entry-protocol suites

```bash
cargo test -p zero-proxy --all-features \
  --test http --test mixed --test socks5 --test socks5_udp \
  --test socks5_udp_idle --test socks5_udp_reuse
```

## Shadowsocks

```bash
SS_RUST_BIN_DIR="$SS_RUST_BIN_DIR" \
  cargo test -p shadowsocks --all-features --test reference_interop \
  -- --ignored --nocapture
SS_RUST_BIN_DIR="$SS_RUST_BIN_DIR" \
  cargo test -p shadowsocks --all-features --test external_sip023 \
  -- --ignored --nocapture
```

## VMess

The authoritative fixed-Xray filter covers both directions, standard `zero`
and the explicit `zero-plus` rejection without selecting supplementary tools.

```bash
TMPDIR=/private/tmp RUST_MIN_STACK=16777216 XRAY_BIN="$XRAY_BIN" \
  cargo test -p zero-proxy --all-features --test vmess_xray_interop \
  xray -- --ignored --nocapture
```

## VLESS

```bash
TMPDIR=/private/tmp RUST_MIN_STACK=16777216 XRAY_BIN="$XRAY_BIN" \
  cargo test -p zero-proxy --all-features --test vless_xray_interop \
  xray -- --ignored --nocapture
XRAY_BIN="$XRAY_BIN" \
  cargo test -p zero-proxy --all-features --test vless_reality_extensions \
  -- --ignored --nocapture
XRAY_BIN="$XRAY_BIN" \
  cargo test -p zero-proxy --all-features --test vless_tls_vision \
  -- --ignored --nocapture
```

Isolated REALITY+Vision regression:

```bash
XRAY_BIN="$XRAY_BIN" \
  cargo test -p zero-proxy --all-features --test vless_xray_interop \
  'reality_vision::zero_vless_reality_vision_outbound_interops_with_xray' \
  -- --ignored --nocapture --exact
```

## Trojan

```bash
TMPDIR=/private/tmp RUST_MIN_STACK=16777216 XRAY_BIN="$XRAY_BIN" \
  cargo test -p zero-proxy --all-features --test trojan_xray_interop \
  xray -- --ignored --nocapture
TROJAN_GO_BIN="$TROJAN_GO_BIN" \
  cargo test -p zero-proxy --all-features --test trojan_go_interop \
  -- --ignored --nocapture
```

The `grpc` command is expected to pass both directions. Run external protocol
fixtures serially because they share fixed loopback port ranges.

## Hysteria2

```bash
HY2_BIN="$HY2_BIN" \
  cargo test -p zero-proxy --all-features --test hysteria2_official_interop \
  -- --ignored --nocapture
HY2_BIN="$HY2_BIN" \
  cargo test -p zero-proxy --all-features --test hysteria2_official_interop \
  zero_to_official_salamander_ca_pin_and_actual_port_hopping \
  -- --ignored --nocapture --exact
cargo test -p zero-proxy --all-features --test hysteria2_tls_policy \
  -- --nocapture
SING_BOX_BIN="$SING_BOX_BIN" \
  cargo test -p zero-proxy --all-features --test hysteria2_sing_box_interop \
  -- --ignored --nocapture
```

## Mieru

```bash
MIERU_REFERENCE_BIN="$MIERU_REFERENCE_BIN" \
  cargo test -p zero-proxy --all-features --test mieru_official_interop \
  -- --ignored --nocapture
```

## Workspace and build gates

```bash
TMPDIR=/private/tmp RUST_MIN_STACK=16777216 \
  cargo test --workspace --all-features
TMPDIR=/private/tmp RUST_MIN_STACK=16777216 \
  cargo test --workspace --all-features --exclude zero-connector
cargo fmt --all -- --check
cargo clippy --workspace --all-targets
cargo build --release
git diff --check
```

The full workspace command compiles ignored external tests but does not count
them as interop passes; the explicit commands above are the acceptance proof.
