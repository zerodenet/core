# Dev acceptance remediation — 2026-09-18

## Identity and decision

- Repository state: `8fcc4ecb8f567e092475387033c1cd415aaaeb1f`
  plus the preserved dirty working tree.
- Final execution time: 2026-09-18 17:03:53 CST.
- No reset, cleanup, commit, push, deployment or production access occurred.
- Decision: the five finite blockers identified by the initial acceptance are
  closed. The current tree passes the dev gate and may enter production trial.

## Remediation results

| Initial finding | Root cause | Remediation | Current evidence | Status |
|---|---|---|---|---|
| VLESS REALITY+Vision Zero→Xray failed | The loaded-run ten-second deadline misclassified a slow but valid path; trace runs completed in roughly five to six seconds | Increased this external-test deadline to 30 seconds and made the complete fixed-Xray filter a CI gate | Serial Xray v26.3.27 matrix 16/16; exact case 10/10 during diagnosis | Closed |
| Trojan TLS+gRPC Zero→Xray failed | The Trojan-owned TLS profile did not advertise carrier-required `h2` ALPN | Store ALPN in the owned profile and add `h2` only when gRPC is selected; raw TLS remains unchanged | Xray v26.3.27 9/9; Trojan-Go v0.10.6 2/2; local raw/no-ALPN regression passed | Closed |
| Trojan descriptor omitted WS/gRPC | Static metadata listed only TCP/TLS | Descriptor now lists `tcp`, `tls`, `ws`, `grpc` | Registry/full-workspace tests passed | Closed |
| VMess package/baseline/CI mismatch | Manifest and generated lock used the obsolete `25.3.1` label while code/docs/tests already targeted Xray v26.3.27 | Manifest/generated lock moved to `26.3.27`; reference checker validates VMess too; dedicated CI added | Baseline checker passed; fixed-Xray matrix 13/13, including standard `zero` both ways and `zero-plus` rejection | Closed |
| Hysteria2 package/baseline/capability mismatch | Package label and descriptor lagged the already-selected official app/v2.12.2 reference | Manifest/generated lock moved to `2.12.2`; CI checks both; descriptor reports supported and drops stale limitation | Official app/v2.12.2 matrix 12/12, including Salamander, CA/pin and observed multi-port hopping | Closed |
| Connector workspace-gate failure | On a nearly full filesystem, percentage reserve exceeded available space, so a tiny ACK could not persist after remote success | Maintenance/ACK writes retain a bounded `min(configured reserve, 64 MiB)` emergency floor; normal puts retain the full reserve | Connector regular suites 12/12 and 17/17; full workspace exit 0 | Closed |

The full rerun also exposed an independent automatic-OCSP test-server EOF race.
The fixture now explicitly shuts down its response stream after writing the
close-delimited body. The exact test passed 20/20 diagnostic repetitions and
passed in the final workspace run.

## Fixed baselines and CI

| Protocol | Fixed reference | Immutable commit | Package / generated lock | Required workflow |
|---|---|---|---|---|
| VMess | Xray-core v26.3.27 | `d2758a023cd7f4174a5a5fa4ff66e487d4342ba0` | `26.3.27` / `26.3.27` | `vmess-interop.yml` |
| VLESS | Xray-core v26.3.27 | `d2758a023cd7f4174a5a5fa4ff66e487d4342ba0` | `26.3.27` / `26.3.27` | `xhttp-interop.yml` now runs all Xray-named cases |
| Trojan | Trojan-Go v0.10.6 plus Xray carrier checks | `2dc60f52e79ff8b910e78e444f1e80678e936450` and the Xray commit above | `0.10.6` / `0.10.6` | `trojan-interop.yml` |
| Hysteria2 | official app/v2.12.2 | `619a6f856b69fb7ee6a7a379e810e68b84004605` | `2.12.2` / `2.12.2` | `hysteria2-interop.yml` |

The repository intentionally ignores `Cargo.lock`; acceptance and CI generate
it, then verify the resolved protocol package versions before using `--locked`.
`scripts/prepare-xhttp-interop.py --check-baseline` passed for the shared
VLESS/VMess Xray identity.

## Final gates

| Command or suite | Result |
|---|---|
| `TMPDIR=/private/tmp RUST_MIN_STACK=16777216 cargo test --workspace --all-features` | Passed, exit 0 |
| `cargo fmt --all -- --check` | Passed |
| `git diff --check` | Passed |
| `TMPDIR=/private/tmp cargo clippy --workspace --all-targets` | Passed |
| `TMPDIR=/private/tmp cargo build --release` | Passed in 10m04s |

Release artifact: `target/release/zero`, 47,102,968 bytes, SHA-256
`71c01ad885e2e020c624432ff5da525f41963d8f3bca47ea924834aabe47606e`.

## Current raw evidence

Raw logs are kept in `/private/tmp`; they are ephemeral, so their digests and
semantic results are recorded here.

| Log | SHA-256 | Result |
|---|---|---|
| `zero-remediation-vless-xray.log` | `6beb9e458558c39dc48ee1ccaabd3fd421bfdb75379150e680648e9aef191176` | Xray 16/16 |
| `zero-remediation-vmess-xray.log` | `c7da8e493e9d8e8bf24a98be0bcabbc95541a67485986fd5918df1e741707c40` | Xray 13/13 |
| `zero-remediation-trojan-xray.log` | `d6565920d501818a7e03a289b41985ffa2e1672901d9ece34d8ede60092c62ad` | Xray 9/9 |
| `zero-remediation-trojan-go.log` | `3107c4ed1f952fffcef402052a951698afc083e6129a83213393a1a005417ca5` | Trojan-Go 2/2 |
| `zero-remediation-hy2-official.log` | `b7bb9aca1a6a32562d7903e3a256624c2ca860e69d36924da96b2ea95673d9d0` | official 12/12 |
| `zero-remediation-workspace.log` | `db495887943d5f134ca7b661dd51ef0ca503fa73e49c53d18318d9ebaafed1f0` | full workspace passed |
| `zero-remediation-clippy.log` | `e2c097f410e73173af031ef6408aaf8a0018be49b5a5da77f6f710eedd0af634` | workspace all-targets passed |
| `zero-remediation-release.log` | `41881b611848d16ae613c8e5b359f15788e34ddf5d503a9050c82db174ce0857` | release rebuild passed |

One attempted parallel evidence capture is intentionally excluded: these
external fixtures share fixed local port ranges and collided with each other.
All authoritative results above are the subsequent serial reruns.

## Non-blocking production trial

Production trial should cover long-lived connections, NAT rebinding and mobile
network churn, real multi-region port hopping, certificate/pin rotation, reload
under traffic, resource ceilings, loss/reordering over hours and packaged
artifacts on production platforms. HTTP outbound, HTTP inbound authentication
and authentication of Mixed's HTTP branch remain explicit scope decisions.
WireGuard remains outside this acceptance.
