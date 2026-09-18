# Evidence log summary

## Audit identity

- Repository: `/Volumes/tool/rust/zero`
- HEAD: `8fcc4ecb8f567e092475387033c1cd415aaaeb1f`
- Execution window represented below: 2026-09-18 14:03–15:14 +0800
- Working tree: dirty before and after; existing modifications preserved
- Temporary root: `/private/tmp`
- External tool root: `/private/tmp/zero-dev-acceptance-tools-run1`
- Production access: none
- Commit/push/deploy: none

Raw logs are intentionally outside the repository because they include verbose
test-process output and temporary paths. This durable summary records their
names, SHA-256 digests and semantic result. Empty `fmt`/`diff-check` logs mean
the commands succeeded without diagnostics.

## Protocol evidence logs

| Raw log | SHA-256 | Result |
|---|---|---|
| `zero-dev-acceptance-entry-protocols.log` | `a8f4ae5bc9d872283824720959735f273191870fa2605a98bb67918542e60926` | HTTP/Mixed/SOCKS5 64 passed |
| `zero-dev-acceptance-ss-reference.log` | `e3c06d57822e8c2f21498ec26f2da6043a2488ae3618aa7c7acb7e8af8ca3b27` | Official SS 2 passed |
| `zero-dev-acceptance-ss-sip023.log` | `dc657049d80baee0c52dc4ddbfb75395dd329a1da646950ec48620528aa7b0f5` | SIP023 5 passed |
| `zero-dev-acceptance-vmess-zero-out-rerun.log` | `f424a667770c9483f80806cf1c73078b723d1c4942f0cb8e05a4b62e4097d7ad` | selected Zero→Xray/sing-box 9 passed |
| `zero-dev-acceptance-vmess-xray-out.log` | `c637704490e1227a382db10d32a299227eaee7be912e1f9d7246877e246c5b7d` | Xray→Zero 6 passed |
| `zero-dev-acceptance-vless-xray.log` | `5824b7ffcffcadd6d07fad45ca6162226101c92c0e75f2d0a741527d1c255457` | 17 passed, REALITY+Vision failed |
| `zero-dev-acceptance-vless-reality-rerun.log` | `024b4376e209cafe0d24a3752b63b0792d5d2ad6d853553ada66c0be6f57a861` | isolated REALITY+Vision failed again |
| `zero-dev-acceptance-vless-reality-extensions.log` | `214800f092b523a3924bf23bc7af26c53104354a5b44eee61d6053dfe579a2c3` | 3 passed |
| `zero-dev-acceptance-vless-tls-vision.log` | `4fcac89ccb623cf9cda5ba5b952c969f45f6ca039423f98048ed2b04e176f5bd` | 2 passed |
| `zero-dev-acceptance-trojan-xray.log` | `911611ea0e386f51a8b12909a0cadea472d33f680129224aba60dff5f5037c29` | raw Xray matrix 5 passed |
| `zero-dev-acceptance-trojan-ws-xray.log` | `82a0d07e12c4640b4289772d455533914f4adb0d120ade264aa8d0f88fe3a777` | WS 2 passed |
| `zero-dev-acceptance-trojan-grpc-xray.log` | `00c9d5b243d8c835537f68af09ef178078f048ba4bf5c0e50f1659bef6818ba3` | gRPC 1 passed, outbound failed |
| `zero-dev-acceptance-trojan-go-rerun.log` | `af67431b7821b07a723117864874c26b9c00835e5a24dbbce85d1f6ca09add33` | Trojan-Go 2 passed |
| `zero-dev-acceptance-hy2-official.log` | `00954be5cb991a8e783eea9cf809e3422eb55354d5d9119f8f42f29628b54bd6` | official HY2 11 passed |
| `zero-dev-acceptance-hy2-node-security-rerun.log` | `9e26665ca34bb4ae5f9536dd2f846970f2e5667b771d1703308b9562ab672de5` | Salamander/CA/pin/actual hopping passed |
| `zero-dev-acceptance-hy2-tls-policy.log` | `b852f186d93200f1efb2ce191bedce526eaec14816882371589b5b6a76b2a5ec` | policy 5 passed, including wrong CA/pin rejection |
| `zero-dev-acceptance-hy2-sing-box-rerun.log` | `d0cc789094b23739b708aa488569e48f0750d29f7935aca43793fad97b4fda7b` | supplementary 6 passed |
| `zero-dev-acceptance-mieru.log` | `498dc9cee1ffb9399f57aada843823f43bbf84f5e4700ee065daca2c4d74c131` | official Mieru 8 passed |

## Repository gates

| Raw log | SHA-256 | Result |
|---|---|---|
| `zero-dev-acceptance-workspace.log` | `19e47476ac8ea66fcac2a4f5f68b2b1d32981d521ba9221d94f3e78ca6ea3fa1` | failed: connector persistence-deadline timing assertion |
| `zero-dev-acceptance-connector-rerun.log` | `4a8720e77dc52d0e6ccd428922f47a806210a4929fc26abb07eacb6234c8cbaa` | first connector failure passed alone |
| `zero-dev-acceptance-workspace-rerun.log` | `c894ff47f756c44c129a64408a4c8c01f5b43f6cd0d74a6543b5d4212262d106` | failed: connector webhook pending count |
| `zero-dev-acceptance-connector-webhook-rerun.log` | `4430cba8260e1865cf14b649ab776a6dac54f2639463c0eb79a5569e70f91d91` | second connector failure reproduced alone |
| `zero-dev-acceptance-workspace-without-connector.log` | `56861ba68d4256a86f598d625b674cf7ded5996ce6ba8fe659e5217b0ae11d18` | zero-proxy completed; later transport OCSP timeout stopped run |
| `zero-dev-acceptance-ocsp-rerun.log` | `1eb683a391f2515880483568b28ccaa120b6abd23cfad6b19545ad8ed66be851` | exact OCSP test passed alone |
| `zero-dev-acceptance-clippy.log` | `0460052fcd62946e8cb023bcbeca5e93e93fdff484b210e018ce3e46f634c648` | workspace/all-targets/all-features passed |
| `zero-dev-acceptance-fmt.log` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | format check passed |
| `zero-dev-acceptance-diff-check.log` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | whitespace/error check passed |
| `zero-dev-acceptance-release-build.log` | `63026fb3769bbed962623712f36d8a9d17962ffa89cca881be812a30da7511b2` | default `cargo build --release` passed in 16m37s |

Release artifact: `target/release/zero`, Mach-O x86_64, 47,102,400 bytes,
SHA-256 `01053ca3f148297666106e2556c47d1a450b575c3c8f37f8e570d7aa974f2995`,
modified 2026-09-18 15:14:46 +0800. This is the historical pre-remediation
artifact; the final artifact identity is recorded below.

## Remediation evidence

The records above preserve the initial audit. The authoritative post-fix logs,
digests, final workspace result and release artifact are recorded in
[remediation.md](remediation.md). The remediated tree passes the dev gate.
