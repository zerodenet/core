# macOS Endpoint Security preparation

2026-11-16: Apple Developer enrollment is not planned, and the later mandatory
Linux VM proposal has also been withdrawn. This directory is an optional
historical capability probe, not a production prerequisite. The refactor retains
native macOS plugins; see [native lifecycle ownership](../../release/macos-plugin-lifecycle.md).

This directory prepares an optional entitlement capability probe and its signing
inputs. It is outside Cargo, CI and normal Zero startup. It does not implement or
qualify the generic plugin process-tree/derived-endpoint provider.

On 2026-11-14 the owner confirmed there is no Apple Developer Program team;
this Mac has no valid signing identity. No request, enrollment, purchase, certificate creation,
build, signing, installation or probe execution has been performed.

## Account and entitlement

1. Enroll the actual person or legal organization in the [Apple Developer
   Program](https://developer.apple.com/programs/enroll/). The account owner
   supplies their own legal information, agreements and payment.
2. Select the actual team and a dedicated helper bundle identifier. Request
   `com.apple.developer.endpoint-security.client` using Apple's [System
   Extensions entitlement form](https://developer.apple.com/contact/request/system-extension/).
3. After Apple approval, obtain the appropriate signing certificate/private key
   and a provisioning profile containing that exact entitlement, team and bundle.
   Adding the key to a local plist does not grant it.

Apple documents the [entitlement requirement](https://developer.apple.com/documentation/bundleresources/entitlements/com.apple.developer.endpoint-security.client)
and the [`es_new_client` result classifications](https://developer.apple.com/documentation/endpointsecurity/client).
An entitled client can still require root privilege and TCC approval. This probe
reports those original API outcomes separately; signing does not grant them.

## Request worksheet

Provide factual values for these fields before submitting a request:

| Field | Information to supply |
| --- | --- |
| Applicant | Actual developer team, legal entity and contact |
| Product | Zero proxy kernel; repository and actual distribution channel |
| Bundle | Dedicated helper identifier owned by that team |
| Proposed use | Track only plugin process subtrees started and owned by Zero, for resource custody and shutdown |
| Proposed events | Fork, exec and exit lifecycle observations needed to maintain that owned subtree |
| Proposed privacy boundary | No unrelated process content, file contents or network payload collection; kernel observations filtered to the owned original lineage |
| Implementation state | Provider is unfinished; this directory contains only an entitlement probe, not production containment |
| Remaining proof | Race-free original installation, process identity rather than PID reuse, missed-event handling, actual subtree retirement and separate endpoint/user/Scope receipts |

Apple decides whether to grant this use. Do not represent Zero as an antivirus
or claim that process containment or a production provider already exists.

## Optional build and packaging

These commands are manual. Run only when compilation/signing is authorized:

```sh
sh scripts/macos-es/build.sh
export APPLE_TEAM_ID='YOUR_TEAM_ID'
export MACOS_SIGN_IDENTITY='YOUR_SELECTED_SIGNING_IDENTITY'
export MACOS_ES_BUNDLE_ID='YOUR_REGISTERED_HELPER_BUNDLE_ID'
export MACOS_ES_PROFILE='/absolute/path/to/approved.provisionprofile'
python3 scripts/macos-es/sign.py
```

Outputs stay under `target/macos-es-preflight`. Packaging checks the selected
profile's entitlement field, expiration, exact application identity and signing
team. macOS still determines actual authorization when creating the ES client. It does not choose a
team, create credentials or run the resulting program. Distribution additionally
needs the team's applicable Developer ID/notarization workflow.

The packaged executable accepts only `--check`. It creates one ES client and
consumes that same thread's `es_delete_client` outcome. It does not subscribe to
events, spawn processes, install a daemon, open IPC or enable protocol admission.
Even a successful result reports `process_tree_provider: unqualified`; client
creation/disposal is not a process-tree, native endpoint or TaskScope close proof.

The existing macOS kernel baseline is unchanged. The macOS 27 descendant-client
API is not used; its newer availability does not solve the current provider.
