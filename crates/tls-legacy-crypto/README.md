# TLS 1.2 compatibility crypto

Safe bounded adapter over AWS-LC 0.40.0, opt-in through rustls `legacy-client`.
It never adds suites to the default rustls provider. Secrets are zeroized.

* AES-CBC SHA1/SHA256/SHA384 and 3DES use AWS-LC's TLS-specific AEAD primitives,
  with explicit unpredictable IVs and its constant-time record authentication.
* AES-256-CBC/SHA256 is absent from AWS-LC's TLS AEAD descriptors. Its fallback
  uses AWS-LC AES and HMAC and evaluates every possible padding length with
  constant-time selection. Work depends on the public record length, not the
  decrypted padding; this costs up to 256 HMAC evaluations per record.
* RFC 7919 DHE uses built-in safe primes, peer-key checks and padded secrets.
  The rustls adapter strips leading zeros for TLS 1.2, as required by RFC 5246.

FFI contexts are local or exclusively owned; no mutable context is shared.
No tests were executed in this implementation-only follow-up. Test sources
cover corruption, sequence mismatch, record limits and invalid DH public keys.
