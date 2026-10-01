//! Related integration cases share one executable; feature gates stay on cases.

#[path = "../client_hello_fingerprint.rs"]
mod client_hello_fingerprint;
#[path = "../client_hello_policy.rs"]
mod client_hello_policy;
#[path = "../fingerprint_handshake.rs"]
mod fingerprint_handshake;
#[path = "../hello.rs"]
mod hello;
