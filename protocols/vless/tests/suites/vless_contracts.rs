//! Related integration cases share one executable; feature gates stay on cases.

#[path = "../common_tests.rs"]
mod common_tests;
#[path = "../deferred_response.rs"]
mod deferred_response;
#[path = "../encryption.rs"]
mod encryption;
#[path = "../fallback.rs"]
mod fallback;
#[path = "../fallback_replay.rs"]
mod fallback_replay;
#[path = "../flow.rs"]
mod flow;
#[path = "../handshake.rs"]
mod handshake;
#[path = "../identity.rs"]
mod identity;
#[path = "../mux.rs"]
mod mux;
#[path = "../mux_crypto.rs"]
mod mux_crypto;
#[path = "../reverse.rs"]
mod reverse;
#[path = "../slide_buffer_tests.rs"]
mod slide_buffer_tests;
#[path = "../vision.rs"]
mod vision;
