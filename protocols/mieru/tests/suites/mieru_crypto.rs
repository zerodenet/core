//! Related integration cases share one executable; feature gates stay on cases.

#[path = "../loopback.rs"]
mod loopback;
#[path = "../outbound.rs"]
mod outbound;
#[path = "../pbkdf2_verify.rs"]
mod pbkdf2_verify;
#[path = "../udp.rs"]
mod udp;
#[path = "../udp_framing.rs"]
mod udp_framing;
