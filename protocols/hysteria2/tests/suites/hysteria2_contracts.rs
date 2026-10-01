//! Related integration cases share one executable; feature gates stay on cases.

#[path = "../negotiation.rs"]
mod negotiation;
#[path = "../settings.rs"]
mod settings;
#[path = "../shared.rs"]
mod shared;
#[path = "../tcp_stream.rs"]
mod tcp_stream;
#[path = "../udp.rs"]
mod udp;
#[path = "../user_profile.rs"]
mod user_profile;
