//! Related integration cases share one executable; feature gates stay on cases.

#[path = "../support/echo.rs"]
mod echo;

#[path = "../echo_translation.rs"]
mod echo_translation;
#[path = "../echo_translation_headers.rs"]
mod echo_translation_headers;
#[path = "../fragment_reassembly.rs"]
mod fragment_reassembly;
#[path = "../icmp_feedback.rs"]
mod icmp_feedback;
#[path = "../icmp_policy.rs"]
mod icmp_policy;
#[path = "../mtu.rs"]
mod mtu;
#[path = "../packet.rs"]
mod packet;
#[path = "../packet_checksums.rs"]
mod packet_checksums;

#[path = "../packet_buffer.rs"]
mod packet_buffer;
