//! Related integration cases share one executable; feature gates stay on cases.

#[path = "../client_udp.rs"]
mod client_udp;
#[path = "../tcp_accept_identity.rs"]
mod tcp_accept_identity;
#[path = "../tcp_receive_recovery.rs"]
mod tcp_receive_recovery;
#[path = "../udp_queue.rs"]
mod udp_queue;

#[path = "../packet_observation.rs"]
mod packet_observation;

#[path = "../packet_reply.rs"]
mod packet_reply;
