//! Related integration cases share one executable; feature gates stay on cases.

#[path = "../support/contract.rs"]
mod contract_support;

#[path = "../egress_interface.rs"]
mod egress_interface;
#[path = "../listener_latency.rs"]
mod listener_latency;
#[path = "../process_lookup.rs"]
mod process_lookup;
#[path = "../tcp_nodelay.rs"]
mod tcp_nodelay;

#[path = "../dial_policy.rs"]
mod dial_policy;

#[path = "../datagram_poll.rs"]
mod datagram_poll;

#[path = "../packet_socket_egress.rs"]
mod packet_socket_egress;

#[path = "../contract_scope.rs"]
mod contract_scope;

#[path = "../contract_channels.rs"]
mod contract_channels;

#[path = "../contract_stream.rs"]
mod contract_stream;

#[path = "../contract_inbound.rs"]
mod contract_inbound;

#[path = "../contract_factory.rs"]
mod contract_factory;

#[path = "../contract_packet.rs"]
mod contract_packet;
