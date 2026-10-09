mod endpoint;
mod fixtures;
mod inbound;
mod listener_addresses;
mod listener_quic;
mod outbound;
mod packet_route;
#[cfg(feature = "udp-runtime")]
mod registration;
mod validation;
