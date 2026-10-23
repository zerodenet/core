//! Canonical cold authority for the existing serial WireGuard packet writer.
use crate::{
    protocol_registry::PreparedInboundBind,
    runtime::inbound_operation::{PreparedInboundListenerOperation, RawIpInboundListenerOperation},
};
use std::{net::IpAddr, num::NonZeroUsize};
use zero_config::InboundConfig;
use zero_core::{request::Destination, Address};
use zero_engine::EngineError;
use zero_platform_tokio::contract::component::ResourceDeclaration;

pub(super) fn cold_bind(
    inbound: &InboundConfig,
    operation: Box<dyn PreparedInboundListenerOperation>,
) -> Result<PreparedInboundBind, EngineError> {
    let ip =
        inbound.listen.address.parse::<IpAddr>().map_err(|_| {
            super::invalid("WireGuard controlled listener requires a literal address")
        })?;
    let address = match ip {
        IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
        IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
    };
    PreparedInboundBind::new(
        operation,
        vec![
            ResourceDeclaration::DatagramListener {
                destination: Destination {
                    address,
                    port: inbound.listen.port,
                },
                // Actual startup uses one attempt; reload's established retry is 50.
                max_bind_attempts: NonZeroUsize::new(50).unwrap(),
                // raw_ip/run owns one mutable writer; each select branch awaits its
                // complete send before entering the next iteration. No send fanout.
                max_pending_sends: NonZeroUsize::new(1).unwrap(),
            },
            ResourceDeclaration::NativeIcmp {
                max_sockets: NonZeroUsize::new(RawIpInboundListenerOperation::MAX_ICMP_PROBES)
                    .unwrap(),
            },
        ]
        .into(),
    )
}
