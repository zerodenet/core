use zero_config::{InboundConfig, InboundProtocolConfig};
use zero_engine::EngineError;

use crate::adapters::direct::DirectAdapter;
use crate::runtime::inbound_operation::PreparedInboundListenerOperation;

impl DirectAdapter {
    pub(super) fn prepare_inbound_listener_impl(
        &self,
        inbound: InboundConfig,
    ) -> Result<Box<dyn PreparedInboundListenerOperation>, EngineError> {
        let (target, port) = match &inbound.protocol {
            InboundProtocolConfig::Direct { target, port, .. } => (target.clone(), *port),
            _ => {
                return Err(EngineError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "direct adapter received non-direct inbound config",
                )));
            }
        };
        let target = target.map(|value| {
            if let Ok(address) = value.parse::<std::net::Ipv4Addr>() {
                zero_core::Address::Ipv4(address.octets())
            } else if let Ok(address) = value.parse::<std::net::Ipv6Addr>() {
                zero_core::Address::Ipv6(address.octets())
            } else {
                zero_core::Address::Domain(value)
            }
        });
        Ok(Box::new(crate::inbound::DirectInboundListenerOperation {
            #[cfg(feature = "managed-datagram-runtime")]
            udp: inbound.udp.enabled,
            target,
            port,
        }))
    }
}

impl DirectAdapter {
    pub(super) async fn bind_inbound_impl(
        &self,
        inbound: &InboundConfig,
    ) -> Result<crate::protocol_registry::BoundInbound, EngineError> {
        use crate::protocol_registry::BoundInbound;
        <Self as crate::adapters::identity::NamedProtocolAdapter>::validate_inbound_config(
            self, inbound,
        )?;
        #[cfg(feature = "managed-datagram-runtime")]
        let wants_udp = inbound.udp.enabled;
        let tcp = zero_platform_tokio::TokioListener::bind(&format!(
            "{}:{}",
            inbound.listen.address, inbound.listen.port
        ))
        .await
        .map_err(EngineError::Io)?;
        #[cfg(feature = "managed-datagram-runtime")]
        if wants_udp {
            let udp = tokio::net::UdpSocket::bind(tcp.local_addr().map_err(EngineError::Io)?)
                .await
                .map_err(EngineError::Io)?;
            return Ok(BoundInbound::TcpAndDatagram(tcp, std::sync::Arc::new(udp)));
        }
        Ok(BoundInbound::Tcp(tcp))
    }
}
