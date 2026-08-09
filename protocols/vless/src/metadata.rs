use zero_traits::{
    ProtocolCapabilityDescriptor, ProtocolCapabilityLevel, ProtocolCapabilityState,
    ProtocolMetadata, ProtocolNetworkCapability,
};

#[derive(Debug, Default, Clone, Copy)]
pub struct VlessProtocol;

impl ProtocolMetadata for VlessProtocol {
    fn descriptor(&self) -> ProtocolCapabilityDescriptor {
        let supported = ProtocolCapabilityState::supported();

        ProtocolCapabilityDescriptor {
            protocol: "vless",
            feature: "vless",
            status: ProtocolCapabilityLevel::Supported,
            compatibility_baseline: "xray_core_vless",
            inbound: ProtocolNetworkCapability::new(supported, supported),
            outbound: ProtocolNetworkCapability::new(supported, supported),
            transports: &[
                "tcp",
                "tls",
                "reality",
                "ws",
                "grpc",
                "h2",
                "http_upgrade",
                "xhttp",
            ],
            mux: supported,
            limitations: &["vless_quic_transport_deprecated_by_xtls"],
        }
    }
}
