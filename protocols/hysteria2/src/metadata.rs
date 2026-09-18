use zero_traits::{
    ProtocolCapabilityDescriptor, ProtocolCapabilityLevel, ProtocolCapabilityState,
    ProtocolMetadata, ProtocolNetworkCapability,
};

#[derive(Debug, Default, Clone, Copy)]
pub struct Hysteria2Protocol;

impl ProtocolMetadata for Hysteria2Protocol {
    fn descriptor(&self) -> ProtocolCapabilityDescriptor {
        let unsupported = ProtocolCapabilityState::unsupported(&[]);
        let supported = ProtocolCapabilityState::supported();

        ProtocolCapabilityDescriptor {
            protocol: "hysteria2",
            feature: "hysteria2",
            status: ProtocolCapabilityLevel::Supported,
            compatibility_baseline: "hysteria_app_v2_12_2",
            inbound: ProtocolNetworkCapability::new(supported, supported),
            outbound: ProtocolNetworkCapability::new(supported, supported),
            transports: &["quic"],
            mux: unsupported,
            limitations: &[],
        }
    }
}
