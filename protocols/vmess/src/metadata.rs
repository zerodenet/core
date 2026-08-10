use zero_traits::{
    ProtocolCapabilityDescriptor, ProtocolCapabilityLevel, ProtocolCapabilityState,
    ProtocolMetadata, ProtocolNetworkCapability,
};

#[derive(Debug, Default, Clone, Copy)]
pub struct VmessProtocol;

impl ProtocolMetadata for VmessProtocol {
    fn descriptor(&self) -> ProtocolCapabilityDescriptor {
        let supported = ProtocolCapabilityState::supported();

        ProtocolCapabilityDescriptor {
            protocol: "vmess",
            feature: "vmess",
            status: ProtocolCapabilityLevel::Supported,
            compatibility_baseline: "xray_core_vmess_aead",
            inbound: ProtocolNetworkCapability::new(supported, supported),
            outbound: ProtocolNetworkCapability::new(supported, supported),
            transports: &["tcp", "tls", "ws", "grpc"],
            mux: supported,
            limitations: &[],
        }
    }
}
