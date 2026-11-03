use zero_traits::{
    ProtocolCapabilityDescriptor, ProtocolCapabilityLevel, ProtocolCapabilityState,
    ProtocolMetadata, ProtocolNetworkCapability,
};

use crate::adapters::identity::NamedProtocolAdapter;

#[cfg(feature = "mixed")]
pub(crate) mod inbound;

#[cfg(feature = "mixed")]
#[derive(Debug)]
pub(crate) struct MixedAdapter;

#[cfg(feature = "mixed")]
impl NamedProtocolAdapter for MixedAdapter {
    const PROTOCOL_NAME: &'static str = "mixed";
    const FEATURE_NAME: &'static str = "mixed";
    const HAS_OUTBOUND: bool = false;
}

#[cfg(feature = "mixed")]
impl ProtocolMetadata for MixedAdapter {
    fn descriptor(&self) -> ProtocolCapabilityDescriptor {
        ProtocolCapabilityDescriptor {
            protocol: "mixed",
            feature: "mixed",
            status: ProtocolCapabilityLevel::Supported,
            compatibility_baseline: "kernel_builtin",
            inbound: ProtocolNetworkCapability::new(
                ProtocolCapabilityState::supported(),
                ProtocolCapabilityState::supported(),
            ),
            outbound: ProtocolNetworkCapability::new(
                ProtocolCapabilityState::unsupported(&[]),
                ProtocolCapabilityState::unsupported(&[]),
            ),
            transports: &["tcp"],
            mux: ProtocolCapabilityState::not_applicable(),
            limitations: &[],
        }
    }
}
