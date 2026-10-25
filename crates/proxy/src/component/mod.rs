//! Cold component assembly. Data channels stay in platform-tokio::contract.
pub(crate) mod background;
pub(crate) mod bound_datagram;
mod business_carrier;
pub(crate) mod business_demand;
pub(crate) mod datagram_retirement;
pub(crate) mod direct_bridge;
pub(crate) mod legacy_stream;
pub(crate) mod request;
pub(crate) mod stream_bridge;

pub use crate::adapters::direct::factory::{DirectFactory, DirectOptions};

#[cfg(test)]
mod tests;

pub use crate::adapters::direct::factory::decode as direct_options;
pub(crate) use zero_runtime::component::{catalog, instance, ServiceAdmission};
pub use zero_runtime::component::{
    ComponentRecipe, ComponentRegistry, PreparedSharedResource, RegistrationError,
    ResourcePreparation, SharedResourceIdentities, ValidatedRecipe,
};
pub fn direct_descriptor() -> zero_traits::ProtocolCapabilityDescriptor {
    zero_traits::ProtocolMetadata::descriptor(&crate::adapters::direct::DirectAdapter::default())
}

pub(crate) mod native_dns;
