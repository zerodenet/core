//! Cold component assembly. Data channels stay in platform-tokio::contract.
mod admission;
pub(crate) use admission::ServiceAdmission;
mod bindings;
pub(crate) mod bound_datagram;
pub(crate) mod business_demand;
pub(crate) mod catalog;
pub(crate) mod datagram_retirement;
mod dependencies;
pub(crate) mod direct_bridge;
mod generations;
pub(crate) mod instance;
pub(crate) mod legacy_stream;
mod preparation;
mod recipe;
mod registry;
pub(crate) mod request;
pub(crate) mod stream_bridge;

pub use crate::adapters::direct::factory::{DirectFactory, DirectOptions};
pub use preparation::{PreparedSharedResource, ResourcePreparation, SharedResourceIdentities};
pub use recipe::ComponentRecipe;
pub use registry::{ComponentRegistry, RegistrationError, ValidatedRecipe};

#[cfg(test)]
mod tests;
