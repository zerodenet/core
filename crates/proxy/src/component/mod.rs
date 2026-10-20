//! Cold component assembly. Data channels stay in platform-tokio::contract.
mod admission;
mod bindings;
pub(crate) mod catalog;
mod dependencies;
pub(crate) mod datagram_retirement;
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
