//! Cold component assembly. Data channels stay in platform-tokio::contract.
mod bindings;
pub(crate) mod catalog;
mod dependencies;
pub(crate) mod direct_bridge;
pub(crate) mod instance;
mod legacy_stream;
mod preparation;
mod recipe;
mod registry;
pub(crate) mod request;
pub(crate) mod stream_bridge;

pub use crate::adapters::direct::factory::{DirectFactory, DirectOptions};
pub use preparation::{PreparedSharedResource, ResourcePreparation};
pub use recipe::ComponentRecipe;
pub use registry::{ComponentRegistry, RegistrationError, ValidatedRecipe};

#[cfg(test)]
mod tests;
