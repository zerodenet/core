//! Cold component assembly. Data channels stay in platform-tokio::contract.
mod bindings;
pub(crate) mod catalog;
pub(crate) mod direct_bridge;
pub(crate) mod instance;
mod legacy_stream;
mod registry;
pub(crate) mod request;

pub use crate::adapters::direct::factory::{DirectFactory, DirectOptions};
pub use registry::{ComponentRegistry, RegistrationError, ValidatedRecipe};

#[cfg(test)]
mod tests;
