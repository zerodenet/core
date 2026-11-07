//! Resource lifetimes, cold assembly and confirmed replacement, independent of protocols.
mod admission;
mod bindings;
pub mod bound_datagram;
pub mod catalog;
pub mod dependencies;
pub mod generations;
pub mod instance;
mod preparation;
mod recipe;
mod registry;
pub use admission::ServiceAdmission;
pub use preparation::{
    PreparedSharedResource, ResourcePreparation, SharedResourceIdentities,
    SourceDependencyDeclarations,
};
pub use recipe::{ComponentRecipe, PacketPathFacts};
pub use registry::{ComponentRegistry, RegistrationError, ValidatedRecipe};
#[cfg(test)]
pub use zero_proxy::component::{DirectFactory, DirectOptions};
#[cfg(test)]
mod tests;
