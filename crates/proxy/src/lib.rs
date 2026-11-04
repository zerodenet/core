#![recursion_limit = "256"]

mod adapters;
pub mod component;
mod groups;
mod inbound;
mod inventory;
mod logging;
mod protocol_registry;
mod register;
mod runtime;
mod transport;
mod validation;

pub use inbound::{TunInterfaceOptions, TunRuntimeOptions};
pub use inventory::ProtocolInventory;
pub use runtime::{ConfigApplyReconciler, ConfigReconcileResult, Proxy, ProxyHandle, RunningProxy};
pub use validation::{validate_config, validate_prepared_configuration};

pub fn compiled_protocol_features() -> Vec<String> {
    register::compiled_protocol_features()
}

#[cfg(all(test, any(feature = "vmess", feature = "mieru")))]
mod dns_fixture;
#[cfg(test)]
mod socket_fixture;
