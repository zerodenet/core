#![recursion_limit = "256"]

mod adapters;
mod groups;
mod inbound;
mod inventory;
mod logging;
mod protocol_registry;
mod register;
mod runtime;
mod transport;

pub use inbound::{TunInterfaceOptions, TunRuntimeOptions};
pub use inventory::ProtocolInventory;
pub use runtime::{ConfigApplyReconciler, ConfigReconcileResult, Proxy, ProxyHandle, RunningProxy};

pub fn compiled_protocol_features() -> Vec<String> {
    register::compiled_protocol_features()
}
