//! Control-plane handle facade.
//!
//! Keep the root thin so command/query/event interception details do not
//! accumulate back into a single runtime file.

mod command;
mod configuration;
mod event;
mod model;
mod query;
mod util;

pub use configuration::{ConfigApplyReconciler, ConfigReconcileResult};
pub use model::ProxyHandle;
