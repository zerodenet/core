//! Common runtime ownership. Protocol implementations and forwarding depend on contracts.
mod apply_owner;
pub mod component;
pub mod configuration;
mod host;
pub use apply_owner::ApplyOwner;
pub use host::RuntimeHost;
