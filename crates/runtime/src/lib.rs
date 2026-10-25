//! Common runtime ownership. Protocol implementations and forwarding depend on contracts.
mod apply_owner;
pub mod component;
mod host;
pub use apply_owner::ApplyOwner;
pub use host::RuntimeHost;
