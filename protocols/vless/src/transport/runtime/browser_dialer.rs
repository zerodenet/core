//! Shared neutral Browser registry; the protocol retains framing only.
pub use zero_transport::browser_dialer::registry::{
    Access, ProviderFactory, ProviderOptions, Registry,
};
#[cfg(test)]
#[path = "../../../tests/transport/browser_dialer.rs"]
mod tests;
