//! Protocol-owned roleless shared-resource factories.
mod plugin;
pub use plugin::{PluginFactory, PluginOptions};
#[cfg(feature = "blake3")]
mod replay;
#[cfg(feature = "blake3")]
pub use replay::{ReplayFactory, ReplayOptions};
