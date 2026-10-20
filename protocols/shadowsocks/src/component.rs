//! Protocol-owned roleless shared-resource factories.
pub mod inbound;
mod plugin;
pub use plugin::{PluginFactory, PluginOptions};
#[cfg(feature = "blake3")]
mod replay;
#[cfg(feature = "blake3")]
pub use replay::{ReplayFactory, ReplayOptions};

#[cfg(feature = "blake3")]
mod tcp;
#[cfg(feature = "blake3")]
pub use tcp::{PreparedTcp, StreamFactory, StreamOptions, TcpPreparation};
