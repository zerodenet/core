//! The existing preconnection registry under its real Host original owner.
mod control;
mod factory;
mod handoff;
mod native;
mod options;
pub use factory::PoolFactory;
pub(crate) use native::Native;
pub use options::PreparedPreconnect;

pub(super) struct Opened {
    pub(super) io: zero_platform_tokio::TcpRelayStream,
    pub(super) control: zero_platform_tokio::contract::lifecycle::BoxChannelControl,
    pub(super) original: zero_platform_tokio::controlled_carrier::CarrierOrigin,
}
