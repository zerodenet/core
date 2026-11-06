//! One concrete native Packet root in the existing canonical cold union.
mod alias;
mod cold;
mod factory;
mod owner;
#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests;
use super::*;
pub(in crate::adapters::direct) use alias::Alias;
use std::sync::{Mutex, OnceLock, Weak};
use zero_core::component::{ComponentId, ComponentInstanceId};
use zero_platform_tokio::contract::{
    component::{ComponentSpec, PacketDeviceBinding, ResourceDeclaration, ResourceOwner, RoleKind},
    packet::DynPacketOutbound,
    scope::TaskScopeIdentity,
    ChannelError, Impact, Reason, Stage,
};
const NAMESPACE: &str = "zero.direct.packet.original.v1";
const NATIVE_NAME: &str = "host.packet.direct";
pub(in crate::adapters::direct) struct Candidate {
    config: Weak<zero_config::RuntimeConfig>,
    id: ComponentId,
    root: Arc<Options>,
}
struct Options {
    config: zero_config::DirectPacketDeviceConfig,
    mtu: u16,
    stack: crate::runtime::raw_ip::RawIpStackOptions,
    spec: ComponentSpec,
    role: OnceLock<Weak<Outbound>>,
    scope: Mutex<Option<TaskScopeIdentity>>,
}
struct Shared {
    device: OnceLock<Arc<HostPacketDevice>>,
}
struct Outbound(Arc<Shared>);
impl Options {
    fn device(&self) -> Option<Arc<HostPacketDevice>> {
        self.role.get()?.upgrade()?.0.device.get().cloned()
    }
}
fn error(reason: Reason, message: &'static str) -> ChannelError {
    ChannelError::new(Stage::Prepare, reason, Impact::Component)
        .with_source(std::io::Error::other(message))
}
