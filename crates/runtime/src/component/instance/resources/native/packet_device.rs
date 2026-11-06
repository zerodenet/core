//! One concrete acquired device original. Native I/O never escapes twice.
use super::*;
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::contract::{
    component::{GrantedPacketDevice, PacketDeviceBinding, PacketDeviceIdentity},
    lifecycle::{CloseReport, Unfinished},
    scope::{TaskJoinWitness, TaskScopeIdentity},
};
mod close;
#[cfg(feature = "raw-ip-runtime")]
mod memory;
#[cfg(feature = "raw-ip-runtime")]
mod original;
#[cfg(feature = "raw-ip-runtime")]
mod view;
#[cfg(all(
    test,
    feature = "raw-ip-runtime",
    any(target_os = "linux", target_os = "macos")
))]
use zero_tun::TunAcquisitionOutcome;
#[cfg(feature = "raw-ip-runtime")]
use zero_tun::{OwnedTunIdentity, TunCloseReceipt};

pub(super) struct PacketDeviceOwner {
    pub(super) ordinal: usize,
    name: Arc<str>,
    binding: PacketDeviceBinding,
    identity: PacketDeviceIdentity,
    cancel: CancellationToken,
    state: Arc<Mutex<State>>,
    #[cfg(feature = "raw-ip-runtime")]
    original: Arc<original::Original>,
    #[cfg(feature = "raw-ip-runtime")]
    memory: memory::Memory,
}
#[derive(Default)]
struct State {
    closing: bool,
    attempted: bool,
    view_live: bool,
    driver: Option<TaskJoinWitness>,
    close_fault: Option<Arc<ChannelError>>,
    original: Option<zero_platform_tokio::controlled_carrier::PacketProviderRegistration>,
    #[cfg(feature = "raw-ip-runtime")]
    reported_close: bool,
    #[cfg(feature = "raw-ip-runtime")]
    issued: Option<OwnedTunIdentity>,
    #[cfg(feature = "raw-ip-runtime")]
    receipt: Option<Arc<TunCloseReceipt>>,
}
#[derive(Clone, Debug)]
pub(in crate::component) struct PacketDeviceEvidence {
    pub ordinal: usize,
    // Only this module can construct/qualify these facts from its same original.
    confirmed: bool,
    pub close_fault: Option<Arc<ChannelError>>,
}
impl PacketDeviceEvidence {
    pub(in crate::component) fn physical_confirmed(&self) -> bool {
        self.confirmed
    }
}
impl PacketDeviceOwner {
    pub(super) fn new(
        ordinal: usize,
        instance: ComponentInstanceId,
        scope: TaskScopeIdentity,
        name: Arc<str>,
        binding: PacketDeviceBinding,
        cancel: CancellationToken,
        originals: Arc<crate::HostOriginalBudgets>,
        original_tasks: Option<zero_platform_tokio::contract::scope::WeakTaskRegistrar>,
    ) -> Result<Self, ChannelError> {
        if name.is_empty() {
            return Err(error(
                instance.component,
                Reason::InvalidConfig,
                "empty Packet resource name",
            ));
        }
        if !cfg!(feature = "raw-ip-runtime") {
            return Err(error(
                instance.component,
                Reason::Unsupported,
                "concrete Packet provider unavailable",
            ));
        }
        #[cfg(feature = "raw-ip-runtime")]
        let original = Arc::new(
            original::Original::new(&binding)
                .map_err(|message| error(instance.component, Reason::InvalidConfig, message))?,
        );
        #[cfg(not(feature = "raw-ip-runtime"))]
        let _ = (originals, original_tasks);
        Ok(Self {
            ordinal,
            name,
            binding,
            identity: PacketDeviceIdentity::new(instance, scope),
            cancel,
            state: Arc::new(Mutex::new(State::default())),
            #[cfg(feature = "raw-ip-runtime")]
            original,
            #[cfg(feature = "raw-ip-runtime")]
            memory: memory::Memory::new(originals, original_tasks),
        })
    }
    pub(super) fn original_registration(
        &self,
    ) -> Option<zero_platform_tokio::controlled_carrier::PacketProviderRegistration> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .original
            .clone()
    }
    pub(super) fn name(&self) -> &str {
        &self.name
    }
    pub(super) async fn open(
        &self,
        driver: TaskJoinWitness,
    ) -> Result<GrantedPacketDevice, ChannelError> {
        // Query the original scope/witness outside every provider/resource lock.
        if !self.identity.scope().resource_open()
            || driver.is_joined()
            || !driver.scope_identity().same_scope(self.identity.scope())
            || self.cancel.is_cancelled()
        {
            return Err(error(
                self.identity.instance().component,
                Reason::Revoked,
                "Packet driver is not in this actual open Scope",
            ));
        }
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.closing || state.attempted {
                return Err(error(
                    self.identity.instance().component,
                    Reason::Revoked,
                    "Packet native reservation was already consumed",
                ));
            }
            state.attempted = true;
            state.driver = Some(driver.clone());
        }
        #[cfg(feature = "raw-ip-runtime")]
        {
            // Deferral covers the real SDK task registration and witness
            // installation only. Never hold hosted callbacks across await.
            let native_read = {
                let _callbacks = self.memory_callbacks();
                self.memory
                    .prepare(&self.original, self.identity.instance().component)?
            };
            if self.cancel.is_cancelled() || !self.identity.scope().resource_open() {
                self.memory.stop(&self.original);
                return Err(error(
                    self.identity.instance().component,
                    Reason::Revoked,
                    "Packet original Scope closed before native acquisition",
                ));
            }
            let acquired = self
                .original
                .acquire(&self.binding, self.identity.instance().component)
                .await;
            // The worker and original owner survive cancellation of this
            // await. Before publishing its unique view, recheck the actual
            // source/driver scope after the native receipt became available.
            let revoked = self.cancel.is_cancelled()
                || !self.identity.scope().resource_open()
                || driver.is_joined();
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.issued = self.original.issued();
            match acquired {
                Ok(native) => {
                    if revoked || state.closing {
                        drop(state);
                        drop(native);
                        self.memory.stop(&self.original);
                        return Err(error(
                            self.identity.instance().component,
                            Reason::Revoked,
                            "Packet original Scope closed during acquisition",
                        ));
                    }
                    let original = zero_platform_tokio::controlled_carrier::PacketProviderRegistration::from_acquired(
                        self.identity.clone(),
                        state.issued.as_ref().expect("same acquired concrete native identity").clone(),
                        driver,
                        self.cancel.clone(),
                    )?;
                    state.original = Some(original.clone());
                    state.view_live = true;
                    Ok(GrantedPacketDevice {
                        io: Box::pin(view::View::new(native.io, self.state.clone())),
                        identity: self.identity.clone(),
                        original,
                        interface: native.interface,
                        native_read,
                    })
                }
                Err(source) => Err(source),
            }
        }
        #[cfg(not(feature = "raw-ip-runtime"))]
        Err(error(
            self.identity.instance().component,
            Reason::Unsupported,
            "concrete Packet provider unavailable",
        ))
    }
}
#[cfg(feature = "raw-ip-runtime")]
#[derive(Debug)]
struct NativeSource(Arc<std::io::Error>);
#[cfg(feature = "raw-ip-runtime")]
impl std::fmt::Display for NativeSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
#[cfg(feature = "raw-ip-runtime")]
impl std::error::Error for NativeSource {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

#[cfg(all(
    test,
    feature = "raw-ip-runtime",
    any(target_os = "linux", target_os = "macos")
))]
mod tests;
