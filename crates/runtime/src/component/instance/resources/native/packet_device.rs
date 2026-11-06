//! One concrete acquired device original. Native I/O never escapes twice.
use super::*;
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::contract::{
    component::{GrantedPacketDevice, PacketDeviceBinding, PacketDeviceIdentity},
    lifecycle::{CloseReport, Unfinished},
    scope::{TaskJoinWitness, TaskScopeIdentity},
};
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
        Ok(Self {
            ordinal,
            name,
            binding,
            identity: PacketDeviceIdentity::new(instance, scope),
            cancel,
            state: Arc::new(Mutex::new(State::default())),
            #[cfg(feature = "raw-ip-runtime")]
            original,
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
    pub(super) fn open(
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
        #[cfg(feature = "raw-ip-runtime")]
        {
            let acquired = self
                .original
                .acquire(&self.binding, self.identity.instance().component);
            state.issued = self.original.issued();
            match acquired {
                Ok(native) => {
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
    pub(super) fn evidence(&self) -> PacketDeviceEvidence {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let joined = state.driver.as_ref().is_none_or(TaskJoinWitness::is_joined);
        let mut confirmed = state.closing && !state.view_live && joined;
        #[cfg(feature = "raw-ip-runtime")]
        {
            confirmed &= self.original.physical_confirmed(state.issued.as_ref());
        }
        #[cfg(not(feature = "raw-ip-runtime"))]
        {
            confirmed = false;
        }
        PacketDeviceEvidence {
            ordinal: self.ordinal,
            confirmed,
            close_fault: state.close_fault.clone(),
        }
    }
    pub(super) async fn shutdown(&self, deadline: Instant) -> CloseReport {
        let driver = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.closing = true;
            state.driver.clone()
        };
        if let Some(driver) = driver {
            if tokio::time::timeout_at(deadline, driver.wait())
                .await
                .is_err()
            {
                return self.unfinished("same Packet driver join is pending");
            }
        }
        // Native close follows the consumed original Scope join, never body-done.
        if self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .view_live
        {
            return self
                .unfinished("unique Packet I/O view has not returned after its driver join");
        }
        #[cfg(feature = "raw-ip-runtime")]
        {
            let closed = tokio::time::timeout_at(deadline, self.original.close()).await;
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match closed {
                Err(_) => return self.unfinished_locked("original native Packet close is pending"),
                Ok(Err(source)) => {
                    state.close_fault.get_or_insert_with(|| {
                        Arc::new(
                            error(
                                self.identity.instance().component,
                                Reason::Io,
                                "original Packet close retained its actual source",
                            )
                            .with_source(NativeSource(source)),
                        )
                    });
                }
                Ok(Ok(receipt)) => state.receipt = receipt,
            }
            if state.receipt.is_none() {
                state.receipt = self.original.receipt();
            }
            if !self.original.physical_confirmed(state.issued.as_ref()) {
                return self.unfinished_locked("original Packet native close remains unconfirmed");
            }
            let mut report = CloseReport::default();
            if state.receipt.is_some() && !state.reported_close {
                report.closed_io.push(format!(
                    "component {} original Packet device explicitly closed",
                    self.identity.instance().component.0
                ));
                state.reported_close = true;
            }
            report
        }
        #[cfg(not(feature = "raw-ip-runtime"))]
        self.unfinished("concrete Packet provider unavailable")
    }
    fn unfinished(&self, message: &str) -> CloseReport {
        self.unfinished_locked(message)
    }
    fn unfinished_locked(&self, message: &str) -> CloseReport {
        CloseReport {
            unfinished: vec![Unfinished::Resource(format!(
                "component {} Packet device {}: {message}",
                self.identity.instance().component.0,
                self.name
            ))],
            ..Default::default()
        }
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
