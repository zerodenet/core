//! One concrete borrowed descriptor original. Native I/O never escapes twice.
use super::*;
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::contract::{
    component::{GrantedPacketDevice, PacketDeviceBinding, PacketDeviceIdentity},
    lifecycle::{CloseReport, Unfinished},
    scope::{TaskJoinWitness, TaskScopeIdentity},
};
#[cfg(all(
    feature = "raw-ip-runtime",
    any(target_os = "linux", target_os = "macos")
))]
mod view;
#[cfg(all(
    feature = "raw-ip-runtime",
    any(target_os = "linux", target_os = "macos")
))]
use zero_tun::{AdoptedTunOwner, OwnedTunIdentity, TunAcquisitionOutcome, TunCloseReceipt};

pub(super) struct PacketDeviceOwner {
    ordinal: usize,
    name: Arc<str>,
    binding: PacketDeviceBinding,
    identity: PacketDeviceIdentity,
    cancel: CancellationToken,
    state: Arc<Mutex<State>>,
    #[cfg(all(
        feature = "raw-ip-runtime",
        any(target_os = "linux", target_os = "macos")
    ))]
    original: Arc<AdoptedTunOwner>,
}
#[derive(Default)]
struct State {
    closing: bool,
    attempted: bool,
    view_live: bool,
    driver: Option<TaskJoinWitness>,
    close_fault: Option<Arc<ChannelError>>,
    #[cfg(all(
        feature = "raw-ip-runtime",
        any(target_os = "linux", target_os = "macos")
    ))]
    reported_close: bool,
    #[cfg(all(
        feature = "raw-ip-runtime",
        any(target_os = "linux", target_os = "macos")
    ))]
    issued: Option<OwnedTunIdentity>,
    #[cfg(all(
        feature = "raw-ip-runtime",
        any(target_os = "linux", target_os = "macos")
    ))]
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
        if !cfg!(all(
            feature = "raw-ip-runtime",
            any(target_os = "linux", target_os = "macos")
        )) {
            return Err(error(
                instance.component,
                Reason::Unsupported,
                "concrete descriptor provider is unavailable",
            ));
        }
        let PacketDeviceBinding::Descriptor { fd, interface } = &binding;
        if name.is_empty() || *fd < 0 || interface.is_empty() {
            return Err(error(
                instance.component,
                Reason::InvalidConfig,
                "invalid borrowed Packet descriptor binding",
            ));
        }
        Ok(Self {
            ordinal,
            name,
            binding,
            identity: PacketDeviceIdentity::new(instance, scope),
            cancel,
            state: Arc::new(Mutex::new(State::default())),
            #[cfg(all(
                feature = "raw-ip-runtime",
                any(target_os = "linux", target_os = "macos")
            ))]
            original: AdoptedTunOwner::new(),
        })
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
        state.driver = Some(driver);
        #[cfg(all(
            feature = "raw-ip-runtime",
            any(target_os = "linux", target_os = "macos")
        ))]
        {
            let PacketDeviceBinding::Descriptor { fd, interface } = &self.binding;
            match self.original.acquire(*fd, interface) {
                Ok(io) => {
                    state.issued = Some(io.owned_identity().clone());
                    state.view_live = true;
                    let granted = GrantedPacketDevice {
                        io: Box::pin(view::View::new(Box::pin(io), self.state.clone())),
                        identity: self.identity.clone(),
                        interface: interface.clone(),
                    };
                    return Ok(granted);
                }
                Err(source) => {
                    let failure = error(
                        self.identity.instance().component,
                        Reason::Io,
                        "original Packet descriptor acquisition failed",
                    )
                    .with_source(source);
                    // Same controller retains its acquisition source. KnownNoNative
                    // must not become an unrelated sticky cleanup failure.
                    return Err(failure);
                }
            }
        }
        #[cfg(not(all(
            feature = "raw-ip-runtime",
            any(target_os = "linux", target_os = "macos")
        )))]
        {
            let _ = &self.binding;
            Err(error(
                self.identity.instance().component,
                Reason::Unsupported,
                "concrete descriptor provider is unavailable",
            ))
        }
    }
    pub(super) fn evidence(&self) -> PacketDeviceEvidence {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let joined = state.driver.as_ref().is_none_or(TaskJoinWitness::is_joined);
        let mut confirmed = state.closing && !state.view_live && joined;
        #[cfg(all(
            feature = "raw-ip-runtime",
            any(target_os = "linux", target_os = "macos")
        ))]
        {
            confirmed &= match self.original.outcome() {
                TunAcquisitionOutcome::KnownNoNative => true,
                TunAcquisitionOutcome::Closed(receipt) => state
                    .issued
                    .as_ref()
                    .is_none_or(|issued| receipt.identity().same_owner(issued)),
                TunAcquisitionOutcome::Unknown(_) => false,
            };
        }
        #[cfg(not(all(
            feature = "raw-ip-runtime",
            any(target_os = "linux", target_os = "macos")
        )))]
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
        #[cfg(all(
            feature = "raw-ip-runtime",
            any(target_os = "linux", target_os = "macos")
        ))]
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let receipt = match self.original.outcome() {
                TunAcquisitionOutcome::KnownNoNative => None,
                TunAcquisitionOutcome::Closed(receipt) => Some(receipt),
                TunAcquisitionOutcome::Unknown(_) => match self.original.close() {
                    Ok(receipt) => Some(receipt),
                    Err(source) => {
                        state.close_fault.get_or_insert_with(|| {
                            Arc::new(
                                error(
                                    self.identity.instance().component,
                                    Reason::Io,
                                    "original Packet native close remains unconfirmed",
                                )
                                .with_source(NativeSource(source)),
                            )
                        });
                        return self
                            .unfinished_locked("original Packet native close remains unconfirmed");
                    }
                },
            };
            if let Some(receipt) = receipt {
                if state
                    .issued
                    .as_ref()
                    .is_some_and(|issued| !receipt.identity().same_owner(issued))
                {
                    return self.unfinished_locked(
                        "native Packet close receipt differs from the issued original",
                    );
                }
                state.receipt = Some(receipt);
            }
            let mut report = CloseReport::default();
            if state.receipt.is_some() && !state.reported_close {
                report.closed_io.push(format!(
                    "component {} original Packet descriptor explicitly closed",
                    self.identity.instance().component.0
                ));
                state.reported_close = true;
            }
            return report;
        }
        #[cfg(not(all(
            feature = "raw-ip-runtime",
            any(target_os = "linux", target_os = "macos")
        )))]
        self.unfinished("concrete descriptor provider is unavailable")
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
#[cfg(all(
    feature = "raw-ip-runtime",
    any(target_os = "linux", target_os = "macos")
))]
#[derive(Debug)]
struct NativeSource(Arc<std::io::Error>);
#[cfg(all(
    feature = "raw-ip-runtime",
    any(target_os = "linux", target_os = "macos")
))]
impl std::fmt::Display for NativeSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
#[cfg(all(
    feature = "raw-ip-runtime",
    any(target_os = "linux", target_os = "macos")
))]
impl std::error::Error for NativeSource {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
