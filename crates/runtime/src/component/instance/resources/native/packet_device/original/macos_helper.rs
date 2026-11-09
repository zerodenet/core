//! The original nonroot creation duty, registered in the same actual Scope.
use super::*;
use std::sync::mpsc;
use tokio::sync::oneshot;
use zero_tun::{packet_memory::PacketMemoryView, MacosHelperAccess};
#[derive(Default)]
pub(super) struct MacosState {
    sender: Option<mpsc::SyncSender<Request>>,
    dedicated: bool,
    requested: bool,
    failure: Option<Arc<ChannelError>>,
    cleanup_failure: Option<Arc<ChannelError>>,
    actor_failure: Option<Arc<ChannelError>>,
}
struct Request {
    binding: PacketDeviceBinding,
    component: ComponentId,
    result: oneshot::Sender<Result<NativeView, ChannelError>>,
}
pub(in super::super) struct MacosWork {
    original: Arc<Original>,
    receiver: mpsc::Receiver<Request>,
    component: ComponentId,
}
struct Pending<'a> {
    original: &'a Original,
    received: bool,
}
impl Drop for Pending<'_> {
    fn drop(&mut self) {
        if !self.received {
            self.original.stop_macos();
        }
    }
}
impl Original {
    pub(in super::super) fn helper_job_original(&self) -> Arc<zero_tun::AdoptedTunOwner> {
        self.owner.clone()
    }

    pub(in super::super) fn receive_helper_actor_failure(&self, source: Arc<ChannelError>) -> bool {
        let mut state = self
            .macos
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(actual) = &state.actor_failure {
            return Arc::ptr_eq(actual, &source);
        }
        state.actor_failure = Some(source);
        true
    }
    pub(in super::super) fn helper_actor_failure_received(
        &self,
        source: &Arc<ChannelError>,
    ) -> bool {
        self.macos
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .actor_failure
            .as_ref()
            .is_some_and(|actual| Arc::ptr_eq(actual, source))
    }

    pub(in super::super) fn prepare_macos_helper(
        self: &Arc<Self>,
        memory: &PacketMemoryView,
        output_limit: usize,
        request_capacity: usize,
        dedicated: bool,
        component: ComponentId,
    ) -> std::io::Result<(MacosHelperAccess, Option<MacosWork>)> {
        let access =
            self.owner
                .prepare_macos_helper(memory, output_limit, dedicated, request_capacity)?;
        let mut state = self
            .macos
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.dedicated = dedicated;
        let work = if dedicated {
            let (sender, receiver) = mpsc::sync_channel(1);
            state.sender = Some(sender);
            Some(MacosWork {
                original: self.clone(),
                receiver,
                component,
            })
        } else {
            None
        };
        Ok((access, work))
    }
    pub(in super::super) fn helper_access(&self) -> Option<MacosHelperAccess> {
        self.owner.macos_helper()
    }
    pub(in super::super) fn stop_macos(&self) {
        let sender = self
            .macos
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .sender
            .take();
        drop(sender);
        self.owner.stop_macos_helper();
    }
    pub(in super::super) fn observe_helper_driver_returned(&self) {
        self.owner.observe_macos_helper_driver_returned();
    }
    pub(in super::super) fn helper_closed(&self) -> bool {
        self.owner.macos_helper_closed()
    }
    pub(super) async fn acquire_macos(
        &self,
        binding: &PacketDeviceBinding,
        component: ComponentId,
    ) -> Result<NativeView, ChannelError> {
        let sender = {
            let mut state = self
                .macos
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !state.dedicated {
                drop(state);
                return self.acquire_unix(binding, component);
            }
            if state.requested {
                return Err(error(
                    component,
                    Reason::Revoked,
                    "same helper creation receipt already issued",
                ));
            }
            state.requested = true;
            state.sender.take().ok_or_else(|| {
                error(
                    component,
                    Reason::Revoked,
                    "original helper registration was stopped",
                )
            })?
        };
        let (result, received) = oneshot::channel();
        let mut pending = Pending {
            original: self,
            received: false,
        };
        sender
            .send(Request {
                binding: binding.clone(),
                component,
                result,
            })
            .map_err(|_| {
                error(
                    component,
                    Reason::Revoked,
                    "actual helper creation duty is no longer receiving",
                )
            })?;
        drop(sender);
        let result = received.await.map_err(|source| {
            error(
                component,
                Reason::Io,
                "same helper acquisition receipt sender failed",
            )
            .with_source(source)
        })?;
        pending.received = true;
        result
    }
    pub(super) fn acquire_unix(
        &self,
        binding: &PacketDeviceBinding,
        component: ComponentId,
    ) -> Result<NativeView, ChannelError> {
        // A created Kernel TUN must have its actual prepared helper context;
        // no unscoped/global authorization fallback is available here.
        let access = self.owner.macos_helper();
        if matches!(binding, PacketDeviceBinding::Create { .. }) && access.is_none() {
            return Err(error(
                component,
                Reason::Internal,
                "created utun has no same-owner helper context",
            ));
        }
        let _context = access
            .as_ref()
            .map(MacosHelperAccess::enter)
            .transpose()
            .map_err(|source| {
                error(
                    component,
                    Reason::Io,
                    "same helper context installation failed",
                )
                .with_source(source)
            })?;
        let acquired = match binding {
            PacketDeviceBinding::Descriptor { fd, interface } => self.owner.acquire(*fd, interface),
            PacketDeviceBinding::Create { name, .. } => self.owner.create(name.as_deref()),
            PacketDeviceBinding::ExistingWindows { .. } => {
                return Err(error(
                    component,
                    Reason::Unsupported,
                    "Windows original unavailable on macOS",
                ))
            }
        };
        let io = acquired.map_err(|source| {
            let unknown = matches!(source.outcome, zero_tun::TunAcquisitionOutcome::Unknown(_));
            let mut failure =
                error(component, Reason::Io, "actual utun acquisition failed").with_source(source);
            if unknown {
                failure.delivery = zero_platform_tokio::contract::DeliveryState::Unknown;
            }
            failure
        })?;
        *self
            .issued
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(io.owned_identity().clone());
        self.configure_and_wrap(io, binding, component)
    }
}
impl MacosWork {
    pub(in super::super) fn run(self) -> Result<(), ChannelError> {
        if let Ok(request) = self.receiver.recv() {
            let result = self
                .original
                .acquire_unix(&request.binding, request.component)
                .map_err(Arc::new);
            if let Err(source) = &result {
                self.original
                    .macos
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .failure = Some(source.clone());
            }
            // Canonical source is strong in its original before the oneshot
            // send can invoke any external wake or cleanup continuation waits.
            let _ = request.result.send(result.map_err(ChannelError::retained));
        }
        if let Err(source) = self.original.owner.wait_macos_helper() {
            let source = Arc::new(
                error(
                    self.component,
                    Reason::Io,
                    "actual helper duty retained its cleanup source",
                )
                .with_source(source),
            );
            self.original
                .macos
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .cleanup_failure = Some(source);
        }
        let state = self
            .original
            .macos
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match (&state.failure, &state.cleanup_failure) {
            (None, None) => Ok(()),
            (Some(source), None) | (None, Some(source)) => {
                Err(ChannelError::retained(source.clone()))
            }
            (Some(acquisition), Some(cleanup)) => Err(error(
                self.component,
                Reason::Io,
                "original helper acquisition and cleanup faults",
            )
            .with_source(HelperFaults {
                acquisition: acquisition.clone(),
                cleanup: cleanup.clone(),
            })),
        }
    }
}
#[derive(Debug)]
struct HelperFaults {
    acquisition: Arc<ChannelError>,
    cleanup: Arc<ChannelError>,
}
impl std::fmt::Display for HelperFaults {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}; {}", self.acquisition, self.cleanup)
    }
}
impl std::error::Error for HelperFaults {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.acquisition.as_ref())
    }
}
