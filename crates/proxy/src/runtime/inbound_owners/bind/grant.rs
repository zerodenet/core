use super::*;
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        OnceLock,
    },
};
use tokio::sync::Mutex as AsyncMutex;
use zero_platform_tokio::{
    contract::{
        component::ResourceDeclaration,
        lifecycle::{CloseReport, ResourceLifecycle},
        CancellationToken, ChannelError, CloseReason, Impact, Reason, Stage,
    },
    ControlledTcpListener, ControlledTcpListenerOwner,
};

pub(super) struct NativeTcpGrant {
    identity: ComponentInstanceId,
    addresses: OnceLock<Arc<[SocketAddr]>>,
    destination: zero_core::request::Destination,
    resolution: OnceLock<(
        zero_core::component::TaskId,
        zero_platform_tokio::contract::scope::TaskJoinWitness,
    )>,
    resolved: OnceLock<Result<Arc<[SocketAddr]>, crate::component::instance::SharedError>>,
    resolution_limit: usize,
    native: AsyncMutex<ControlledTcpListenerOwner>,
    cancel: CancellationToken,
    bound: AtomicBool,
    accepted: Option<zero_platform_tokio::AcceptedTcpTaskReceiver>,
}
mod resolution;
impl NativeTcpGrant {
    #[cfg(test)]
    pub(super) fn accepted_evidence(&self) -> Option<zero_platform_tokio::AcceptedTcpEvidence> {
        self.native.try_lock().ok()?.accepted_evidence()
    }

    pub(super) fn new(
        identity: ComponentInstanceId,
        declaration: &ResourceDeclaration,
        address: Option<SocketAddr>,
        max_bind_attempts: NonZeroUsize,
        cancel: CancellationToken,
        registrar: &zero_platform_tokio::contract::scope::TaskRegistrar,
    ) -> Result<Self, ChannelError> {
        let ResourceDeclaration::TcpListener {
            destination,
            max_bind_attempts: ceiling,
            ..
        } = declaration
        else {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::InvalidConfig,
                Impact::Component,
            ));
        };
        let native = match declaration {
            ResourceDeclaration::TcpListener {
                accepted: Some(limits),
                ..
            } if address.is_none() => {
                ControlledTcpListenerOwner::new_resolving_with_accepted_graph(
                    declaration,
                    *ceiling,
                    cancel.clone(),
                    limits.max_sockets,
                    limits.max_tasks_per_socket,
                    registrar,
                )?
            }
            ResourceDeclaration::TcpListener {
                accepted: Some(limits),
                ..
            } => ControlledTcpListenerOwner::new_with_accepted_graph(
                declaration,
                max_bind_attempts,
                cancel.clone(),
                limits.max_sockets,
                limits.max_tasks_per_socket,
                registrar,
            )?,
            _ => ControlledTcpListenerOwner::new(declaration, max_bind_attempts, cancel.clone())?,
        };
        let accepted = native.accepted_task_receiver();
        let addresses = OnceLock::new();
        if let Some(address) = address {
            let _ = addresses.set(Arc::from([address]));
        }
        Ok(Self {
            identity,
            addresses,
            destination: destination.clone(),
            resolution: OnceLock::new(),
            resolved: OnceLock::new(),
            resolution_limit: ceiling.get(),
            native: AsyncMutex::new(native),
            accepted,
            cancel,
            bound: AtomicBool::new(false),
        })
    }
    pub(super) fn bind_original_budgets(
        &self,
        roots: &Arc<zero_runtime::HostOriginalBudgets>,
        tasks: &zero_platform_tokio::contract::scope::TaskRegistrar,
    ) -> Result<(), ChannelError> {
        let mut native = self
            .native
            .try_lock()
            .map_err(|_| ChannelError::new(Stage::Prepare, Reason::Capacity, Impact::Component))?;
        native.bind_original_totals(self.identity, tasks, &roots.carrier)
    }
    pub(super) fn accepted_task_receiver(
        &self,
    ) -> Option<zero_platform_tokio::AcceptedTcpTaskReceiver> {
        self.accepted.clone()
    }
    pub(super) fn bind(&self) -> Result<ControlledTcpListener, EngineError> {
        if self.cancel.is_cancelled() {
            return Err(super::super::preparing::stopped_preparation());
        }
        if self.needs_resolution()
            && !self
                .resolution
                .get()
                .is_some_and(|(_, witness)| witness.is_joined())
        {
            return Err(self.error(
                Reason::InvalidConfig,
                "listener lookup has no original Scope join receipt",
            ));
        }
        let mut native = self.native.try_lock().map_err(|_| {
            if self.cancel.is_cancelled() {
                super::super::preparing::stopped_preparation()
            } else {
                self.error(Reason::Revoked, "native listener is already closing")
            }
        })?;
        let addresses = self.addresses.get().ok_or_else(|| {
            self.error(
                Reason::InvalidConfig,
                "listener resolution is not confirmed",
            )
        })?;
        let mut last = None;
        for address in addresses.iter() {
            match native.bind_listener(*address) {
                Ok(listener) => {
                    self.bound.store(true, Ordering::Release);
                    return Ok(listener);
                }
                Err(error) if error.reason == Reason::Revoked && self.cancel.is_cancelled() => {
                    // The platform's explicit pre-acquisition revocation, not an
                    // OS bind/registration/close error (those retain Reason::Io).
                    return Err(super::super::preparing::stopped_preparation());
                }
                Err(mut error) => {
                    error.component.get_or_insert(self.identity.component);
                    if native.remaining_bind_attempts() == 0 {
                        // Preserve the actual last OS/native error; an outer
                        // retry must not replace it with synthetic Capacity.
                        error.retry = zero_platform_tokio::contract::RetryClass::Never;
                    }
                    let retry = error.retry_permitted();
                    last = Some(error);
                    if !retry || native.remaining_bind_attempts() == 0 {
                        break;
                    }
                }
            }
        }
        Err(std::io::Error::other(last.expect("one frozen listener candidate")).into())
    }
    pub(super) async fn close_original(
        &self,
        deadline: Instant,
    ) -> Result<CloseReport, ChannelError> {
        self.native.lock().await.wait_closed(deadline).await
    }
    fn error(&self, reason: Reason, message: &'static str) -> EngineError {
        let mut error = ChannelError::new(Stage::Prepare, reason, Impact::Component)
            .with_source(std::io::Error::other(message));
        error.component = Some(self.identity.component);
        std::io::Error::other(error).into()
    }
}
pub(super) struct TcpResource(pub(super) Arc<NativeTcpGrant>);
impl ResourceLifecycle for TcpResource {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        if self.0.cancel.is_cancelled() || !self.0.bound.load(Ordering::Acquire) {
            let mut error = ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Component);
            error.component = Some(self.0.identity.component);
            return Err(error);
        }
        Ok(()) // The actual native acquisition already completed; no new I/O.
    }
    fn revoke(&self, _: CloseReason) {
        self.0.cancel.cancel();
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.0.cancel.cancel();
        self.0
            .native
            .lock()
            .await
            .wait_closed(deadline)
            .await
            .map_err(|mut error| {
                error.component.get_or_insert(self.0.identity.component);
                error
            })
    }
}
