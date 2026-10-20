use super::*;
use std::{
    net::SocketAddr,
    sync::atomic::{AtomicBool, Ordering},
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
    address: SocketAddr,
    native: AsyncMutex<ControlledTcpListenerOwner>,
    cancel: CancellationToken,
    bound: AtomicBool,
}
impl NativeTcpGrant {
    pub(super) fn new(
        identity: ComponentInstanceId,
        declaration: &ResourceDeclaration,
        address: SocketAddr,
        max_bind_attempts: NonZeroUsize,
        cancel: CancellationToken,
    ) -> Result<Self, ChannelError> {
        Ok(Self {
            identity,
            address,
            native: AsyncMutex::new(ControlledTcpListenerOwner::new(
                declaration,
                max_bind_attempts,
                cancel.clone(),
            )?),
            cancel,
            bound: AtomicBool::new(false),
        })
    }
    pub(super) fn bind(&self) -> Result<ControlledTcpListener, EngineError> {
        if self.cancel.is_cancelled() {
            return Err(super::super::preparing::stopped_preparation());
        }
        let mut native = self.native.try_lock().map_err(|_| {
            if self.cancel.is_cancelled() {
                super::super::preparing::stopped_preparation()
            } else {
                self.error(Reason::Revoked, "native listener is already closing")
            }
        })?;
        let listener = match native.bind_listener(self.address) {
            Ok(listener) => listener,
            Err(error) if error.reason == Reason::Revoked && self.cancel.is_cancelled() => {
                // The platform's explicit pre-acquisition revocation, not an
                // OS bind/registration/close error (those retain Reason::Io).
                return Err(super::super::preparing::stopped_preparation());
            }
            Err(mut error) => {
                error.component.get_or_insert(self.identity.component);
                return Err(std::io::Error::other(error).into());
            }
        };
        // No post-bind cancellation check can drop the original FD: the same
        // native owner already retains it; only the accept view is transferred.
        self.bound.store(true, Ordering::Release);
        Ok(listener)
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
