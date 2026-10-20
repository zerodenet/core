//! The runtime phase guard around the single canonical native datagram owner.
use super::*;
use crate::component::instance::HostDatagramListenerOwner;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Mutex as AsyncMutex;
use zero_platform_tokio::contract::{
    component::GrantedDatagramListener,
    lifecycle::{CloseReport, ResourceLifecycle},
    CloseReason, Impact, Reason, Stage,
};

pub(in crate::runtime::inbound_owners::bind) struct NativeDatagramGrant {
    identity: ComponentInstanceId,
    address: std::net::SocketAddr,
    native: AsyncMutex<HostDatagramListenerOwner>,
    cancel: CancellationToken,
    bound: AtomicBool,
}
impl NativeDatagramGrant {
    pub(super) fn new(
        identity: ComponentInstanceId,
        declaration: &ResourceDeclaration,
        address: std::net::SocketAddr,
        attempts: NonZeroUsize,
        cancel: CancellationToken,
    ) -> Result<Self, ChannelError> {
        Ok(Self {
            identity,
            address,
            native: AsyncMutex::new(HostDatagramListenerOwner::new(
                identity.component,
                declaration,
                attempts,
            )?),
            cancel,
            bound: AtomicBool::new(false),
        })
    }
    pub(super) fn bind(&self) -> Result<GrantedDatagramListener, EngineError> {
        if self.cancel.is_cancelled() {
            return Err(crate::runtime::inbound_owners::preparing::stopped_preparation());
        }
        let mut native = self
            .native
            .try_lock()
            .map_err(|_| std::io::Error::other("native datagram owner is already closing"))?;
        // The unique native ledger was registered before this synchronous call;
        // no await or post-bind cancellation can lose the original socket.
        let view = native
            .bind_listener(self.address)
            .map_err(std::io::Error::other)?;
        self.bound.store(true, Ordering::Release);
        Ok(view)
    }
}

pub(super) struct DatagramResource(pub(super) Arc<NativeDatagramGrant>);
impl ResourceLifecycle for DatagramResource {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        if self.0.cancel.is_cancelled() || !self.0.bound.load(Ordering::Acquire) {
            let mut error = ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Component);
            error.component = Some(self.0.identity.component);
            return Err(error);
        }
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        self.0.cancel.cancel();
        // Bind is synchronous, so it cannot hold this mutex over a suspension.
        // An in-progress shutdown already revoked the original views.
        if let Ok(mut native) = self.0.native.try_lock() {
            native.revoke();
        }
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.0.cancel.cancel();
        let mut native = self.0.native.lock().await;
        native.revoke();
        native.shutdown(deadline)
    }
}
