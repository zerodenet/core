//! The runtime phase guard around the single canonical native datagram owner.
use super::*;
use crate::component::instance::HostDatagramListenerOwner;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex,
};
use zero_platform_tokio::contract::{
    component::GrantedDatagramListener,
    lifecycle::{CloseReport, ResourceLifecycle},
    CloseReason, Impact, Reason, Stage,
};

pub(in crate::runtime::inbound_owners::bind) struct NativeDatagramGrant {
    identity: ComponentInstanceId,
    address: std::net::SocketAddr,
    native: Mutex<HostDatagramListenerOwner>,
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
            native: Mutex::new(HostDatagramListenerOwner::new(
                identity.component,
                declaration,
                attempts,
            )?),
            cancel,
            bound: AtomicBool::new(false),
        })
    }
    pub(super) fn bind(&self) -> Result<GrantedDatagramListener, EngineError> {
        self.bind_after(|| {})
    }
    fn bind_after(&self, acquired: impl FnOnce()) -> Result<GrantedDatagramListener, EngineError> {
        if self.cancel.is_cancelled() {
            return Err(crate::runtime::inbound_owners::preparing::stopped_preparation());
        }
        let mut native = self.native.lock().expect("native datagram owner poisoned");
        if self.cancel.is_cancelled() {
            return Err(crate::runtime::inbound_owners::preparing::stopped_preparation());
        }
        // The unique native ledger was registered before this synchronous call;
        // no await or post-bind cancellation can lose the original socket.
        let view = native
            .bind_listener(self.address)
            .map_err(std::io::Error::other)?;
        self.bound.store(true, Ordering::Release);
        acquired();
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
        // Every native operation is synchronous. This same short lock makes
        // revoke linearize after an in-flight bind, including another thread.
        self.0
            .native
            .lock()
            .expect("native datagram owner poisoned")
            .revoke();
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.0.cancel.cancel();
        let mut native = self
            .0
            .native
            .lock()
            .expect("native datagram owner poisoned");
        native.revoke();
        native.shutdown(deadline)
    }
}

#[cfg(test)]
#[path = "udp/tests.rs"]
mod tests;
