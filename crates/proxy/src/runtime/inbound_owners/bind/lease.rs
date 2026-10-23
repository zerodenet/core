use super::*;
use crate::runtime::inbound_operation::PreparedInboundListenerOperation;
#[cfg(test)]
use zero_platform_tokio::ControlledTcpListener;

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct InboundBindContext {
    grant: Arc<native::NativeBindGrant>,
}
#[cfg(test)]
impl InboundBindContext {
    pub(crate) fn listen_tcp(&self) -> Result<ControlledTcpListener, EngineError> {
        self.grant.bind_tcp()
    }
}
pub(crate) struct InboundBindLease {
    registry: Weak<InboundOwnerRegistry>,
    tag: String,
    protocols: ProtocolInventory,
    prepared: Option<PreparedInboundBind>,
    owner: Arc<InboundOwner>,
    grant: Arc<native::NativeBindGrant>,
    shutdown: watch::Sender<bool>,
    listener: Option<NativeListenerView>,
    promoted: bool,
}
impl InboundBindLease {
    pub(super) fn new(
        registry: Weak<InboundOwnerRegistry>,
        tag: String,
        protocols: ProtocolInventory,
        prepared: PreparedInboundBind,
        owner: Arc<InboundOwner>,
        grant: Arc<native::NativeBindGrant>,
        shutdown: watch::Sender<bool>,
    ) -> Self {
        Self {
            registry,
            tag,
            protocols,
            prepared: Some(prepared),
            owner,
            grant,
            shutdown,
            listener: None,
            promoted: false,
        }
    }
    pub(crate) fn owner(&self) -> Arc<InboundOwner> {
        self.owner.clone()
    }
    pub(crate) fn shutdown_sender(&self) -> watch::Sender<bool> {
        self.shutdown.clone()
    }
    #[cfg(test)]
    pub(crate) fn context(&self) -> InboundBindContext {
        InboundBindContext {
            grant: self.grant.clone(),
        }
    }
    #[cfg(test)]
    pub(crate) fn bind_tcp(&mut self) -> Result<(), EngineError> {
        if self.listener.is_some() {
            return Err(io("listener lease already received its accept view"));
        }
        self.listener = Some(NativeListenerView::Tcp(self.context().listen_tcp()?));
        Ok(())
    }
    pub(crate) fn bind_native(&mut self) -> Result<(), EngineError> {
        if self.listener.is_some() {
            return Err(io("listener lease already received its native view"));
        }
        self.listener = Some(self.grant.bind()?);
        Ok(())
    }
    pub(crate) fn finish(self) -> Result<LeasedInbound, EngineError> {
        if self.listener.is_none() {
            return Err(self.reject(io("listener lease has no actual accept view")));
        }
        Ok(LeasedInbound { lease: self })
    }
    pub(crate) fn reject(&self, source: EngineError) -> EngineError {
        self.owner.task_access().request_stop();
        self.owner.retain_failed_driver_installation(source)
    }
}
impl Drop for InboundBindLease {
    fn drop(&mut self) {
        if !self.promoted {
            // This is only a stop request. Registry still owns the real native
            // holder and must obtain its explicit close and actual Scope joins.
            self.owner.task_access().request_stop();
        }
    }
}
pub(crate) struct LeasedInbound {
    lease: InboundBindLease,
}
impl LeasedInbound {
    #[cfg(test)]
    pub(super) fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.lease
            .listener
            .as_ref()
            .expect("actual bound view")
            .local_addr()
    }
    pub(crate) fn shutdown_sender(&self) -> watch::Sender<bool> {
        self.lease.shutdown_sender()
    }
    #[cfg(test)]
    pub(crate) fn promote(
        self,
    ) -> Result<
        (
            Box<dyn PreparedInboundListenerOperation>,
            ControlledTcpListener,
            InboundInstallContext,
        ),
        EngineError,
    > {
        if !matches!(self.lease.listener, Some(NativeListenerView::Tcp(_))) {
            return Err(self.lease.reject(io("TCP promotion requires a TCP view")));
        }
        let (operation, view, installed) = self.promote_native()?;
        let listener = match view {
            NativeListenerView::Tcp(listener) => listener,
            #[cfg(feature = "datagram-route-runtime")]
            NativeListenerView::Datagram(_) => unreachable!("checked TCP view"),
        };
        Ok((operation, listener, installed))
    }
    pub(crate) fn promote_native(
        mut self,
    ) -> Result<
        (
            Box<dyn PreparedInboundListenerOperation>,
            NativeListenerView,
            InboundInstallContext,
        ),
        EngineError,
    > {
        let Some(registry) = self.lease.registry.upgrade() else {
            return Err(self
                .lease
                .reject(io("listener registry is no longer available")));
        };
        let mut active = registry
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut reserved = registry
            .reservations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if reserved.closing || self.lease.owner.task_access().cancellation().is_cancelled() {
            return Err(self.lease.reject(self.lease.owner.ready_stopped()));
        }
        if active.contains_key(&self.lease.tag) {
            return Err(self.lease.reject(io(
                "old active listener still requires confirmed retirement",
            )));
        }
        if !reserved
            .entries
            .get(&self.lease.tag)
            .is_some_and(|owner| Arc::ptr_eq(owner, &self.lease.owner))
        {
            return Err(self
                .lease
                .reject(io("listener reservation is stopped or no longer current")));
        }
        self.lease.owner.begin_listener_installation();
        reserved.entries.remove(&self.lease.tag);
        if reserved.entries.is_empty() {
            reserved.config = Weak::new();
        }
        active.insert(self.lease.tag.clone(), self.lease.owner.clone());
        self.lease.promoted = true;
        Ok((
            self.lease
                .prepared
                .take()
                .expect("one prepared operation")
                .into_operation(),
            self.lease.listener.take().expect("one actual accept view"),
            InboundInstallContext {
                entry: self.lease.owner.clone(),
                protocols: self.lease.protocols.clone(),
            },
        ))
    }
}
impl InboundOwner {
    /// Only Leased setup uses this: its actual native holder was installed
    /// before bind, even if task registration consumed the submitted view.
    pub(crate) fn retain_managed_listener_error(&self, source: EngineError) -> EngineError {
        self.task_access().request_stop();
        let error = self.retain_failed_driver_installation(source);
        self.seal_listener_installation();
        error
    }
}
