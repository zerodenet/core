//! Cold listener selection. Installation alone claims the original executor.
use super::device::ListenerLease;
use super::*;
use crate::runtime::inbound_owners::InboundInstallContext;
use std::{future::Future, pin::Pin};

#[derive(Clone)]
pub(in crate::adapters::wireguard) enum Binding {
    Standalone,
    Ready(Arc<LinkedEndpoint>),
    Pending(Arc<super::super::lifecycle::PendingLinkedEndpoint>),
}
impl Binding {
    pub(super) fn ready(&self) -> Result<Option<Arc<LinkedEndpoint>>, EngineError> {
        match self {
            Self::Standalone => Ok(None),
            Self::Ready(linked) => Ok(Some(linked.clone())),
            Self::Pending(pending) => pending
                .ready()
                .map(Some)
                .ok_or_else(|| invalid("WireGuard linked dependency has not completed BuildNew")),
        }
    }
}

pub(in crate::adapters::wireguard) struct ColdListener {
    live: Arc<LiveInboundDevice>,
    binding: Binding,
    update: Option<(InboundDevice, [u8; 32])>,
    operation: Option<Box<dyn PreparedInboundListenerOperation>>,
    lease: Option<ListenerLease>,
    recovery: Arc<super::recovery::Recovery>,
}
impl ColdListener {
    pub(in crate::adapters::wireguard) fn new(
        inbound: &InboundConfig,
        live: Arc<LiveInboundDevice>,
        binding: Binding,
        pending: super::super::PendingEndpointMap,
    ) -> Result<Self, EngineError> {
        let identity = inbound_protocol_identity(&inbound.protocol)
            .ok_or_else(|| invalid("cannot identify WireGuard inbound"))?;
        let update = if live.matches_identity(identity) {
            None
        } else {
            Some((prepare_device(inbound)?, identity))
        };
        let recovery = Arc::new(super::recovery::Recovery::new(
            inbound,
            live.clone(),
            binding.clone(),
            pending,
        )?);
        Ok(Self {
            live,
            binding,
            update,
            operation: None,
            lease: None,
            recovery,
        })
    }
}
impl PreparedInboundListenerOperation for ColdListener {
    fn internal_tasks(&self) -> usize {
        RawIpInboundListenerOperation::internal_task_limit()
    }

    fn recovery_state(&self) -> Option<Arc<dyn crate::protocol_registry::InboundRecovery>> {
        Some(self.recovery.clone())
    }
    fn install_resources(&mut self, context: &InboundInstallContext) -> Result<(), EngineError> {
        if self.operation.is_some() || self.lease.is_some() {
            return Err(invalid("WireGuard cold listener was already installed"));
        }
        let linked = self.binding.ready()?;
        if let Some(linked) = &linked {
            if !Arc::ptr_eq(&linked.live, &self.live) {
                return Err(invalid("WireGuard linked dependency changed crypto owner"));
            }
            if linked
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .devices
                .iter()
                .any(|device| !device.is_usable())
            {
                return Err(invalid("WireGuard linked dependency was retired"));
            }
        }
        self.lease = Some(self.live.claim_listener()?);
        let operation = match linked {
            Some(linked) => linked.take_operation()?,
            None => prepare_unlinked_with_live(self.live.clone()),
        };
        self.operation = Some(operation);
        self.operation
            .as_mut()
            .unwrap()
            .install_resources(context)?;
        if let Some((device, identity)) = self.update.take() {
            if !self.live.matches_identity(identity) {
                self.live.replace(device, identity);
            }
        }
        Ok(())
    }
    fn execute(
        mut self: Box<Self>,
        runtime: crate::runtime::route_runtime::InboundListenerRuntime,
        bound: crate::protocol_registry::BoundInbound,
        shutdown: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), EngineError>> + Send>> {
        Box::pin(async move {
            // Metadata only. Scope joins and native owners provide closure proof.
            let _lease = self.lease.take();
            let operation = self
                .operation
                .take()
                .ok_or_else(|| invalid("WireGuard cold listener was not installed"))?;
            operation.execute(runtime, bound, shutdown).await
        })
    }
}
