//! Explicit association declarations share this actual listener owner and Scope.
use super::*;

#[cfg(feature = "managed-datagram-runtime")]
impl InboundOwner {
    pub(in crate::runtime) fn datagram_continuity(
        &self,
    ) -> Option<Arc<crate::runtime::route_runtime::DatagramContinuityHost>> {
        self.continuity.lock().unwrap().clone()
    }
    pub(in crate::runtime::inbound_owners) fn install_datagram_continuity(
        &self,
        protocols: ProtocolInventory,
        limit: NonZeroUsize,
        max_tasks: NonZeroUsize,
        max_resources: usize,
    ) -> Result<(), EngineError> {
        let mut slot = self.continuity.lock().unwrap();
        if slot.is_none() {
            let peers = self.install_child_group(protocols, limit)?;
            *slot = Some(Arc::new(
                crate::runtime::route_runtime::DatagramContinuityHost::new(
                    self.task_access(),
                    peers,
                    limit,
                    max_tasks,
                    max_resources,
                ),
            ));
        }
        Ok(())
    }
}
