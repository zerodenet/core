//! Same accepted caller/Source book, exact child originals before first IO.
use super::*;
use crate::controlled_carrier::{ProtocolOriginal, ProtocolOriginalChildren};
impl AcceptedCaller {
    pub fn protocol_metadata(
        &self,
    ) -> Result<(crate::contract::component::PacketDriverMetadataView, usize), ChannelError> {
        self.check_open()?;
        self.users.upgrade().ok_or_else(gone)?.protocol_memory()
    }
    pub fn observe_protocol_original(
        &self,
        original: ProtocolOriginal,
    ) -> Result<(), ChannelError> {
        self.check_open()?;
        let caller = self.task_join_witness().ok_or_else(gone)?;
        if !caller
            .native_task_id()
            .is_some_and(|id| Some(id) == tokio::task::try_id())
        {
            return Err(gone());
        }
        let users = self.users.upgrade().ok_or_else(gone)?;
        users
            .protocol_children()?
            .retain_for_caller(original, caller)?;
        users.logical_dirty.store(true, Ordering::Release);
        users.changed.notify_waiters();
        Ok(())
    }
}
impl Users {
    fn protocol_memory(
        &self,
    ) -> Result<(crate::contract::component::PacketDriverMetadataView, usize), ChannelError> {
        let pool = self.pool.upgrade();
        let (instance, root) =
            if let Some(binding) = pool.as_ref().and_then(|pool| pool.originals.get()) {
                if !self
                    .authority
                    .matches_original_scope(&binding.scope.scope_identity())
                {
                    return Err(gone());
                }
                (binding.instance, binding.root.upgrade())
            } else if let Some((instance, root)) = self.lookup_totals.get() {
                (*instance, root.upgrade())
            } else {
                return Err(gone());
            };
        let root = root.ok_or_else(gone)?;
        let receiver = self.task_failures.get().ok_or_else(gone)?;
        if receiver.instance() != instance
            || !receiver.scope_identity().same_scope(&self.authority.scope)
        {
            return Err(gone());
        }
        Ok((
            receiver.metadata_view().clone(),
            root.snapshot().limit.get(),
        ))
    }
    fn protocol_children(&self) -> Result<&ProtocolOriginalChildren, ChannelError> {
        let (memory, limit) = self.protocol_memory()?;
        let children = self
            .protocols
            .get_or_init(|| ProtocolOriginalChildren::new(memory.clone(), limit));
        if !children.same_memory(&memory) {
            return Err(gone());
        }
        Ok(children)
    }
    pub(super) fn has_protocol_caller(&self, caller: &TaskJoinWitness) -> bool {
        self.protocols
            .get()
            .is_some_and(|children| children.has_caller(caller))
    }
    pub(super) async fn close_protocols(&self, end: Instant, joined_only: bool) -> bool {
        let Some(children) = self.protocols.get() else {
            return true;
        };
        let mut after = None;
        let mut complete = true;
        loop {
            // Copy one existing witness only. No Root/child observation runs
            // under the ingress graph gate, and no full-table Vec is created.
            let next = {
                let state = self.state();
                state
                    .tasks
                    .iter()
                    .find(|(id, _)| after.is_none_or(|after| **id > after))
                    .map(|(id, node)| (*id, node.witness.clone()))
            };
            let Some((id, caller)) = next else { break };
            after = Some(id);
            if joined_only && !caller.is_joined() {
                continue;
            }
            match children.wait_caller_returned(&caller, end).await {
                Ok(returned) => complete &= returned,
                Err(source) => {
                    self.remember(Arc::new(source));
                    complete = false;
                }
            }
        }
        complete
    }
}
