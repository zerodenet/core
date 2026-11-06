use super::*;
/// Unique non-clone checkpoint delivered to the matching original task body.
/// The root's request/observation handle cannot acknowledge either task.
pub struct PacketSourceCheckpoint {
    pub(super) original: Arc<PacketSourcePause>,
    pub(super) participant: PacketPauseParticipant,
    pub(super) witness: TaskJoinWitness,
}
impl PacketSourceCheckpoint {
    pub(crate) fn bind_origin(
        &self,
        instance: ComponentInstanceId,
        scope: &TaskScopeIdentity,
        driver: &TaskJoinWitness,
    ) -> Option<std::sync::Weak<PacketSourcePause>> {
        if matches!(self.participant, PacketPauseParticipant::Wire)
            && self.original.instance() == instance
            && self.original.scope().same_scope(scope)
            && self.witness.same_registration(driver)
        {
            Some(Arc::downgrade(&self.original))
        } else {
            None
        }
    }
    pub fn witness(&self) -> &TaskJoinWitness {
        &self.witness
    }
    pub async fn requested(&self) -> u64 {
        self.original.requested().await
    }
    pub fn request_deadline(&self, request: u64) -> Option<tokio::time::Instant> {
        let state = self.original.state.lock().unwrap();
        (state.request == Some(request))
            .then_some(state.deadline)
            .flatten()
    }
    pub fn acknowledge(&self, request: u64, idle: bool) {
        let mut state = self.original.state.lock().unwrap();
        if state.request == Some(request)
            && state.actual[self.participant.index()]
                .as_ref()
                .is_some_and(|actual| actual.same_registration(&self.witness))
        {
            state.acknowledged[self.participant.index()] = Some(idle);
        }
        drop(state);
        self.original.changed.notify_waiters();
    }
    /// Preserve a real local fault (for example failed SDK pause signaling).
    pub fn reject(&self, request: u64, error: ChannelError) {
        let mut state = self.original.state.lock().unwrap();
        if state.request == Some(request) {
            state.acknowledged[self.participant.index()] = Some(false);
            state.fault.get_or_insert_with(|| Arc::new(error));
        }
        drop(state);
        self.original.changed.notify_waiters();
    }
    pub async fn resumed(&self, request: u64) {
        self.original.resumed(request).await;
    }
}
impl Drop for PacketSourceCheckpoint {
    fn drop(&mut self) {
        self.original.state.lock().unwrap().present[self.participant.index()] = false;
        self.original.changed.notify_waiters();
    }
}
