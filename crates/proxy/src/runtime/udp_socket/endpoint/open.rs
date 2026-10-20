use super::*;

impl DirectEndpoint {
    pub(crate) fn identity(&self) -> DirectEndpointIdentity {
        self.0.identity
    }
    /// Actual business and association IDs are supplied by the accepted flow.
    /// No socket opens until the first bounded command is executed.
    pub(crate) fn open(
        &self,
        session: SessionId,
        association: AssociationId,
        policy: DirectUdpPolicy,
        cancel: CancellationToken,
    ) -> Result<DirectEndpointSession, ChannelError> {
        self.open_isolated(session, association, policy, cancel, None)
    }
    /// Preserve the protocol-supplied opaque client-session isolation key.
    /// It is separate from the host-assigned logical association identity.
    pub(crate) fn open_isolated(
        &self,
        session: SessionId,
        association: AssociationId,
        policy: DirectUdpPolicy,
        cancel: CancellationToken,
        isolation: Option<u64>,
    ) -> Result<DirectEndpointSession, ChannelError> {
        self.open_internal(session, association, policy, cancel, isolation, None)
    }
    /// The host passes the actual current operation's scoped binding. Protocols
    /// cannot choose a dial policy or turn this into an unrestricted opener.
    pub(crate) fn open_bound(
        &self,
        session: SessionId,
        association: AssociationId,
        binding: DirectEndpointBinding,
        cancel: CancellationToken,
        isolation: Option<u64>,
    ) -> Result<DirectEndpointSession, ChannelError> {
        if binding.id.owner() != self.0.identity.resource {
            return Err(error(
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        binding.check_current()?;
        self.open_internal(
            session,
            association,
            binding.policy.clone(),
            cancel,
            isolation,
            Some(binding),
        )
    }
    fn open_internal(
        &self,
        session: SessionId,
        association: AssociationId,
        policy: DirectUdpPolicy,
        cancel: CancellationToken,
        isolation: Option<u64>,
        binding: Option<DirectEndpointBinding>,
    ) -> Result<DirectEndpointSession, ChannelError> {
        if self.0.bound_mode != binding.is_some() {
            return Err(error(
                Stage::Prepare,
                Reason::Unsupported,
                DeliveryState::NotSubmitted,
            ));
        }
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(source) = &state.failure {
            return Err(
                error(Stage::Prepare, Reason::Io, DeliveryState::NotSubmitted)
                    .with_source(source.clone()),
            );
        }
        if !state.started
            || state.closing
            || self.0.scope_cancel.is_cancelled()
            || cancel.is_cancelled()
        {
            return Err(error(
                Stage::Prepare,
                Reason::Revoked,
                DeliveryState::NotSubmitted,
            ));
        }
        if (self.0.fixed_peer.is_some() && state.admitted)
            || state.sessions.len() >= self.0.limits.logical.get()
        {
            return Err(error(
                Stage::Prepare,
                Reason::Capacity,
                DeliveryState::NotSubmitted,
            ));
        }
        if state.sessions.contains_key(&session)
            || state
                .sessions
                .values()
                .any(|entry| entry.logical.association == association)
        {
            return Err(error(
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        if self.0.fixed_peer.is_some() && policy.dial_policy != zero_traits::DialPolicy::default() {
            return Err(error(
                Stage::Prepare,
                Reason::Unsupported,
                DeliveryState::NotSubmitted,
            ));
        }
        let (responses, rx) = mpsc::channel(self.0.queue.capacity);
        let logical = Arc::new(Logical {
            session,
            association,
            policy,
            binding,
            isolation,
            cancel,
            closed: AtomicBool::new(false),
            changed: Notify::new(),
            responses,
            receiver: Mutex::new(rx),
            bytes: AtomicUsize::new(0),
        });
        state.admitted = true;
        state.sessions.insert(
            session,
            Entry {
                logical: logical.clone(),
                watched: false,
            },
        );
        drop(state);
        self.0.changed.notify_one();
        Ok(DirectEndpointSession {
            tx: DirectEndpointTx::new(self.0.clone(), logical.clone()),
            rx: DirectEndpointRx::new(logical.clone()),
            control: DirectEndpointControl::new(self.0.clone(), logical),
        })
    }
}

impl DirectEndpoint {
    /// A coherent snapshot of this actual owner only. Snapshotting does not
    /// clear history, cancel work, reclaim capacity or claim native I/O closed.
    pub(crate) fn queue_usage(&self) -> DirectEndpointQueueUsage {
        self.0.copies.snapshot()
    }
}
