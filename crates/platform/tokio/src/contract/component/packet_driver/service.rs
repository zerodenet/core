use super::*;
use zero_core::component::ComponentInstanceId;
/// Exact immutable target facts projected by the host's already prepared graph.
/// A declaration is cleanup metadata, never endpoint, native or task authority.
pub struct PacketDriverTarget {
    pub instance: ComponentInstanceId,
    pub role: zero_core::component::RoleInstanceId,
    pub datagram: bool,
    pub binding: UpstreamBinding,
    pub originals: Arc<[crate::controlled_carrier::CarrierOwnerRegistration]>,
    pub packet_originals: Arc<[CarrierOrigin]>,
    pub metadata: crate::contract::component::PacketMetadataVec<
        crate::controlled_datagram::metadata_pool::MetadataPoolRegistration,
    >,
    pub pending_capacity: usize,
}
/// Actual driver authority issued by ResourceAccess. It retains neither an
/// Engine nor a historical snapshot; selecting a target remains host checked.
#[derive(Clone)]
pub struct GrantedPacketServiceCaller {
    calls: Arc<PacketDriverCalls>,
    caller: TaskJoinWitness,
    lineage: MetadataRef<lineage::Lineage>,
    root_binding: UpstreamBinding,
    call: CarrierContext,
    targets: Arc<[PacketDriverTarget]>,
    attempt: Option<Arc<Attempt>>,
}
struct Attempt {
    records: Mutex<Vec<RecordWeak>>,
}
impl GrantedPacketServiceCaller {
    pub fn matches_cleanup_failure(&self, source: &ChannelError) -> bool {
        self.calls.source_failures.owns(source)
    }

    #[cfg(feature = "native-packet-origin")]
    pub(super) fn packet_calls(&self) -> &Arc<PacketDriverCalls> {
        &self.calls
    }

    pub fn matches_calls(&self, calls: &Arc<PacketDriverCalls>) -> bool {
        Arc::ptr_eq(&self.calls, calls)
    }

    pub fn from_registered(
        calls: Arc<PacketDriverCalls>,
        original: &CarrierOrigin,
        caller: &TaskJoinWitness,
        binding: UpstreamBinding,
        call: CarrierContext,
        targets: Arc<[PacketDriverTarget]>,
    ) -> Result<Self, ChannelError> {
        let wire = calls
            .original
            .driver()
            .ok_or_else(|| failure(Reason::Revoked))?;
        if !calls.original.same_original(original)
            || !calls.accepts_actor(caller)
            || !caller.scope_identity().same_scope(&wire.scope_identity())
            || caller.task_id().is_none()
            || caller.is_joined()
            || !caller.scope_identity().resource_open()
            || original.is_retired()
            || targets.is_empty()
            || call.session.is_some()
            || call.cancel.is_cancelled()
            || call.deadline.is_some_and(|end| Instant::now() >= end)
            || targets
                .iter()
                .any(|target| target.binding.network_generation != binding.network_generation)
        {
            return Err(failure(Reason::StalePlan));
        }
        let lineage = calls.new_lineage(caller)?;
        Ok(Self {
            lineage,
            calls,
            caller: caller.clone(),
            root_binding: binding,
            call,
            targets,
            attempt: None,
        })
    }
    /// Narrow one original attempt's deadline without changing its actor,
    /// Source provenance, immutable targets or parent cancellation ownership.
    pub fn for_attempt(&self, end: Instant) -> Result<Self, ChannelError> {
        if self.call.cancel.is_cancelled()
            || self
                .lineage
                .retiring
                .load(std::sync::atomic::Ordering::Acquire)
            || self.caller.is_joined()
            || Instant::now() >= end
        {
            return Err(failure(Reason::Revoked));
        }
        self.calls.collect_retired();
        // ANY retained row is still this call's old use, not permission to
        // overlap a candidate with it. Provider cleanup runs outside the lock;
        // only exact lineage metadata is inspected in this admission census.
        let calls = self
            .calls
            .calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let blocked = calls
            .iter()
            .any(|record| MetadataRef::ptr_eq(&record.lineage, &self.lineage));
        if blocked {
            let mut pending = failure(Reason::Capacity);
            pending.delivery = DeliveryState::Unknown;
            return Err(pending);
        }
        // The concrete carrier retains its existing single actual setup or
        // reconnect actor. This census additionally rejects an outstanding
        // original from that invocation's complete previous history.
        drop(calls);
        let mut attempt = self.clone();
        attempt.attempt = Some(Arc::new(Attempt {
            records: Mutex::new(Vec::new()),
        }));
        attempt.call.deadline = Some(self.call.deadline.map_or(end, |current| current.min(end)));
        Ok(attempt)
    }
    /// Settle only the concrete originals created by this same failed build.
    /// This cannot wait for or synthesize the still-running parent actor exit.
    pub async fn retire_attempt(&self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        let attempt = self
            .attempt
            .as_ref()
            .ok_or_else(|| failure(Reason::InvalidConfig))?;
        let mut report = CloseReport::default();
        let mut first = None;
        for index in 0..self.targets.len() {
            let record = attempt
                .records
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(index)
                .and_then(RecordWeak::upgrade);
            let Some(record) = record else { continue };
            match record.close(deadline).await {
                Ok(closed) => report.append(closed),
                Err(source) => {
                    first.get_or_insert(source);
                }
            }
        }
        self.calls.collect_retired();
        match first {
            Some(source) => Err(source),
            None => Ok(report),
        }
    }
    /// Move the existing N=1 reconnect body into this original Scope; no extra
    /// worker and no authority reconstructed from a witness or task number.
    pub fn spawn_reconnect<F, Fut>(
        &self,
        tasks: &WeakTaskRegistrar,
        make: F,
    ) -> Result<(zero_core::component::TaskId, TaskJoinWitness), ChannelError>
    where
        F: FnOnce(Self) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<(), crate::contract::lifecycle::ComponentError>>
            + Send
            + 'static,
    {
        if self
            .lineage
            .retiring
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(failure(Reason::Revoked));
        }
        let template = self.clone();
        self.calls
            .spawn_reconnect(tasks, &self.caller, move |witness| {
                let mut actor = template;
                actor.caller = witness;
                make(actor)
            })
    }
    /// Fence this actual carrier call, including all of its reconnects. Other
    /// peers may share the wire task; their calls retain independent admission.
    pub fn request_carrier_retirement(&self) {
        self.calls.request_lineage_retirement(&self.lineage);
    }
    /// Retain real joined reconnect faults before a cleanup waiter requires
    /// their Host ACK. This grants no task-completion or refund authority.
    pub fn receive_reconnect_failures(&self) -> Option<ChannelError> {
        self.calls.receive_actor_failures()
    }
    /// Pure fault delivery only, not evidence of native/user/task settlement.
    pub fn cleanup_failures_received(&self) -> bool {
        self.calls.source_failures.accepted_or_empty()
    }
    /// Settle only this actual carrier call's complete attempt history.
    /// Shared donor originals retain their own Host obligations.
    pub async fn retire_carrier_attempts(
        &self,
        deadline: Instant,
    ) -> Result<CloseReport, ChannelError> {
        self.calls
            .retire_lineage_attempts(&self.lineage, deadline)
            .await
    }

    pub fn original(&self) -> &CarrierOrigin {
        &self.calls.original
    }
    pub fn caller(&self) -> &TaskJoinWitness {
        &self.caller
    }
    /// The original Source's cancellation domain, not a replacement token.
    pub fn cancellation(&self) -> &crate::contract::CancellationToken {
        &self.call.cancel
    }

    pub fn binding(&self) -> &UpstreamBinding {
        &self.root_binding
    }
    /// Same actual selected target, immutable dial/prefix and real generation.
    /// This only attaches a cleanup observer; the role still owns IO admission.
    pub fn for_target(
        &self,
        target: ComponentInstanceId,
        binding: &UpstreamBinding,
    ) -> Result<PacketDriverCallGrant, ChannelError> {
        let attempt = self
            .attempt
            .as_ref()
            .ok_or_else(|| failure(Reason::InvalidConfig))?;
        let selected = self
            .targets
            .iter()
            .find(|actual| actual.instance == target && actual.datagram)
            .ok_or_else(|| failure(Reason::StalePlan))?;
        if selected.binding.dial != binding.dial
            || selected.binding.network_generation != binding.network_generation
            || self.root_binding.prefix != binding.prefix
        {
            return Err(failure(Reason::StalePlan));
        }
        let mut originals = selected.originals.to_vec();
        let mut packet_originals = selected.packet_originals.to_vec();
        let mut seen = Vec::with_capacity(binding.prefix.len());
        for role in &binding.prefix {
            if seen.contains(role) || *role == selected.role {
                return Err(failure(Reason::StalePlan));
            }
            seen.push(*role);
            let prefix = self
                .targets
                .iter()
                .find(|actual| actual.role == *role)
                .ok_or_else(|| failure(Reason::StalePlan))?;
            for original in prefix.packet_originals.iter() {
                if !packet_originals
                    .iter()
                    .any(|actual| actual.same_original(original))
                {
                    packet_originals.push(original.clone());
                }
            }
            for original in prefix.originals.iter() {
                if !originals.iter().any(|actual| actual.same_owner(original)) {
                    originals.push(original.clone());
                }
            }
        }
        let capacity = originals
            .iter()
            .try_fold(0usize, |used, original| {
                used.checked_add(original.limit().get())
            })
            .ok_or_else(|| failure(Reason::Capacity))?;
        let grant = self.calls.register_with_protocol_targets(
            &self.caller,
            binding.clone(),
            self.call.clone(),
            super::registration::OriginalTargets {
                lineage: Some(self.lineage.clone()),
                max_pending: capacity,
                allowed: originals.into(),
                packet_originals: packet_originals.into(),
                protocols: Some((self.targets.clone(), selected.role)),
            },
        )?;
        attempt
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(RecordWeak::new(&grant.record));
        Ok(grant)
    }
}
