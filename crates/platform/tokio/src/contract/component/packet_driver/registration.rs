//! Same actual Source registration and finite per-callee cleanup records.
use super::*;
impl PacketDriverCalls {
    pub fn matches_original(&self, original: &CarrierOrigin) -> bool {
        self.original.same_original(original)
    }

    pub fn from_registered(
        original: CarrierOrigin,
        tasks: &WeakTaskRegistrar,
    ) -> Result<Self, ChannelError> {
        let max_calls = tasks
            .declared_task_limit()
            .ok_or_else(|| failure(Reason::InvalidConfig))?;
        let driver = original
            .driver()
            .ok_or_else(|| failure(Reason::InvalidConfig))?;
        if original.is_retired() || !driver.scope_identity().same_scope(&tasks.scope_identity()) {
            return Err(failure(Reason::StalePlan));
        }
        let source_failures = Arc::new(source_failures::SourceDelivery::new());
        let native_failure = source_failures.prepare();
        Ok(Self {
            original,
            max_calls,
            retiring: false.into(),
            failures: Mutex::new(faults::Delivery::new()),
            source_failures,
            native_failure,
            calls: Mutex::new(Default::default()),
            reconnects: Mutex::new(Default::default()),
            #[cfg(feature = "native-packet-origin")]
            copy_workspaces: OnceLock::new(),
        })
    }
    /// Host validates the selected actual callee and its complete binding before
    /// entering here. This only reserves bounded cleanup metadata before IO.
    pub fn register(
        &self,
        caller: &TaskJoinWitness,
        binding: UpstreamBinding,
        call: CarrierContext,
        max_pending: usize,
        allowed: Arc<[crate::controlled_carrier::CarrierOwnerRegistration]>,
        packet_originals: Arc<[CarrierOrigin]>,
    ) -> Result<PacketDriverCallGrant, ChannelError> {
        let source = self
            .original
            .driver()
            .ok_or_else(|| failure(Reason::Revoked))?;
        if self.retiring.load(std::sync::atomic::Ordering::Acquire)
            || self.original.is_retired()
            || !self.accepts_actor(caller)
            || caller.task_id().is_none()
            || caller.is_joined()
            || !caller.scope_identity().same_scope(&source.scope_identity())
            || !caller.scope_identity().resource_open()
            || call.session.is_some()
            || call.cancel.is_cancelled()
            || call.deadline.is_some_and(|end| Instant::now() >= end)
        {
            return Err(failure(Reason::Revoked));
        }
        #[cfg(feature = "native-packet-origin")]
        let copies = self
            .copy_workspaces
            .get()
            .cloned()
            .ok_or_else(|| failure(Reason::InvalidConfig))?;
        #[cfg(feature = "native-packet-origin")]
        let observer_memory = copies
            .first()
            .ok_or_else(|| failure(Reason::InvalidConfig))?
            .grant
            .memory_view()
            .map_err(memory::memory_error)?;
        #[cfg(feature = "native-packet-origin")]
        let observer_credit = observer_memory
            .reserve_driver_metadata(
                crate::contract::network::UpstreamTransport::packet_driver_opener_layout::<
                    pending::Observer,
                >(&binding)
                .map_err(memory::memory_error)?,
            )
            .map_err(memory::memory_error)?;
        // Declare before ALL owning table locks. Credits/slots return at the
        // actual deallocation; only external Root wakes wait for these locks.
        #[cfg(feature = "native-packet-origin")]
        let _metadata_notifications = observer_memory.defer_notifications();
        #[cfg(feature = "native-packet-origin")]
        let record_credit = observer_memory
            .reserve_driver_metadata(RecordRef::layout().map_err(memory::memory_error)?)
            .map_err(memory::memory_error)?;
        #[cfg(feature = "native-packet-origin")]
        let facts_credit = observer_memory
            .reserve_driver_metadata(
                crate::contract::context::ServiceFactsOwner::paid_layout()
                    .map_err(memory::memory_error)?,
            )
            .map_err(memory::memory_error)?;
        #[cfg(feature = "native-packet-origin")]
        let identity = crate::contract::context::ServiceFactsOwner::from_prepaid(
            ServiceRequestFacts {
                origin: ServiceOrigin::Runtime,
                purpose: ServicePurpose::BackgroundControl,
            },
            facts_credit,
        )
        .map_err(|(source, _, _)| memory::memory_error(source))?;
        #[cfg(not(feature = "native-packet-origin"))]
        let identity = Arc::new(ServiceRequestFacts {
            origin: ServiceOrigin::Runtime,
            purpose: ServicePurpose::BackgroundControl,
        })
        .into();
        #[cfg(feature = "native-packet-origin")]
        let installed_metadata = observer_memory
            .reserve_driver_metadata(
                InstalledRef::layout()
                    .and_then(|layout| {
                        layout.checked_add(
                            zero_traits::packet_metadata::PacketMetadataLayout::boxed::<
                                control::StoredControl,
                            >(),
                        )
                    })
                    .map_err(memory::memory_error)?,
            )
            .map_err(memory::memory_error)?;
        #[cfg(feature = "native-packet-origin")]
        let settled_metadata = observer_memory
            .reserve_driver_metadata(SettledRef::layout().map_err(memory::memory_error)?)
            .map_err(memory::memory_error)?;
        #[cfg(feature = "native-packet-origin")]
        let settled =
            SettledRef::from_prepaid(std::sync::atomic::AtomicBool::new(false), settled_metadata)
                .map_err(|(source, _, _)| memory::memory_error(source))?;
        #[cfg(not(feature = "native-packet-origin"))]
        let settled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        if let Some(source) = self.receive_actor_failures() {
            return Err(source);
        }
        self.collect_retired();
        let mut retired_epoch = None;
        let mut calls = self
            .calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let delivery = self.advance_fault_epoch(&calls, &mut retired_epoch)?;
        // Collection is driven separately by the actual owner, outside this lock.
        // A join without registration ACK cannot free a row.
        if delivery.failed() || self.retiring.load(std::sync::atomic::Ordering::Acquire) {
            return Err(failure(Reason::Revoked));
        }
        if calls.len() >= self.max_calls.get() {
            return Err(failure(Reason::Capacity));
        }
        #[cfg(feature = "native-packet-origin")]
        calls
            .try_reserve_one(&observer_memory, self.max_calls.get())
            .map_err(memory::memory_error)?;
        let failures = Arc::new(faults::RecordFailures::default());
        delivery.register(failures.clone());
        let value = Record {
            original: self.original.clone(),
            caller: caller.clone(),
            binding,
            identity,
            call: CarrierContext {
                session: None,
                deadline: call.deadline,
                cancel: call.cancel.child_token(),
            },
            max_pending,
            #[cfg(feature = "native-packet-origin")]
            copies,
            allowed,
            packet_originals,
            streams: Mutex::new(Vec::new()),
            observer: OnceLock::new(),
            upstream: OnceLock::new(),
            pending: Mutex::new(Vec::new()),
            installed: OnceLock::new(),
            #[cfg(feature = "native-packet-origin")]
            installed_metadata: Mutex::new(Some(installed_metadata)),
            failures,
            delivery,
            source_failures: self.source_failures.clone(),
            forwarded: self.source_failures.prepare(),
            settled,
        };
        #[cfg(feature = "native-packet-origin")]
        let record = match RecordRef::from_prepaid(value, record_credit) {
            Ok(record) => record,
            Err((source, value, credit)) => {
                value.delivery.remove_empty(&value.failures);
                self.source_failures.remove_empty(&value.forwarded);
                drop(value);
                drop(credit);
                return Err(memory::memory_error(source));
            }
        };
        #[cfg(not(feature = "native-packet-origin"))]
        let record = Arc::new(value);
        let observer = pending::Observer(RecordRef::downgrade(&record));
        #[cfg(feature = "native-packet-origin")]
        let observer =
            match crate::contract::network::UpstreamTransport::from_prepaid_packet_driver_opener(
                record.binding.clone(),
                observer,
                observer_credit,
            ) {
                Ok(observer) => observer,
                Err((source, _, _, _)) => {
                    record.delivery.remove_empty(&record.failures);
                    self.source_failures.remove_empty(&record.forwarded);
                    return Err(memory::memory_error(source));
                }
            };
        #[cfg(not(feature = "native-packet-origin"))]
        let observer = crate::contract::network::UpstreamTransport::new(
            record.binding.clone(),
            Arc::new(observer),
        );
        record
            .observer
            .set(observer)
            .unwrap_or_else(|_| unreachable!("fresh caller observer"));
        #[cfg(feature = "native-packet-origin")]
        calls
            .push_reserved(record.clone())
            .unwrap_or_else(|_| unreachable!("same lock owns the prepaid table slot"));
        #[cfg(not(feature = "native-packet-origin"))]
        calls.push(record.clone());
        Ok(PacketDriverCallGrant {
            record,
            installed: false,
        })
    }
}
