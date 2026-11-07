//! Concrete kernel reader and the existing independent embedding source.
use super::super::*;
use super::budget::SourceBudget;

pub(in crate::dispatcher) struct Source<S> {
    standalone: S,
    #[cfg(feature = "kernel-events")]
    kernel: Option<zero_engine::EngineHandle>,
}
pub(in crate::dispatcher) enum Subscriber<S> {
    Standalone(S),
    #[cfg(feature = "kernel-events")]
    Kernel(zero_engine::EventSubscriber),
}
pub(in crate::dispatcher) enum Record {
    Standalone(RawApiEvent),
    #[cfg(feature = "kernel-events")]
    Kernel(zero_engine::RetainedEvent),
}
impl std::ops::Deref for Record {
    type Target = RawApiEvent;
    fn deref(&self) -> &RawApiEvent {
        match self {
            Self::Standalone(event) => event,
            #[cfg(feature = "kernel-events")]
            Self::Kernel(event) => event,
        }
    }
}
impl Record {
    pub(super) fn into_shared(self) -> Arc<RawApiEvent> {
        match self {
            Self::Standalone(event) => Arc::new(event),
            #[cfg(feature = "kernel-events")]
            Self::Kernel(event) => event.into_shared(),
        }
    }
}
impl<S: EventStream> Subscriber<S> {
    pub(super) fn receive(
        &self,
        timeout: Duration,
    ) -> Result<Record, std::sync::mpsc::RecvTimeoutError> {
        match self {
            Self::Standalone(stream) => match stream.recv_timeout(timeout) {
                EventStreamReceive::Event(event) => Ok(Record::Standalone(*event)),
                EventStreamReceive::Timeout => Err(std::sync::mpsc::RecvTimeoutError::Timeout),
                EventStreamReceive::Closed => Err(std::sync::mpsc::RecvTimeoutError::Disconnected),
            },
            #[cfg(feature = "kernel-events")]
            Self::Kernel(stream) => stream.recv_retained_timeout(timeout).map(Record::Kernel),
        }
    }
    pub(super) fn try_receive(&self) -> Option<Record> {
        match self {
            Self::Standalone(stream) => stream.try_recv().map(Record::Standalone),
            #[cfg(feature = "kernel-events")]
            Self::Kernel(stream) => stream.try_recv_retained().map(Record::Kernel),
        }
    }
}
impl<S: EventSource> Source<S> {
    pub(in crate::dispatcher) fn standalone(source: S) -> Self {
        Self {
            standalone: source,
            #[cfg(feature = "kernel-events")]
            kernel: None,
        }
    }
    pub(in crate::dispatcher) fn core_instance_id(&self) -> zero_api::ApiResult<String> {
        #[cfg(feature = "kernel-events")]
        if let Some(kernel) = &self.kernel {
            return Ok(kernel
                .inner()
                .original_event_history()
                .core_instance_id()
                .to_owned());
        }
        self.standalone
            .since(0, 0, EventFilter::default())
            .map(|replay| replay.core_instance_id)
    }
    pub(in crate::dispatcher) fn startup_sequence(&self) -> zero_api::ApiResult<u64> {
        #[cfg(feature = "kernel-events")]
        if let Some(kernel) = &self.kernel {
            return Ok(kernel.inner().latest_event_sequence());
        }
        self.standalone
            .latest(1, EventFilter::default())
            .map(|events| events.first().and_then(|event| event.sequence).unwrap_or(0))
    }
    pub(in crate::dispatcher) fn subscribe(
        &self,
    ) -> zero_api::ApiResult<(Subscriber<S::Stream>, Option<u64>)> {
        #[cfg(feature = "kernel-events")]
        if let Some(kernel) = &self.kernel {
            let (live, _) = kernel.subscribe_control_live(EventFilter::default())?;
            // Persistent sinks never consume flow.snapshot's payload. Preserve
            // its post-registration cursor directly, without constructing it.
            let baseline = kernel.inner().latest_event_sequence();
            return Ok((Subscriber::Kernel(live), Some(baseline)));
        }
        self.standalone
            .subscribe(EventFilter::default())
            .map(|stream| (Subscriber::Standalone(stream), None))
    }
    pub(in crate::dispatcher) fn bootstrap(
        &self,
        budget: &Arc<SourceBudget>,
    ) -> zero_api::ApiResult<Option<BlockedEvent>> {
        #[cfg(not(feature = "kernel-events"))]
        let _ = budget;
        #[cfg(feature = "kernel-events")]
        if let Some(kernel) = &self.kernel {
            let history = kernel.inner().original_event_history();
            return Ok(history
                .latest_engine_started()
                .and_then(|event| Self::retain(event, budget)));
        }
        let events = self.standalone.latest(
            1,
            EventFilter {
                event_types: vec![event_type::ENGINE_STARTED.to_owned()],
                ..EventFilter::default()
            },
        )?;
        Ok(events.into_iter().next().map(BlockedEvent::new))
    }
    #[cfg(feature = "kernel-events")]
    fn retain(event: &Arc<RawApiEvent>, budget: &Arc<SourceBudget>) -> Option<BlockedEvent> {
        // Never wait with the history lock held. A full source book leaves the
        // cursor unchanged; the existing dispatcher retries after credit returns.
        let credit = budget.try_reserve_event(&**event)?;
        let mut blocked = BlockedEvent::from_shared(event.clone());
        blocked.source_reservation = Some(credit);
        Some(blocked)
    }
    pub(in crate::dispatcher) fn next(
        &self,
        since: u64,
        budget: &Arc<SourceBudget>,
    ) -> zero_api::ApiResult<(u64, bool, Option<BlockedEvent>)> {
        #[cfg(not(feature = "kernel-events"))]
        let _ = budget;
        #[cfg(feature = "kernel-events")]
        if let Some(kernel) = &self.kernel {
            // A queued future live event must not repeatedly steal the only
            // byte credit from its missing replay prefix. Pause that same
            // bridge's admission; already held originals remain charged.
            budget.pause_replay();
            let history = kernel.inner().original_event_history();
            let (actual, gap, event) = history.next(since);
            let retained = event.and_then(|event| Self::retain(event, budget));
            if event.is_none() || retained.is_some() {
                budget.resume_live();
            }
            return Ok((actual, gap, retained));
        }
        self.standalone
            .since(since, 1, EventFilter::default())
            .map(|mut replay| {
                (
                    replay.actual_from,
                    replay.has_gap,
                    replay.events.pop().map(BlockedEvent::new),
                )
            })
    }
}
#[cfg(feature = "kernel-events")]
impl Source<zero_engine::EngineHandle> {
    pub(in crate::dispatcher) fn kernel(source: zero_engine::EngineHandle) -> Self {
        Self {
            standalone: source.clone(),
            kernel: Some(source),
        }
    }
}

#[cfg(all(test, feature = "kernel-events"))]
mod tests;
