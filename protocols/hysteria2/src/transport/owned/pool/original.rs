//! The same authenticated pool retains each pre-delivery native original.
//! A waiting caller observes custody; it never becomes the pool's IO owner.
use super::*;
use zero_platform_tokio::{
    contract::{lifecycle::DynChannelControl, BoxFuture, CancellationToken},
    controlled_datagram::metadata_pool::MetadataPoolPreparation,
};

struct Control {
    entry: Weak<Entry>,
    origin: Arc<OnceLock<MetadataPoolOrigin>>,
    stop: CancellationToken,
}
impl DynChannelControl for Control {
    fn cancel(&self, _: CloseReason) {
        self.stop.cancel();
    }
    fn wait_closed(&mut self, _end: Instant) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        self.stop.cancel();
        Box::pin(async move {
            let origin = self
                .origin
                .get()
                .ok_or_else(|| error(Stage::Close, Reason::Internal))?;
            if origin.closed().is_some() {
                return Ok(CloseReport::default());
            }
            let entry = self
                .entry
                .upgrade()
                .ok_or_else(|| error(Stage::Close, Reason::Revoked))?;
            // The original setup task observes this exact stop token. Retain
            // its exit before waiting for the state it held across native IO.
            *entry.protocol_wait.lock().unwrap() = Some(("setup exit receipt", None, None));
            entry.receive_setup_original().await?;
            *entry.protocol_wait.lock().unwrap() = Some(("entry state", None, None));
            let mut state = entry.state.lock().await;
            if !state
                .protocol_origin
                .as_ref()
                .is_some_and(|actual| actual.same_original(origin))
            {
                return Err(error(Stage::Close, Reason::StalePlan));
            }
            state.retire_protocol_endpoint_original(&entry).await?;
            if let Some(receipt) = state.anchor_receipt.as_ref().or(state.receipt.as_ref()) {
                origin
                    .retain_users_receipt(
                        receipt,
                        state
                            .tasks
                            .as_ref()
                            .ok_or_else(|| error(Stage::Close, Reason::Internal))?,
                    )
                    .await?;
            }
            // Only protocol, driver-body and transferred-view cleanup is
            // reported here. Native closure and real Host ACKs stay separate.
            Ok(CloseReport::default())
        })
    }
}

impl Generation {
    pub(super) fn observe_retained_original(
        &self,
        state: &EntryState,
        profile: usize,
        upstream: &UpstreamTransport,
    ) -> Result<(), ChannelError> {
        let Some(observer) = upstream.protocol_original_observer() else {
            return Ok(());
        };
        let origin = state
            .protocol_origin
            .as_ref()
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?;
        let retirement = state
            .retirement
            .as_ref()
            .ok_or_else(|| error(Stage::Prepare, Reason::Unsupported))?;
        let original = origin.retained_protocol_original(
            upstream,
            observer.clone(),
            &self.profiles[profile].endpoint,
            retirement,
        )?;
        observer.retain_protocol_original(original)
    }

    pub(super) fn prepare_original(
        &self,
        entry: &Arc<Entry>,
        state: &mut EntryState,
        candidate: &setup::Candidate,
    ) -> Result<Option<MetadataPoolPreparation>, ChannelError> {
        if state.permit.is_some() || state.retirement.is_some() || state.origin.is_some() {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        let origin = Arc::new(OnceLock::new());
        let stop = entry.cancel.child_token();
        let control = Box::new(Control {
            entry: Arc::downgrade(entry),
            origin: origin.clone(),
            stop: stop.clone(),
        })
        .into();
        let (prepared, retirement) = self.metadata[candidate.profile]
            .grants()
            .prepare_protocol_original(&self.tasks, control)
            .map_err(|rejected| rejected.error)?;
        let actual = prepared.origin();
        origin
            .set(actual.clone())
            .map_err(|_| error(Stage::Prepare, Reason::Internal))?;
        state.native_sources = actual.original_sources();
        state.protocol_origin = Some(actual.clone());
        state.origin = Some(actual);
        state.retirement = Some(retirement.clone());
        state.original_stop = Some(stop);
        state.protocol_closed = false;
        if let Some(observer) = candidate.upstream.protocol_original_observer() {
            let original = prepared.protocol_original(
                &candidate.upstream,
                observer.clone(),
                &self.profiles[candidate.profile].endpoint,
                &retirement,
            )?;
            observer.retain_protocol_original(original)?;
        }
        Ok(Some(prepared))
    }
}

impl Entry {
    pub(super) async fn retire_current_endpoint(&self, end: Instant) -> Result<(), ChannelError> {
        let original = {
            let state = self.state.lock().await;
            state
                .retirement
                .as_ref()
                .and_then(|_| state.protocol_origin.clone())
                .zip(state.tasks.clone())
        };
        if let Some((origin, tasks)) = original {
            // Never hold Entry state while its Root-held controller needs that
            // same state. This drives the existing persistent phase inline.
            if !origin.retire_protocol(&tasks, end).await? {
                return Err(self.close_timeout("original endpoint protocol retirement", None));
            }
        }
        let mut state = tokio::time::timeout_at(end, self.state.lock())
            .await
            .map_err(|source| {
                self.close_timeout("entry state before native close", Some(source))
            })?;
        state.retire_endpoint(self, end).await
    }
}
