use super::*;
pub(super) struct Receiver {
    pub(super) binding: UpstreamBinding,
    pub(super) scope: TaskScopeIdentity,
    pub(super) anchor: OnceLock<PendingCarrierObserver>,
    pub(super) original: Mutex<Option<ProtocolOriginal>>,
}
impl UpstreamOpener for Receiver {
    fn protocol_original_observer(&self) -> Option<PendingCarrierObserver> {
        self.anchor.get().cloned()
    }
    fn retain_protocol_original(
        &self,
        binding: &UpstreamBinding,
        original: ProtocolOriginal,
    ) -> Result<(), ChannelError> {
        if binding != &self.binding
            || !original.matches_observer(self.anchor.get().unwrap())
            || !original.scope_identity().same_scope(&self.scope)
        {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::StalePlan,
                crate::contract::Impact::Component,
            ));
        }
        let mut same = self.original.lock().unwrap();
        if let Some(previous) = &*same {
            assert!(previous.same_original(&original));
        } else {
            *same = Some(original);
        }
        Ok(())
    }
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: zero_core::request::Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<crate::contract::stream::BoxStream, ChannelError>> {
        Box::pin(async { panic!("observer cannot open native IO") })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: crate::contract::datagram::DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<crate::contract::datagram::BoxDatagramSession, ChannelError>> {
        Box::pin(async { panic!("observation grants no native datagram IO") })
    }
}
pub(super) struct OriginalClose {
    pub(super) ready: AtomicBool,
    pub(super) changed: tokio::sync::Notify,
    pub(super) origin: OnceLock<MetadataPoolOrigin>,
    pub(super) tasks: crate::contract::scope::TaskRegistrar,
    pub(super) wire: Mutex<Option<crate::contract::component::GrantedMetadataListener>>,
    pub(super) claim: Mutex<Option<MetadataViewClaim>>,
}
pub(super) struct Control(pub(super) Arc<OriginalClose>);
impl DynChannelControl for Control {
    fn cancel(&self, _: CloseReason) {}
    fn wait_closed(&mut self, _: Instant) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        Box::pin(async move {
            loop {
                let changed = self.0.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.0.ready.load(Ordering::Acquire) {
                    break;
                }
                changed.await;
            }
            let wire = { self.0.wire.lock().unwrap().take() };
            if let Some(mut wire) = wire {
                let claim = wire.users_claim.take();
                drop(wire);
                *self.0.claim.lock().unwrap() = claim;
            }
            let mut claim = self.0.claim.lock().unwrap().take();
            if claim.is_some() {
                self.0
                    .origin
                    .get()
                    .unwrap()
                    .return_unbound(&mut claim, &self.0.tasks)?;
            } else {
                assert!(
                    self.0.origin.get().unwrap().endpoints().is_none(),
                    "known unacquired original has no native views to return"
                );
            }
            Ok(CloseReport::default())
        })
    }
}
pub(super) fn observer(
    binding: &UpstreamBinding,
    tasks: &crate::contract::scope::TaskRegistrar,
) -> (Arc<Receiver>, UpstreamTransport) {
    let receiver = Arc::new(Receiver {
        binding: binding.clone(),
        scope: tasks.scope_identity(),
        anchor: OnceLock::new(),
        original: Mutex::new(None),
    });
    let transport = UpstreamTransport::new(binding.clone(), receiver.clone());
    assert!(receiver
        .anchor
        .set(transport.pending_carrier_observer())
        .is_ok());
    (receiver, transport)
}
