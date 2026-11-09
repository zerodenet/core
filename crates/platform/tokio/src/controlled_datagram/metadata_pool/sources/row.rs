//! Only the same Root row owns this wrapper; the public getter is native-only.
use super::*;
use crate::contract::{
    lifecycle::{ResourceFailure, ResourceFailurePhase},
    CleanupFailureReceipt,
};
pub(super) struct Payload {
    originals: MetadataOriginalSources,
    protocol: OnceLock<Arc<ResourceFailure>>,
}
pub(in crate::controlled_datagram::metadata_pool) struct MetadataCloseCohort {
    instance: ComponentInstanceId,
    payload: Allocation<Payload>,
    receipt: CleanupFailureReceipt,
    canonical: Canonical,
    failure: OnceLock<Arc<ResourceFailure>>,
}
impl MetadataCloseCohort {
    pub(in crate::controlled_datagram::metadata_pool) fn new(
        instance: ComponentInstanceId,
        limits: super::super::super::metadata::MetadataLimits,
        prepaid: &mut Prepaid,
    ) -> Self {
        let originals = MetadataOriginalSources::new(limits, prepaid);
        let payload = prepaid.allocate(Payload {
            originals,
            protocol: OnceLock::new(),
        });
        let receipt = prepaid.receipt();
        assert!(payload.bind(&receipt));
        Self {
            instance,
            payload,
            receipt,
            canonical: prepaid.canonical(),
            failure: OnceLock::new(),
        }
    }
    pub(in crate::controlled_datagram::metadata_pool) fn originals(
        &self,
    ) -> MetadataOriginalSources {
        self.payload.originals.clone()
    }
    pub(in crate::controlled_datagram::metadata_pool) fn receive_protocol(
        &self,
        source: Arc<ResourceFailure>,
    ) {
        assert_eq!(source.instance, self.instance);
        assert_eq!(source.controller, ResourceFailurePhase::DeclaredResources);
        let original = self.payload.protocol.get_or_init(|| source.clone());
        assert!(
            Arc::ptr_eq(original, &source),
            "same actual phase has one canonical row"
        );
        if let (Some(row), Some(ledger)) = (self.failure.get(), original.original_sources()) {
            assert!(row.install_original_sources(ledger.clone()));
        }
        // This pure row durably retains that exact child before local ACK. The
        // Root receives our own canonical separately; none of this closes IO.
        assert!(original.acknowledge_retained(original));
    }
    pub(in crate::controlled_datagram::metadata_pool) fn failure(
        &self,
    ) -> Option<zero_error::channel::RetainedError> {
        if !self.payload.originals.failed() && self.payload.protocol.get().is_none() {
            return None;
        }
        Some(self.canonical.get_or_init(|| {
            let error = self
                .payload
                .protocol
                .get()
                .map(|actual| ChannelError::retained(actual.source.clone()))
                .unwrap_or_else(|| ChannelError::new(Stage::Close, Reason::Io, Impact::Component));
            self.payload
                .attach(error.with_cleanup_failure_receipt(self.receipt.clone()))
        }))
    }
    pub(in crate::controlled_datagram::metadata_pool) fn resource_failure(
        &self,
    ) -> Option<Arc<ResourceFailure>> {
        let source = self.failure()?;
        let actual = self
            .failure
            .get_or_init(|| {
                Arc::new(ResourceFailure::new(
                    self.instance,
                    ResourceFailurePhase::DeclaredResources,
                    source,
                ))
            })
            .clone();
        // Pair this check with receive_protocol's check of the published row:
        // neither concurrent OnceLock initialization ordering can miss a ledger.
        if let Some(ledger) = self
            .payload
            .protocol
            .get()
            .and_then(|phase| phase.original_sources())
        {
            assert!(actual.install_original_sources(ledger.clone()));
        }
        Some(actual)
    }
    pub(in crate::controlled_datagram::metadata_pool) fn fault_received(&self) -> bool {
        self.resource_failure()
            .is_none_or(|source| source.ownership_received())
    }
}
impl fmt::Debug for Payload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetadataOriginCloseSources")
            .field("protocol", &self.protocol.get().is_some())
            .field("native", &self.originals)
            .finish()
    }
}
impl fmt::Display for Payload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("same metadata original protocol and native sources")
    }
}
impl Error for Payload {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.protocol
            .get()
            .map(|actual| actual.source.as_ref() as &(dyn Error + 'static))
            .or_else(|| self.originals.source())
    }
}
