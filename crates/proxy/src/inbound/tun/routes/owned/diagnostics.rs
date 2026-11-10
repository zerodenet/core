//! The route worker's fixed failure phases, on its original prepaid Root.
use super::*;
use zero_platform_tokio::contract::{CleanupFailureReceipt, RetainedError};
use zero_traits::packet_metadata::PacketMetadataError;
#[derive(Clone)]
pub(super) struct RouteDiagnostics {
    record: PacketMetadataArc<Record>,
    receipt: CleanupFailureReceipt,
    error: RetainedError,
}
struct Record {
    operation: OnceLock<SourceError>,
    cleanup: OnceLock<SourceError>,
    files: zero_tun::RouteFileFaults,
    file_error: PacketMetadataError,
}
impl RouteDiagnostics {
    pub(super) fn layout(
    ) -> Result<PacketMetadataLayout, zero_tun::packet_memory::PacketMemoryError> {
        PacketMetadataArc::<Record>::layout()?
            .checked_add(RetainedError::metadata_layout()?)?
            .checked_add(CleanupFailureReceipt::metadata_layout()?)?
            .checked_add(zero_tun::RouteFileFaults::metadata_layout()?)
    }
    pub(super) fn from_prepaid(mut credit: PacketMetadataCredit) -> Result<Self, ChannelError> {
        let capacity = |source| {
            error(Reason::Capacity, "same route diagnostic storage cannot fit").with_source(source)
        };
        let header = credit
            .split(PacketMetadataArc::<Record>::layout().map_err(capacity)?)
            .map_err(capacity)?;
        let file_credit = credit
            .split(zero_tun::RouteFileFaults::metadata_layout().map_err(capacity)?)
            .map_err(capacity)?;
        let receipt_credit = credit
            .split(CleanupFailureReceipt::metadata_layout().map_err(capacity)?)
            .map_err(capacity)?;
        let receipt = CleanupFailureReceipt::from_prepaid(receipt_credit)
            .map_err(|(source, _)| capacity(source))?;
        let files = zero_tun::RouteFileFaults::from_prepaid(file_credit)
            .map_err(|(source, _)| capacity(source))?;
        let file_error = files.diagnostic();
        let record = PacketMetadataArc::from_prepaid(
            Record {
                operation: OnceLock::new(),
                cleanup: OnceLock::new(),
                files,
                file_error,
            },
            header,
        )
        .map_err(|(source, _, _)| capacity(source))?;
        assert!(receipt.bind_metadata_payload(record.clone().into_error()));
        let source = ChannelError::new(Stage::Prepare, Reason::Io, Impact::Component)
            .with_metadata_source(record.clone().into_error())
            .with_cleanup_failure_receipt(receipt.clone());
        let error = RetainedError::from_prepaid(source, credit)
            .map_err(|(source, _, _)| capacity(source))?;
        Ok(Self {
            record,
            receipt,
            error,
        })
    }
    pub(super) fn files(&self) -> &zero_tun::RouteFileFaults {
        &self.record.files
    }
    pub(super) fn record_operation(&self, source: SourceError) {
        // The actual command waiter also owns its returned error. Preserve the
        // worker's first failed operation, matching its established semantics.
        let _ = self.record.operation.set(source);
    }
    pub(super) fn record_cleanup(&self, source: SourceError) {
        let _ = self.record.cleanup.set(source);
    }
    pub(super) fn unaccepted_failure(&self) -> Option<ChannelError> {
        (self.has_failures() && !self.receipt.is_acknowledged())
            .then(|| ChannelError::retained(self.error.clone()))
    }
    fn has_failures(&self) -> bool {
        self.record.operation.get().is_some()
            || self.record.cleanup.get().is_some()
            || self.record.files.has_failures()
    }
    pub(super) fn finish_worker(
        &self,
        operation: Option<SourceError>,
        cleanup: Option<SourceError>,
    ) -> Result<(), ChannelError> {
        // Only the one original blocking route worker reaches its final exit.
        // Its owner and all Native gates have been released before these moves.
        if let Some(source) = operation {
            self.record_operation(source);
        }
        if let Some(source) = cleanup {
            self.record_cleanup(source);
        }
        if self.has_failures() {
            Err(ChannelError::retained(self.error.clone()))
        } else {
            Ok(())
        }
    }
}
impl std::fmt::Debug for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OriginalRouteWorkerFailures")
            .field("operation", &self.operation.get().is_some())
            .field("cleanup", &self.cleanup.get().is_some())
            .field("files", &self.files.has_failures())
            .finish()
    }
}
impl std::fmt::Display for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original route worker retained its actual failures")?;
        for source in [self.operation.get(), self.cleanup.get()]
            .into_iter()
            .flatten()
        {
            write!(f, "; {source}")?;
        }
        if self.files.has_failures() {
            write!(f, "; {}", self.file_error.as_error())?;
        }
        Ok(())
    }
}
impl std::error::Error for Record {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.operation
            .get()
            .or_else(|| self.cleanup.get())
            .map(|source| source.as_ref() as &(dyn std::error::Error + 'static))
            .or_else(|| {
                self.files
                    .has_failures()
                    .then(|| self.file_error.as_error())
            })
    }
}
