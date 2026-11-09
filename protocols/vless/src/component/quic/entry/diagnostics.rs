//! Only this actual Aux operation, its exceptional task exit, and one concrete
//! protocol-return invariant. No IO/Scope/controller or canonical backedge.
use super::*;
use std::sync::OnceLock;
use zero_platform_tokio::contract::CleanupFailureReceipt;
use zero_traits::packet_metadata::PacketMetadataLayout;
pub(super) struct Sources {
    operation: OnceLock<RetainedError>,
    terminal: OnceLock<Arc<ChannelError>>,
    physical: OnceLock<RetainedError>,
}
impl Sources {
    fn exceptional(&self) -> bool {
        self.physical.get().is_some()
            || (self.operation.get().is_some() && self.terminal.get().is_some())
    }
}
impl std::fmt::Debug for Sources {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActualQuicSetupSources")
            .field("operation", &self.operation.get().is_some())
            .field("terminal", &self.terminal.get().is_some())
            .field("physical", &self.physical.get().is_some())
            .finish()
    }
}
impl std::fmt::Display for Sources {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("actual QUIC setup/return sources")
    }
}
impl std::error::Error for Sources {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.physical
            .get()
            .or_else(|| self.operation.get())
            .map(|source| source as _)
            .or_else(|| self.terminal.get().map(|source| source.as_ref() as _))
    }
}
pub(in crate::component::quic) struct Diagnostics {
    payload: PacketMetadataArc<Sources>,
    receipt: CleanupFailureReceipt,
    operation_memory: Mutex<Option<PacketMetadataCredit>>,
    physical_memory: Mutex<Option<PacketMetadataCredit>>,
    canonical_memory: Mutex<Option<PacketMetadataCredit>>,
    canonical: Mutex<Option<RetainedError>>,
}
impl Diagnostics {
    pub fn prepare(memory: &PacketDriverMetadataView) -> Result<Self, ChannelError> {
        let source_layout = PacketMetadataArc::<Sources>::layout().map_err(capacity)?;
        let receipt_layout = CleanupFailureReceipt::metadata_layout().map_err(capacity)?;
        let error_layout = RetainedError::metadata_layout().map_err(capacity)?;
        let total = source_layout
            .checked_add(receipt_layout)
            .and_then(|v| v.checked_add(error_layout))
            .and_then(|v| v.checked_add(error_layout))
            .and_then(|v| v.checked_add(error_layout))
            .map_err(capacity)?;
        let mut paid = memory.reserve(total).map_err(capacity)?;
        let header = paid.split(source_layout).map_err(capacity)?;
        let payload = PacketMetadataArc::from_prepaid(
            Sources {
                operation: OnceLock::new(),
                terminal: OnceLock::new(),
                physical: OnceLock::new(),
            },
            header,
        )
        .map_err(|(source, _, _)| capacity(source))?;
        let receipt =
            CleanupFailureReceipt::from_prepaid(paid.split(receipt_layout).map_err(capacity)?)
                .map_err(|(source, _)| capacity(source))?;
        assert!(receipt.bind_metadata_payload(payload.clone().into_error()));
        let operation_memory = paid.split(error_layout).map_err(capacity)?;
        let physical_memory = paid.split(error_layout).map_err(capacity)?;
        Ok(Self {
            payload,
            receipt,
            operation_memory: Mutex::new(Some(operation_memory)),
            physical_memory: Mutex::new(Some(physical_memory)),
            canonical_memory: Mutex::new(Some(paid)),
            canonical: Mutex::new(None),
        })
    }
    pub fn operation(&self, source: ChannelError) -> RetainedError {
        if let Some(original) = self.payload.operation.get() {
            return original.clone();
        }
        let credit = self
            .operation_memory
            .lock()
            .unwrap()
            .take()
            .expect("one actual setup result");
        let source = RetainedError::from_prepaid(source, credit)
            .unwrap_or_else(|_| panic!("actual setup header prepaid"));
        assert!(self.payload.operation.set(source.clone()).is_ok());
        source
    }
    pub fn terminal(&self, source: Arc<ChannelError>) {
        if let Some(original) = self.payload.operation.get() {
            let mut observed = source.as_ref();
            for _ in 0..32 {
                let Some(retained) = observed.retained_diagnostic() else {
                    break;
                };
                if retained.ptr_eq(original) {
                    return;
                }
                observed = retained;
            }
        }
        // This is the sole exact TaskJoined, never another arbitrary retry.
        if self.payload.terminal.get().is_none() {
            let _ = self.payload.terminal.set(source);
        }
    }
    pub fn physical(&self, source: ChannelError) {
        if self.payload.physical.get().is_some() {
            return;
        }
        let credit = self
            .physical_memory
            .lock()
            .unwrap()
            .take()
            .expect("one concrete protocol invariant");
        let source = RetainedError::from_prepaid(source, credit)
            .unwrap_or_else(|_| panic!("actual protocol header prepaid"));
        let _ = self.payload.physical.set(source);
    }
    pub fn pending_failure(&self) -> Option<RetainedError> {
        if !self.payload.exceptional() || self.receipt.is_acknowledged() {
            return None;
        }
        let previous = self.canonical.lock().unwrap().clone();
        if let Some(previous) = previous {
            return Some(previous);
        }
        let credit = self
            .canonical_memory
            .lock()
            .unwrap()
            .take()
            .expect("one actual canonical source");
        let error = error(Stage::Close, Reason::Io)
            .with_metadata_source(self.payload.clone().into_error())
            .with_cleanup_failure_receipt(self.receipt.clone());
        let original = RetainedError::from_prepaid(error, credit)
            .unwrap_or_else(|_| panic!("canonical header prepaid"));
        *self.canonical.lock().unwrap() = Some(original.clone());
        Some(original)
    }
}
fn capacity(source: impl std::error::Error + Send + Sync + 'static) -> ChannelError {
    error(Stage::Prepare, Reason::Capacity).with_source(source)
}
