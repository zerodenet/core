//! One Source-owned quota counter, born before decode and adopted unchanged.
use super::*;
use std::task::Waker;
use zero_traits::packet_metadata::{
    PacketMetadataArc, PacketMetadataCounter, PacketMetadataCredit, PacketMetadataLayout,
    PacketMetadataLimitCandidate, PacketMetadataSnapshot,
};
struct Memory {
    counter: Arc<PacketMetadataCounter>,
    policy_header: Mutex<Option<PacketMetadataCredit>>,
}
/// Allocation/snapshot/identity only. No raw counter or configuration issuer.
#[derive(Clone)]
pub struct QuotaMemoryBudget(PacketMetadataArc<Memory>);
/// Owned custody, not an IO or task permission. No foreign Waker is accepted.
pub struct QuotaMemoryCredit(PacketMetadataCredit);
fn memory_error(error: zero_traits::packet_metadata::PacketMemoryError) -> io::Error {
    let kind = match error {
        zero_traits::packet_metadata::PacketMemoryError::Busy
        | zero_traits::packet_metadata::PacketMemoryError::Capacity
        | zero_traits::packet_metadata::PacketMemoryError::Paused => io::ErrorKind::WouldBlock,
        zero_traits::packet_metadata::PacketMemoryError::Stopped => io::ErrorKind::BrokenPipe,
        zero_traits::packet_metadata::PacketMemoryError::Overflow => io::ErrorKind::InvalidData,
    };
    io::Error::new(kind, error)
}
pub(super) fn limits(
    runtime: &zero_config::RuntimeOptionsConfig,
) -> io::Result<PacketMetadataLayout> {
    let bytes = runtime.effective_principal_quota_total_max_storage_bytes();
    if bytes == 0 || std::alloc::Layout::array::<u8>(bytes).is_err() {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    Ok(PacketMetadataLayout::new(bytes, bytes))
}
impl QuotaMemoryBudget {
    fn new(runtime: &zero_config::RuntimeOptionsConfig) -> io::Result<Self> {
        let counter = PacketMetadataCounter::new_owned(limits(runtime)?).map_err(memory_error)?;
        // Reserve the real policy header before any data can fill this counter.
        // Failed cold preparation publishes neither counter nor native original.
        let policy_header = counter
            .reserve(
                PacketMetadataCounter::candidate_layout().map_err(memory_error)?,
                Waker::noop().clone(),
            )
            .map_err(memory_error)?;
        let header = counter
            .reserve(
                PacketMetadataArc::<Memory>::layout().map_err(memory_error)?,
                Waker::noop().clone(),
            )
            .map_err(memory_error)?;
        let value = Memory {
            counter,
            policy_header: Mutex::new(Some(policy_header)),
        };
        PacketMetadataArc::from_prepaid(value, header)
            .map(Self)
            .map_err(|(error, value, header)| {
                drop(value);
                drop(header);
                memory_error(error)
            })
    }
    pub fn snapshot(&self) -> PacketMetadataSnapshot {
        self.0.counter.snapshot()
    }
    pub fn same_budget(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0.counter, &other.0.counter)
    }
    pub fn reserve(&self, layout: PacketMetadataLayout) -> io::Result<QuotaMemoryCredit> {
        let wake = Waker::noop().clone();
        let header_wake = Waker::noop().clone();
        let mut header = self
            .0
            .policy_header
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if header.is_none() {
            *header = Some(
                self.0
                    .counter
                    .reserve(
                        PacketMetadataCounter::candidate_layout().map_err(memory_error)?,
                        header_wake,
                    )
                    .map_err(memory_error)?,
            );
        }
        drop(header);
        self.0
            .counter
            .reserve(layout, wake)
            .map(QuotaMemoryCredit)
            .map_err(memory_error)
    }
    fn prepare(
        &self,
        target: PacketMetadataLayout,
    ) -> io::Result<Option<PacketMetadataLimitCandidate>> {
        if self.snapshot().limit == target {
            return Ok(None);
        }
        let wake = Waker::noop().clone();
        let header = {
            let mut slot = self
                .0
                .policy_header
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            match slot.take() {
                Some(header) => header,
                None => self
                    .0
                    .counter
                    .reserve(
                        PacketMetadataCounter::candidate_layout().map_err(memory_error)?,
                        wake,
                    )
                    .map_err(memory_error)?,
            }
        };
        match self.0.counter.prepare_limit(target, header) {
            Ok(candidate) => Ok(Some(candidate)),
            Err((error, header)) => {
                let rejected = {
                    let mut slot = self
                        .0
                        .policy_header
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());
                    if slot.is_none() {
                        *slot = Some(header);
                        None
                    } else {
                        Some(header)
                    }
                };
                drop(rejected);
                Err(memory_error(error))
            }
        }
    }
}
impl std::fmt::Debug for QuotaMemoryBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuotaMemoryBudget")
            .field("usage", &self.snapshot())
            .finish()
    }
}
impl std::fmt::Debug for QuotaMemoryCredit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuotaMemoryCredit")
            .field("layout", &self.layout())
            .finish()
    }
}
impl QuotaMemoryCredit {
    /// Transfer only this already-paid storage custody into an existing concrete
    /// PacketMetadataArc. No counter/configuration or issuance authority escapes.
    pub fn into_metadata(self) -> PacketMetadataCredit {
        self.0
    }
    pub fn split(&mut self, layout: PacketMetadataLayout) -> io::Result<Self> {
        self.0
            .split_prepared(layout, Waker::noop().clone())
            .map(Self)
            .map_err(|(error, wake)| {
                drop(wake);
                memory_error(error)
            })
    }

    pub fn layout(&self) -> PacketMetadataLayout {
        self.0.layout()
    }
    /// Success returns the original now-empty other. Its caller drops that
    /// pointer/Waker after releasing the actual book/envelope mutex.
    pub fn absorb(&mut self, other: Self) -> Result<Self, (io::Error, Self)> {
        self.0
            .absorb(other.0)
            .map(Self)
            .map_err(|(error, credit)| (memory_error(error), Self(credit)))
    }
}
impl ConfigurationSource {
    pub fn quota_memory(
        &self,
        runtime: &zero_config::RuntimeOptionsConfig,
    ) -> io::Result<QuotaMemoryBudget> {
        let mut slot = self
            .quota
            .memory
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(memory) = slot.as_ref() {
            // Allocation/inspection borrows the authoritative same book. It
            // neither changes limits nor interprets an older config snapshot
            // as permission to defeat an in-flight publication fence.
            return Ok(memory.clone());
        }
        let memory = QuotaMemoryBudget::new(runtime)?;
        *slot = Some(memory.clone());
        Ok(memory)
    }
    /// The genuinely standalone Engine constructor is an explicit source
    /// configuration owner. Kernel changes must instead use the accepted
    /// publication candidate; a readonly allocator/inspector cannot grow it.
    pub fn configure_initial_quota_memory(&self, runtime: &zero_config::RuntimeOptionsConfig) -> io::Result<QuotaMemoryBudget> {
        let memory = self.quota_memory(runtime)?;
        if memory.snapshot().limit == limits(runtime)? { return Ok(memory); }
        if self.persistence_book.get().is_some() {
            return Err(io::Error::new(io::ErrorKind::PermissionDenied, "Kernel quota limit requires its accepted Source publication candidate"));
        }
        {
            let storage = self.quota.current.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
            if let Some(storage) = storage { storage.confirm_closed()?; }
        }
        if let Some(candidate) = memory.prepare(limits(runtime)?)? {
            candidate.seal().map_err(memory_error)?;
            candidate.validate_publication().map_err(memory_error)?;
            candidate.commit_validated_deferred();
            candidate.publish();
        }
        Ok(memory)
    }

    pub fn adopt_quota_memory(&self, roots: &crate::HostOriginalBudgets) -> io::Result<()> {
        let memory = self
            .quota
            .memory
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let Some(memory) = memory else {
            return Ok(());
        }; // exact no-quota cold declaration
        let counter = roots.quota_memory.get_or_init(|| memory.0.counter.clone());
        if !Arc::ptr_eq(counter, &memory.0.counter) {
            return Err(io::Error::other("foreign quota Source counter"));
        }
        let tasks = self.quota.tasks.get_or_init(|| roots.component_tasks.clone());
        if !Arc::ptr_eq(tasks, &roots.component_tasks) { return Err(io::Error::new(io::ErrorKind::PermissionDenied, "foreign quota Host task budget")); }
        let storage = self.quota.current.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
        if let Some(owned::StorageEntry::Owned(storage)) = storage {
            storage.execution.bind_kernel(tasks.clone())?;
        }
        Ok(())
    }
    pub fn prepare_quota_memory(
        &self,
        runtime: &zero_config::RuntimeOptionsConfig,
    ) -> io::Result<Option<PacketMetadataLimitCandidate>> {
        let memory = self
            .quota
            .memory
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        match memory {
            Some(memory) => memory.prepare(limits(runtime)?),
            None => Ok(None),
        }
    }
}
