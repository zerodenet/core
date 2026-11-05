//! Bounded read-only observations of actual query registrations and native I/O.
use super::*;
use std::sync::atomic::Ordering;
use zero_platform_tokio::controlled_carrier::CarrierOrigin;

pub struct OwnedDnsQueryObservation {
    pub task: TaskJoinWitness,
    pub original: Option<CarrierOrigin>,
    pub metadata:
        Option<zero_platform_tokio::controlled_datagram::metadata_pool::MetadataPoolOrigin>,
    pub io_pending: bool,
}
impl OwnedDnsQueryTasks {
    /// The provider records its original before awaiting QUIC setup. This weak
    /// observation grants neither acquisition nor close authority.
    pub fn record_metadata(
        original: &zero_platform_tokio::controlled_datagram::metadata_pool::MetadataPoolOrigin,
    ) -> io::Result<()> {
        QUERY_RECORD
            .try_with(|record| {
                let retired = record
                    .metadata
                    .lock()
                    .expect("DNS metadata original poisoned")
                    .replace(original.clone());
                drop(retired);
            })
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "metadata I/O has no actual DNS query record",
                )
            })
    }
    /// Confirm joins so a host can continue native cleanup even after a joined
    /// worker fault. The actual TaskScope retains and reports that same fault;
    /// this is neither task success nor native-I/O closure evidence.
    pub async fn wait_closed(&self, deadline: tokio::time::Instant) -> io::Result<()> {
        let result = self.shutdown(deadline).await;
        let joined = {
            let state = self.0.state.lock().expect("DNS query tasks poisoned");
            state.closing && state.records.is_empty() && !state.identity_fault
        };
        if joined {
            Ok(())
        } else {
            result
        }
    }
    pub fn observe(&self) -> Vec<OwnedDnsQueryObservation> {
        let records = self
            .0
            .state
            .lock()
            .expect("DNS query tasks poisoned")
            .records
            .clone();
        records
            .into_iter()
            .filter_map(|record| {
                let registration = record.registration.get()?.as_ref()?;
                let original = record
                    .original
                    .lock()
                    .expect("DNS query original poisoned")
                    .clone();
                Some(OwnedDnsQueryObservation {
                    task: registration.witness.clone(),
                    original,
                    metadata: record
                        .metadata
                        .lock()
                        .expect("DNS metadata original poisoned")
                        .clone(),
                    io_pending: record.io_pending.load(Ordering::Acquire),
                })
            })
            .collect()
    }
    /// Associate the actual provider original with this registered caller
    /// before protocol preparation awaits. This grants no close authority.
    pub fn record_native(original: &CarrierOrigin) -> io::Result<()> {
        QUERY_RECORD
            .try_with(|record| {
                let retired = record
                    .original
                    .lock()
                    .expect("DNS query original poisoned")
                    .replace(original.clone());
                drop(retired);
            })
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "native I/O has no original query record",
                )
            })
    }
    pub(crate) async fn native_io<T>(work: impl Future<Output = T>) -> io::Result<T> {
        let record = QUERY_RECORD.try_with(Clone::clone).map_err(|_| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "native I/O has no original query task",
            )
        })?;
        struct Pending(Arc<Record>);
        impl Drop for Pending {
            fn drop(&mut self) {
                self.0.io_pending.store(false, Ordering::Release);
            }
        }
        let pending = Pending(record);
        tokio::pin!(work);
        let result = std::future::poll_fn(|cx| {
            let result = work.as_mut().poll(cx);
            pending
                .0
                .io_pending
                .store(result.is_pending(), Ordering::Release);
            result
        })
        .await;
        Ok(result)
    }
}
