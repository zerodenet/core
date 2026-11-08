//! Native-only immutable-position sources for one actual MetadataPool origin.
//! The one attempted initialization bounds main IDs and probe attempt IDs.
use super::*;
use std::{
    error::Error,
    fmt, io,
    sync::atomic::{AtomicBool, Ordering},
};
mod row;
pub(super) use row::MetadataCloseCohort;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataOriginalNamespace {
    Main,
    Probe,
}
#[derive(Default)]
struct Original {
    reservation: OnceLock<super::super::DatagramReservationId>,
    acquisition: OnceLock<Arc<io::Error>>,
    close: OnceLock<Arc<io::Error>>,
    unknown: OnceLock<Arc<io::Error>>,
}
struct Originals {
    main: Box<[Original]>,
    probes: Box<[Original]>,
    failed: AtomicBool,
}
/// Getter-only native causes. Cloning retains exactly this same actual origin's
/// finite rows; it grants neither native IO nor receiver/registration ACK.
#[derive(Clone)]
pub struct MetadataOriginalSources(
    Arc<Originals>,
    Arc<OnceLock<Arc<ChannelError>>>,
    crate::contract::CleanupFailureReceipt,
    Arc<NativePayload>,
);
#[derive(Clone, Copy)]
pub struct MetadataOriginalFault<'a> {
    pub namespace: MetadataOriginalNamespace,
    pub reservation: super::super::DatagramReservationId,
    pub acquisition: Option<&'a Arc<io::Error>>,
    pub first_close: Option<&'a Arc<io::Error>>,
    pub unknown: Option<&'a Arc<io::Error>>,
}
impl MetadataOriginalSources {
    fn new(limits: super::super::metadata::MetadataLimits) -> Self {
        let rows = |len| {
            (0..len)
                .map(|_| Original::default())
                .collect::<Vec<_>>()
                .into_boxed_slice()
        };
        let originals = Arc::new(Originals {
            main: rows(limits.main_sockets.get()),
            probes: rows(limits.probe_attempts_per_initialization.get()),
            failed: AtomicBool::new(false),
        });
        let receipt = crate::contract::CleanupFailureReceipt::new();
        let payload = Arc::new(NativePayload(originals.clone()));
        assert!(receipt.bind_payload(payload.clone()));
        Self(originals, Arc::new(OnceLock::new()), receipt, payload)
    }
    pub fn same_original(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub fn failed(&self) -> bool {
        self.0.failed.load(Ordering::Acquire)
    }
    /// Stable native-only diagnostic for the actual protocol receiver. The
    /// payload cannot retain this canonical cell or a MetadataPool Slot.
    pub fn canonical_error(&self) -> Option<Arc<ChannelError>> {
        if !self.failed() {
            return None;
        }
        Some(
            self.1
                .get_or_init(|| {
                    Arc::new(
                        ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                            .with_source(self.3.clone())
                            .with_cleanup_failure_receipt(self.2.clone()),
                    )
                })
                .clone(),
        )
    }
    pub fn originals(&self) -> impl Iterator<Item = MetadataOriginalFault<'_>> {
        original_rows(MetadataOriginalNamespace::Main, &self.0.main).chain(original_rows(
            MetadataOriginalNamespace::Probe,
            &self.0.probes,
        ))
    }
    pub(in crate::controlled_datagram) fn validate_limits(
        limits: super::super::metadata::MetadataLimits,
    ) -> io::Result<()> {
        for count in [
            limits.main_sockets.get(),
            limits.probe_attempts_per_initialization.get(),
        ] {
            std::alloc::Layout::array::<Original>(count).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "metadata original source layout exceeds allocation range",
                )
            })?;
        }
        Ok(())
    }
    pub(in crate::controlled_datagram) fn mark_failed(&self) {
        self.0.failed.store(true, Ordering::Release);
    }
    fn row(
        &self,
        namespace: MetadataOriginalNamespace,
        reservation: super::super::DatagramReservationId,
    ) -> &Original {
        let rows = match namespace {
            MetadataOriginalNamespace::Main => &self.0.main,
            MetadataOriginalNamespace::Probe => &self.0.probes,
        };
        let index = usize::try_from(reservation.get()).expect("bounded native original");
        let row = rows
            .get(index)
            .expect("one actual initialization obeys declared main/probe attempts");
        assert_eq!(*row.reservation.get_or_init(|| reservation), reservation);
        row
    }
    pub(in crate::controlled_datagram) fn acquisition(
        &self,
        namespace: MetadataOriginalNamespace,
        reservation: super::super::DatagramReservationId,
        source: &Arc<io::Error>,
    ) {
        self.row(namespace, reservation)
            .acquisition
            .get_or_init(|| source.clone());
    }
    pub(in crate::controlled_datagram) fn try_capture(
        &self,
        namespace: MetadataOriginalNamespace,
        owner: &super::super::ControlledDatagramOwner,
    ) {
        for entry in owner.entries.values() {
            let row = self.row(namespace, entry.id);
            if let Some(source) = entry.first_close_failure.get() {
                row.close.get_or_init(|| source.clone());
                self.mark_failed();
            }
            let acquisition = match entry.acquisition_failure.try_lock() {
                Ok(source) => Some(source),
                Err(std::sync::TryLockError::Poisoned(source)) => Some(source.into_inner()),
                Err(std::sync::TryLockError::WouldBlock) => None,
            };
            if let Some(acquisition) = acquisition {
                if let Some(source) = acquisition.as_ref() {
                    row.acquisition.get_or_init(|| source.clone());
                }
            }
            let native = match entry.native.try_lock() {
                Ok(native) => Some(native),
                Err(std::sync::TryLockError::Poisoned(native)) => Some(native.into_inner()),
                Err(std::sync::TryLockError::WouldBlock) => None,
            };
            if let Some(native) = native {
                if let super::super::NativeHandleState::Unknown(source) = &*native {
                    row.unknown.get_or_init(|| source.clone());
                    self.mark_failed();
                }
            }
        }
    }
    pub(in crate::controlled_datagram) fn report(
        &self,
        namespace: MetadataOriginalNamespace,
        owner: &super::super::ControlledDatagramOwner,
        report: &super::super::DatagramOwnerReport,
    ) {
        for unknown in &report.unconfirmed {
            let row = self.row(namespace, unknown.reservation);
            if let Some(source) = &unknown.acquisition_source {
                row.acquisition.get_or_init(|| source.clone());
            }
            if let Some(source) = owner
                .entries
                .get(&unknown.reservation)
                .and_then(|entry| entry.first_close_failure.get())
            {
                row.close.get_or_init(|| source.clone());
            }
            // The report carries the original's canonical Unknown source;
            // latest retry SOCKET custody remains exclusively in native state.
            row.unknown.get_or_init(|| unknown.source.clone());
            self.mark_failed();
        }
    }
    /// No report/Vec is retained or replaced here. Each reference is cloned only
    /// into its empty original cell while the actual owner retains that source.
    pub(in crate::controlled_datagram) fn capture(
        &self,
        namespace: MetadataOriginalNamespace,
        owner: &super::super::ControlledDatagramOwner,
    ) {
        for entry in owner.entries.values() {
            let row = self.row(namespace, entry.id);
            let native = entry.lock();
            let acquisition = entry
                .acquisition_failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(source) = acquisition.as_ref() {
                self.acquisition(namespace, entry.id, source);
            }
            if let Some(source) = entry.first_close_failure.get() {
                row.close.get_or_init(|| source.clone());
                self.mark_failed();
            }
            if let super::super::NativeHandleState::Unknown(source) = &*native {
                row.unknown.get_or_init(|| source.clone());
                self.mark_failed();
            }
        }
    }
}
impl fmt::Debug for MetadataOriginalSources {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetadataOriginalSources")
            .field("main_limit", &self.0.main.len())
            .field("probe_attempts", &self.0.probes.len())
            .field("recorded", &self.originals().count())
            .field("failed", &self.failed())
            .finish()
    }
}
impl fmt::Display for MetadataOriginalSources {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("same metadata origin main/probe native sources")
    }
}
impl Error for MetadataOriginalSources {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.originals()
            .find_map(|row| row.acquisition.or(row.first_close).or(row.unknown))
            .map(|source| source.as_ref() as _)
    }
}

#[cfg(all(test, target_os = "linux"))]
#[path = "../../../tests/controlled_datagram/metadata_cohort.rs"]
mod tests;

fn original_rows(
    namespace: MetadataOriginalNamespace,
    originals: &[Original],
) -> impl Iterator<Item = MetadataOriginalFault<'_>> {
    originals.iter().filter_map(move |row| {
        Some(MetadataOriginalFault {
            namespace,
            reservation: *row.reservation.get()?,
            acquisition: row.acquisition.get(),
            first_close: row.close.get(),
            unknown: row.unknown.get(),
        })
    })
}

struct NativePayload(Arc<Originals>);
impl fmt::Debug for NativePayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeMetadataSources")
            .field("main", &self.0.main.len())
            .field("probes", &self.0.probes.len())
            .finish_non_exhaustive()
    }
}
impl fmt::Display for NativePayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("same original main/probe metadata native source cells")
    }
}
impl Error for NativePayload {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        original_rows(MetadataOriginalNamespace::Main, &self.0.main)
            .chain(original_rows(
                MetadataOriginalNamespace::Probe,
                &self.0.probes,
            ))
            .find_map(|row| row.acquisition.or(row.first_close).or(row.unknown))
            .map(|source| source.as_ref() as _)
    }
}
