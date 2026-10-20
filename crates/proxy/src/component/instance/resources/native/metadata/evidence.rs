use super::*;
#[derive(Clone, Debug)]
pub(in crate::component) struct OriginalUnknown {
    pub reservation: zero_platform_tokio::controlled_datagram::DatagramReservationId,
    pub source: Arc<std::io::Error>,
    pub acquisition_source: Option<Arc<std::io::Error>>,
}
#[derive(Clone, Debug, Default)]
pub(in crate::component) struct Originals {
    pub confirmed_closed: u64,
    pub known_no_native: u64,
    pub live: usize,
    pub unknown: Vec<OriginalUnknown>,
}
impl Originals {
    pub(super) fn complete(&self) -> bool {
        self.live == 0 && self.unknown.is_empty()
    }
    pub(super) fn record(&mut self, report: DatagramOwnerReport) {
        self.confirmed_closed += report.confirmed_closed;
        self.known_no_native += report.known_no_native;
        self.live = report.retained_live;
        self.unknown = report
            .unconfirmed
            .into_iter()
            .map(|value| OriginalUnknown {
                reservation: value.reservation,
                source: value.source,
                acquisition_source: value.acquisition_source,
            })
            .collect();
    }
}
#[derive(Clone, Debug)]
pub(in crate::component) struct MetadataIoEvidence {
    pub ordinal: usize,
    /// Busy invalidates native fields as a current completion snapshot. Parent
    /// lock contention returns conservative empty fields, never no-acquisition.
    pub busy: bool,
    pub attempted: bool,
    pub main: Originals,
    pub probes: Originals,
    pub first_fault: Option<Arc<ChannelError>>,
    // Private snapshot is made only from this provider's original issuers and
    // retained opaque confirmations. Mutable public diagnostic counts/fields
    // cannot be used to manufacture user-graph coverage.
    users: Option<UsersEvidence>,
}
#[derive(Clone, Debug)]
struct UsersEvidence {
    pending: usize,
    confirmed: Vec<MetadataUsersClosedReceipt>,
}
impl MetadataIoEvidence {
    pub(in crate::component) fn physical_confirmed(&self) -> bool {
        !self.busy
            && self.main.complete()
            && self.probes.complete()
            && self.users.as_ref().is_some_and(|users| users.pending == 0)
    }
    pub(in crate::component) fn unconfirmed_users(&self) -> Option<usize> {
        self.users.as_ref().map(|users| users.pending)
    }
    pub(in crate::component) fn confirmed_users(&self) -> usize {
        self.users.as_ref().map_or(0, |users| users.confirmed.len())
    }
}
impl MetadataDeclarations {
    pub(in crate::component::instance::resources::native) fn evidence(
        &self,
        component: ComponentId,
    ) -> Vec<MetadataIoEvidence> {
        self.entries
            .iter()
            .map(|(ordinal, entry)| {
                let Some(mut native) = entry.lock() else {
                    return MetadataIoEvidence {
                        ordinal: *ordinal,
                        busy: true,
                        attempted: true,
                        main: Default::default(),
                        probes: Default::default(),
                        first_fault: None,
                        users: None,
                    };
                };
                native.observe_originals(component);
                MetadataIoEvidence {
                    ordinal: *ordinal,
                    busy: native.main_busy || native.probes_busy,
                    attempted: native.attempted,
                    main: native.main.clone(),
                    probes: native.probes.clone(),
                    first_fault: native.first_fault.clone(),
                    users: Some(UsersEvidence {
                        pending: native
                            .users
                            .iter()
                            .filter(|claim| claim.transferred && claim.confirmed.is_none())
                            .count(),
                        confirmed: native
                            .users
                            .iter()
                            .filter_map(|claim| claim.confirmed.clone())
                            .collect(),
                    }),
                }
            })
            .collect()
    }
}
#[derive(Debug)]
pub(super) struct NativeSource(pub(super) Arc<std::io::Error>);
impl std::fmt::Display for NativeSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for NativeSource {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
