use super::*;
/// Native receipts, actual joined drivers and user release remain distinct.
#[derive(Debug)]
pub struct OwnedEndpointReport {
    pub active_users: usize,
    pub drivers: DriverTaskStatus,
    pub joined: Vec<TaskJoined>,
    pub native: Arc<MetadataOwnerReport>,
    pub confirmed_main: u128,
    pub confirmed_probes: u128,
    pub native_count_overflow: bool,
    pub original_native_failures: Vec<Arc<OwnedNativeFailure>>,
    pub observer_faulted: bool,
    pub suppressed_observer_panics: usize,
}
/// Logical user and real task results only; native cleanup stays with host.
#[derive(Debug)]
pub struct OwnedEndpointUsersReport {
    pub active_users: usize,
    pub drivers: DriverTaskStatus,
    pub joined: Vec<TaskJoined>,
    pub observer_faulted: bool,
    pub suppressed_observer_panics: usize,
}
impl OwnedEndpointUsersReport {
    pub fn users_and_drivers_complete(&self) -> bool {
        self.active_users == 0
            && self.drivers.pending_registration == 0
            && self.drivers.unjoined.is_empty()
            && self.drivers.unreceived.is_empty()
            && self.suppressed_observer_panics == 0
    }
}
/// Original unresolved I/O sources, with the actual reservation namespace.
#[derive(Debug)]
pub struct OwnedNativeFailure {
    pub probe: bool,
    pub reservation: DatagramReservationId,
    pub source: Arc<io::Error>,
    pub acquisition_source: Option<Arc<io::Error>>,
}
impl OwnedEndpointReport {
    pub fn is_complete(&self) -> bool {
        self.active_users == 0
            && self.drivers.pending_registration == 0
            && self.drivers.unjoined.is_empty()
            && self.drivers.unreceived.is_empty()
            && self.native.is_complete()
            && !self.native_count_overflow
            && self.suppressed_observer_panics == 0
    }
}
