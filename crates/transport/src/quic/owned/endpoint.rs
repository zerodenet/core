//! Sealed Quinn user handles over the same actual driver/native owners.
use super::{
    drivers::Admission, DriverExitReceiver, DriverTaskStatus, MetadataSocketOwner, QuinnDriverOwner,
};
use std::{
    collections::BTreeMap,
    io,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc, Mutex, Weak,
    },
};
use tokio::time::Instant;
use zero_platform_tokio::{
    contract::{
        scope::{TaskJoined, TaskRegistrar},
        ChannelError, Impact, Reason, Stage,
    },
    controlled_datagram::{metadata::MetadataOwnerReport, DatagramReservationId},
};
mod connection;
mod handle;
mod native;
use native::Native;
mod stream;
mod users;
pub use connection::OwnedConnection;
pub use handle::OwnedEndpointHandle;
pub use stream::{OwnedRecvStream, OwnedSendStream};
use users::{error, Users};

/// Caller-assigned finite counts. No production configuration is implied.
#[derive(Clone, Copy, Debug)]
pub struct OwnedEndpointLimits {
    pub drivers: NonZeroUsize,
    pub users: NonZeroUsize,
}
pub struct OwnedEndpointOwner {
    shared: Arc<Shared>,
    native: Native,
    drivers: QuinnDriverOwner,
    native_snapshot: Option<Arc<MetadataOwnerReport>>,
    confirmed_main: u128,
    confirmed_probes: u128,
    native_count_overflow: bool,
    native_failures: BTreeMap<(bool, DatagramReservationId), Arc<OwnedNativeFailure>>,
}
struct Shared {
    endpoint: Mutex<Option<quinn::Endpoint>>,
    phase: AtomicU8,
    users: Arc<Users>,
    admission: Admission,
    callbacks: Weak<super::callbacks::Broker>,
}
mod report;
pub use report::{OwnedEndpointReport, OwnedEndpointUsersReport, OwnedNativeFailure};
impl OwnedEndpointOwner {
    /// Cold construction, with already installed original native ownership and
    /// the caller's existing task registrar. No task/endpoint is started here.
    pub fn new(
        native: MetadataSocketOwner,
        registrar: TaskRegistrar,
        limits: OwnedEndpointLimits,
    ) -> (Self, DriverExitReceiver) {
        Self::construct(Native::Owned(Box::new(native)), registrar, limits)
    }
    /// Same host metadata views and actual registrar; no native acquisition or
    /// control transfers to this consumer. Keep this owner before start.
    pub fn new_hosted(
        native: super::HostedMetadataSocket,
        registrar: TaskRegistrar,
        limits: OwnedEndpointLimits,
    ) -> Result<(Self, DriverExitReceiver), super::HostedMetadataSocket> {
        if !native.matches(&registrar) {
            return Err(native);
        }
        Ok(Self::construct(Native::Hosted(native), registrar, limits))
    }
    fn construct(
        native: Native,
        registrar: TaskRegistrar,
        limits: OwnedEndpointLimits,
    ) -> (Self, DriverExitReceiver) {
        let cancel = registrar.cancellation();
        let (drivers, receiver) = QuinnDriverOwner::new(registrar, limits.drivers);
        let users = Users::new(limits.users, cancel, drivers.admission());
        let shared = Arc::new(Shared {
            endpoint: Mutex::new(None),
            phase: AtomicU8::new(0),
            users,
            admission: drivers.admission(),
            callbacks: Arc::downgrade(native.callbacks()),
        });
        (
            Self {
                shared,
                native,
                drivers,
                native_snapshot: None,
                confirmed_main: 0,
                confirmed_probes: 0,
                native_count_overflow: false,
                native_failures: BTreeMap::new(),
            },
            receiver,
        )
    }
    /// The host retains this owner before starting actual Quinn drivers.
    pub fn start(
        &mut self,
        config: quinn::EndpointConfig,
        server: Option<quinn::ServerConfig>,
        client: Option<quinn::ClientConfig>,
    ) -> Result<OwnedEndpointHandle, ChannelError> {
        self.shared
            .phase
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| error(Stage::Prepare, Reason::Revoked))?;
        let result = quinn::Endpoint::new_with_abstract_socket(
            config,
            server,
            self.native.socket(),
            self.drivers
                .runtime_for_callbacks(self.native.callbacks().clone()),
        );
        let mut endpoint = match result {
            Ok(endpoint) => endpoint,
            Err(source) => {
                self.begin_shutdown();
                return Err(
                    ChannelError::new(Stage::Prepare, Reason::Io, Impact::Component)
                        .with_source(source),
                );
            }
        };
        if let Err(failure) = self.shared.admission.check().and_then(|()| {
            if self.shared.users.cancel.is_cancelled() {
                Err(error(Stage::Prepare, Reason::Revoked))
            } else {
                Ok(())
            }
        }) {
            endpoint.close(0u32.into(), b"driver admission rejected");
            drop(endpoint);
            self.begin_shutdown();
            return Err(failure);
        }
        if let Some(client) = client {
            endpoint.set_default_client_config(client);
        }
        *self.shared.endpoint.lock().unwrap() = Some(endpoint);
        self.shared.phase.store(2, Ordering::Release);
        Ok(OwnedEndpointHandle(Arc::downgrade(&self.shared)))
    }
    pub fn active_users(&self) -> usize {
        self.shared.users.active()
    }
    pub fn driver_tasks(&self) -> Vec<zero_core::component::TaskId> {
        self.drivers.tasks()
    }
    pub fn driver_status(&self) -> DriverTaskStatus {
        self.drivers.task_status()
    }
    pub fn reclaim_received_drivers(&self) {
        self.drivers.reclaim_received();
    }
    pub fn begin_shutdown(&self) {
        self.shared.phase.store(3, Ordering::Release);
        self.shared.users.cancel.cancel();
        self.drivers.revoke_registration();
        let endpoint = self.shared.endpoint.lock().unwrap().take();
        if let Some(endpoint) = endpoint {
            endpoint.close(0u32.into(), b"owner shutdown");
            drop(endpoint);
        }
    }
    /// Stop users, wait for this same host's actual task joins, then explicitly
    /// close originals. The host must drive its join collector concurrently.
    /// This never acknowledges an Auxiliary failure on behalf of a receiver.
    pub async fn shutdown(
        &mut self,
        deadline: Instant,
    ) -> Result<OwnedEndpointReport, ChannelError> {
        let users = self.shutdown_users(deadline).await?;
        let Native::Owned(native) = &mut self.native else {
            return Err(error(Stage::Close, Reason::Unsupported));
        };
        let joined = users.joined;
        if self
            .native_snapshot
            .as_ref()
            .is_none_or(|report| !report.is_complete())
            && !self.native_count_overflow
        {
            let snapshot = Arc::new(native.close_io());
            for (probe, report) in [(false, &snapshot.main), (true, &snapshot.probes)] {
                for unknown in &report.unconfirmed {
                    self.native_failures
                        .entry((probe, unknown.reservation))
                        .or_insert_with(|| {
                            Arc::new(OwnedNativeFailure {
                                probe,
                                reservation: unknown.reservation,
                                source: unknown.source.clone(),
                                acquisition_source: unknown.acquisition_source.clone(),
                            })
                        });
                }
            }
            // Persist each actual delta before any further fallible step. No
            // new acquisition/start is permitted during this owner generation.
            self.native_snapshot = Some(snapshot.clone());
            match (
                self.confirmed_main
                    .checked_add(snapshot.main.confirmed_closed as u128),
                self.confirmed_probes
                    .checked_add(snapshot.probes.confirmed_closed as u128),
            ) {
                (Some(main), Some(probes)) => {
                    self.confirmed_main = main;
                    self.confirmed_probes = probes;
                }
                _ => self.native_count_overflow = true,
            }
        }
        Ok(OwnedEndpointReport {
            active_users: self.active_users(),
            drivers: self.drivers.task_status(),
            joined,
            native: self.native_snapshot.as_ref().unwrap().clone(),
            confirmed_main: self.confirmed_main,
            confirmed_probes: self.confirmed_probes,
            native_count_overflow: self.native_count_overflow,
            original_native_failures: self.native_failures.values().cloned().collect(),
            observer_faulted: self.native.callbacks().failed(),
            suppressed_observer_panics: self.native.suppressed(),
        })
    }
    /// Complete only actual sealed users and driver joins. This never closes
    /// or acknowledges the granting host's native main/probe originals.
    pub async fn shutdown_users(
        &self,
        deadline: Instant,
    ) -> Result<OwnedEndpointUsersReport, ChannelError> {
        self.begin_shutdown();
        self.shared.users.wait_empty(deadline).await?;
        let joined = self.drivers.wait_joined(deadline).await?;
        Ok(OwnedEndpointUsersReport {
            active_users: self.active_users(),
            drivers: self.drivers.task_status(),
            joined,
            observer_faulted: self.native.callbacks().failed(),
            suppressed_observer_panics: self.native.suppressed(),
        })
    }
    pub fn take_suppressed_observer_panic(&mut self) -> Option<Box<dyn std::any::Any + Send>> {
        self.native.callbacks().take_suppressed()
    }
}
impl Drop for OwnedEndpointOwner {
    fn drop(&mut self) {
        // Emergency cancellation only, never a resource/TaskExit/native ACK.
        self.begin_shutdown();
    }
}
impl Shared {
    fn check(&self) -> Result<(), ChannelError> {
        if self.phase.load(Ordering::Acquire) != 2 || self.users.cancel.is_cancelled() {
            return Err(error(Stage::Prepare, Reason::Revoked));
        }
        self.admission.check()?;
        if self
            .callbacks
            .upgrade()
            .is_none_or(|callbacks| callbacks.failed())
        {
            return Err(error(Stage::Prepare, Reason::Revoked));
        }
        Ok(())
    }
}
