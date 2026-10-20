//! Quinn-specific driver output. The original task/receipt ledger lives once
//! in neutral platform tracking and is shared by every runtime and user lease.
use std::{io, num::NonZeroUsize, sync::Arc};
use zero_core::component::TaskId;
use zero_platform_tokio::{
    contract::{
        scope::{TaskJoined, TaskRegistrar},
        ChannelError,
    },
    controlled_datagram::metadata::users::drivers as tracked,
};
mod runtime;

/// Actual lifecycle origin; H3 is never labelled as a Quinn endpoint or
/// connection driver. Every variant uses the same original host task ledger.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnedDriverOrigin {
    Quinn(quinn::DriverOrigin),
    #[cfg(any(feature = "split_http", feature = "hysteria"))]
    Http3,
}
impl From<quinn::DriverOrigin> for OwnedDriverOrigin {
    fn from(origin: quinn::DriverOrigin) -> Self {
        Self::Quinn(origin)
    }
}

pub type DriverExitReceiver = tracked::DriverExitReceiver<OwnedDriverOrigin, DriverBody>;
pub type DriverDelivery = tracked::DriverDelivery<DriverBody>;
pub type DriverTaskStatus = tracked::DriverTaskStatus<OwnedDriverOrigin>;
pub type DriverRegistrationFailure = tracked::DriverRegistrationFailure<OwnedDriverOrigin>;
pub use tracked::DriverRegistrationReason;
pub(super) type Admission = tracked::Admission<OwnedDriverOrigin, DriverBody>;
/// Original driver output, independent of real native task consumption.
#[derive(Debug)]
pub struct DriverBody {
    pub origin: OwnedDriverOrigin,
    pub io_error: Option<Arc<io::Error>>,
    pub close_reason: Option<quinn::ConnectionError>,
    #[cfg(any(feature = "split_http", feature = "hysteria"))]
    pub http3: Option<Arc<super::h3::Http3DriverExit>>,
}

pub struct QuinnDriverOwner {
    ledger: tracked::DriverOwner<OwnedDriverOrigin, DriverBody>,
}
impl QuinnDriverOwner {
    pub(super) fn from_ledger(ledger: tracked::DriverOwner<OwnedDriverOrigin, DriverBody>) -> Self {
        Self { ledger }
    }

    pub fn new(registrar: TaskRegistrar, limit: NonZeroUsize) -> (Self, DriverExitReceiver) {
        let (ledger, receiver) = tracked::DriverOwner::new(registrar, limit);
        (Self { ledger }, receiver)
    }
    /// Abstract metadata sockets only; no native acquisition is granted here.
    pub fn runtime(&self, socket: &super::MetadataSocketOwner) -> Arc<dyn quinn::Runtime> {
        self.runtime_for_callbacks(socket.callbacks.clone())
    }
    pub(super) fn runtime_for_callbacks(
        &self,
        callbacks: Arc<super::callbacks::Broker>,
    ) -> Arc<dyn quinn::Runtime> {
        Arc::new(runtime::OwnedRuntime {
            owner: self.ledger.downgrade(),
            callbacks,
        })
    }
    pub fn revoke_registration(&self) {
        self.ledger.revoke_registration();
    }
    pub(super) fn admission(&self) -> Admission {
        self.ledger.admission()
    }
    pub(super) async fn wait_joined(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<Vec<TaskJoined>, ChannelError> {
        self.ledger.wait_joined(deadline).await
    }
    pub fn task_status(&self) -> DriverTaskStatus {
        self.ledger.task_status()
    }
    pub fn tasks(&self) -> Vec<TaskId> {
        self.ledger.tasks()
    }
    pub fn body(&self, id: TaskId) -> Option<Arc<DriverBody>> {
        self.ledger.body(id)
    }
    pub fn reclaim_received(&self) {
        self.ledger.reclaim_received();
    }
}
