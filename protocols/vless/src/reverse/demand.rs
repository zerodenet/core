//! Pure protocol demand and worker status. Carrier, task and original-resource
//! custody remain with the caller; this object never opens or spawns anything.
use super::{worker::WorkerStatus, MONITOR_INTERVAL};
use crate::mux::{MuxResponseBacklogPolicy, VlessInboundMuxServer};
use std::sync::{Arc, Mutex, Weak};

#[derive(Clone)]
pub struct WorkerDemand {
    ownership: Arc<DemandState>,
    pub(crate) uuid: [u8; 16],
    pub(crate) flow: Option<&'static str>,
    pub(super) backlog: MuxResponseBacklogPolicy,
}
struct DemandState {
    workers: Mutex<Vec<Weak<WorkerStatus>>>,
    next_attempt: Arc<tokio::sync::Mutex<tokio::time::Instant>>,
}
impl WorkerDemand {
    pub fn from_options(
        input: crate::transport::VlessOutboundOptionsRef<'_>,
    ) -> Result<Self, zero_core::Error> {
        let protocol = crate::outbound::PreparedVlessOutboundRequestBundle::from_config_with_transport_hints_mux_policy_and_testseed(
            input.id, input.flow, input.testseed, input.mux_concurrency,
            input.xudp_concurrency, input.mux_idle_timeout_secs,
            input.mux_response_backlog_frames, input.mux_response_backlog_bytes,
            Default::default(),
        )?;
        Ok(Self::from_protocol(&protocol))
    }
    pub(crate) fn from_protocol(
        protocol: &crate::outbound::PreparedVlessOutboundRequestBundle,
    ) -> Self {
        let (uuid, flow, _testseed, backlog) = protocol.reverse_parameters();
        Self {
            ownership: Arc::new(DemandState {
                workers: Mutex::new(Vec::new()),
                next_attempt: Arc::new(tokio::sync::Mutex::new(tokio::time::Instant::now())),
            }),
            uuid,
            flow,
            backlog,
        }
    }
    fn needs_worker(&self) -> bool {
        let mut workers = self
            .ownership
            .workers
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        workers.retain(|worker| worker.upgrade().is_some_and(|worker| worker.load().active));
        super::needs_worker(
            workers
                .iter()
                .filter_map(Weak::upgrade)
                .map(|worker| worker.load()),
        )
    }
    /// The returned attempt holds serialized admission through authentication.
    /// Cancelling a failed attempt retains the next retry's original pacing.
    pub async fn next(&self) -> WorkerAttempt {
        let mut next = self.ownership.next_attempt.clone().lock_owned().await;
        loop {
            tokio::time::sleep_until(*next).await;
            *next = tokio::time::Instant::now() + MONITOR_INTERVAL;
            if self.needs_worker() {
                break;
            }
        }
        WorkerAttempt {
            ownership: self.ownership.clone(),
            uuid: self.uuid,
            backlog: self.backlog,
            _serial: next,
        }
    }
}

pub struct WorkerAttempt {
    ownership: Arc<DemandState>,
    uuid: [u8; 16],
    backlog: MuxResponseBacklogPolicy,
    _serial: tokio::sync::OwnedMutexGuard<tokio::time::Instant>,
}
impl WorkerAttempt {
    pub(crate) fn server(&self) -> VlessInboundMuxServer {
        let status = Arc::new(WorkerStatus::default());
        let server =
            VlessInboundMuxServer::from_master_uuid_with_auth(self.uuid, None, self.backlog)
                .with_reverse(status.clone());
        self.ownership
            .workers
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(Arc::downgrade(&status));
        server
    }
    /// The supplied loan IO owns the standard role's Rvs preparation and
    /// optional Vision framing. Its real controller must be retained before
    /// polling. This source adds no second handshake or business identity;
    /// its original attempt stays with the driver through the first frame.
    pub fn into_requests(
        self,
        stream: zero_platform_tokio::contract::stream::BoxStream,
        context: &zero_platform_tokio::contract::context::HandshakeContext,
        facts: zero_platform_tokio::contract::context::InboundConnectionFacts,
    ) -> Result<
        zero_platform_tokio::contract::inbound::AcceptedRequest,
        zero_platform_tokio::contract::ChannelError,
    > {
        let uuid = self.uuid;
        let server = self.server();
        crate::mux::standard::reverse_source(stream, server, uuid, context, facts, self)
    }
}

#[cfg(test)]
#[path = "demand/tests.rs"]
mod tests;
