//! Active session registry, accounting, cancellation, and snapshots.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use zero_core::{Address, Network, ProtocolType, Session, SessionAuth};

use crate::observability::SessionOutcome;
use crate::principal::{PrincipalDeviceRegistration, PrincipalQuotaRegistration};

use super::observation::{
    FlowFailureObservation, FlowPathObservation, FlowRemoteEndpoint, FlowRouteObservation,
};
use super::traffic::TrafficSampler;
use super::CompletedSessionRecord;

#[derive(Debug, Default)]
pub struct SessionRegistry {
    inner: Mutex<HashMap<u64, Arc<ActiveSessionEntry>>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SessionTrafficDelta {
    pub(crate) bytes_up: u64,
    pub(crate) bytes_down: u64,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct SessionTrafficUpdate {
    pub(crate) delta: SessionTrafficDelta,
    pub(crate) quota_exhausted_principal: Option<String>,
}

#[derive(Debug)]
pub(crate) struct FinishedSession {
    pub(crate) record: CompletedSessionRecord,
    pub(crate) traffic_delta: SessionTrafficDelta,
}

impl SessionRegistry {
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn insert(
        &self,
        session: &Session,
        mode: &str,
        device_registration: Option<PrincipalDeviceRegistration>,
        quota_registration: Option<PrincipalQuotaRegistration>,
    ) {
        let active = Arc::new(ActiveSessionEntry::new(
            session,
            mode,
            device_registration,
            quota_registration,
        ));
        self.inner
            .lock()
            .expect("session registry lock poisoned")
            .insert(active.id, active);
    }

    pub fn update_outbound(
        &self,
        session_id: u64,
        outbound_tag: Option<&str>,
        outbound_protocol: Option<&str>,
        remote: Option<FlowRemoteEndpoint>,
        relay_chain: Vec<(String, String)>,
    ) -> Option<ActiveSession> {
        let session = self.get(session_id)?;
        session.update_outbound(outbound_tag, outbound_protocol, remote, relay_chain);
        Some(session.snapshot_and_mark_emitted())
    }

    pub fn update_route(&self, session_id: u64, route: FlowRouteObservation) {
        if let Some(session) = self.get(session_id) {
            session.update_route(route);
        }
    }

    pub fn record_upload(&self, session_id: u64, bytes: u64) -> Option<SessionTrafficUpdate> {
        let session = self.get(session_id)?;
        let quota_exhausted_principal = session.record_upload(bytes);
        Some(session.traffic_update(quota_exhausted_principal))
    }

    pub fn record_download(&self, session_id: u64, bytes: u64) -> Option<SessionTrafficUpdate> {
        let session = self.get(session_id)?;
        let quota_exhausted_principal = session.record_download(bytes);
        Some(session.traffic_update(quota_exhausted_principal))
    }

    pub fn record_inbound_rx(&self, session_id: u64, bytes: u64) -> Option<SessionTrafficUpdate> {
        let session = self.get(session_id)?;
        let quota_exhausted_principal = session.record_inbound_rx(bytes);
        Some(session.traffic_update(quota_exhausted_principal))
    }

    pub fn record_inbound_tx(&self, session_id: u64, bytes: u64) -> Option<SessionTrafficUpdate> {
        let session = self.get(session_id)?;
        let quota_exhausted_principal = session.record_inbound_tx(bytes);
        Some(session.traffic_update(quota_exhausted_principal))
    }

    pub fn record_outbound_rx(&self, session_id: u64, bytes: u64) -> Option<SessionTrafficUpdate> {
        let session = self.get(session_id)?;
        session.record_outbound_rx(bytes);
        Some(session.traffic_update(None))
    }

    pub fn record_outbound_tx(&self, session_id: u64, bytes: u64) -> Option<SessionTrafficUpdate> {
        let session = self.get(session_id)?;
        session.record_outbound_tx(bytes);
        Some(session.traffic_update(None))
    }

    pub fn finish(
        &self,
        session_id: u64,
        outcome: SessionOutcome,
        close_reason: Option<String>,
        failure: Option<FlowFailureObservation>,
    ) -> Option<FinishedSession> {
        self.inner
            .lock()
            .expect("session registry lock poisoned")
            .remove(&session_id)
            .map(|session| {
                let record = session.finish(outcome, close_reason, failure);
                let traffic_delta =
                    session.claim_traffic_delta_for(record.bytes_up, record.bytes_down);
                FinishedSession {
                    record,
                    traffic_delta,
                }
            })
    }

    pub fn snapshot(&self) -> Vec<ActiveSession> {
        let mut sessions = self
            .inner
            .lock()
            .expect("session registry lock poisoned")
            .values()
            .map(|session| session.snapshot())
            .collect::<Vec<_>>();
        sessions.sort_by_key(|session| session.id);
        sessions
    }

    pub fn snapshot_one(&self, session_id: u64) -> Option<ActiveSession> {
        self.get(session_id).map(|session| session.snapshot())
    }

    pub fn dirty_snapshot(&self) -> Vec<ActiveSession> {
        self.inner
            .lock()
            .expect("session registry lock poisoned")
            .values()
            .filter_map(|session| session.snapshot_if_dirty())
            .collect()
    }

    pub fn register_cancellation<F>(&self, session_id: u64, cancel: F) -> bool
    where
        F: FnOnce() + Send + 'static,
    {
        let Some(session) = self.get(session_id) else {
            return false;
        };
        session.register_cancellation(Box::new(cancel));
        true
    }

    pub fn cancel(&self, session_id: u64, reason: &str) -> bool {
        let Some(session) = self.get(session_id) else {
            return false;
        };
        session.cancel(reason);
        true
    }

    pub fn cancel_principal(&self, principal_key: &str, reason: &str) -> Vec<u64> {
        let sessions = self
            .inner
            .lock()
            .expect("session registry lock poisoned")
            .values()
            .filter(|session| {
                session
                    .auth
                    .as_ref()
                    .and_then(|auth| auth.principal_key.as_deref())
                    == Some(principal_key)
            })
            .cloned()
            .collect::<Vec<_>>();
        let mut session_ids = sessions
            .into_iter()
            .map(|session| {
                session.cancel(reason);
                session.id
            })
            .collect::<Vec<_>>();
        session_ids.sort_unstable();
        session_ids
    }

    pub fn is_cancelled(&self, session_id: u64) -> bool {
        self.get(session_id)
            .is_some_and(|session| session.cancelled.load(Ordering::Acquire))
    }

    pub fn cancellation_reason(&self, session_id: u64) -> Option<String> {
        self.get(session_id)
            .and_then(|session| session.cancellation_reason())
    }

    fn get(&self, session_id: u64) -> Option<Arc<ActiveSessionEntry>> {
        self.inner
            .lock()
            .expect("session registry lock poisoned")
            .get(&session_id)
            .cloned()
    }
}

#[derive(Debug)]
struct ActiveSessionEntry {
    id: u64,
    inbound_tag: Option<String>,
    outbound_tag: Mutex<Option<String>>,
    route: Mutex<Option<FlowRouteObservation>>,
    path: Mutex<FlowPathObservation>,
    target: Address,
    port: u16,
    protocol: ProtocolType,
    auth: Option<SessionAuth>,
    network: Network,
    mode: String,
    started_at: Instant,
    started_at_unix_ms: u64,
    last_activity_at_unix_ms: AtomicU64,
    inbound_rx_bytes: AtomicU64,
    inbound_tx_bytes: AtomicU64,
    outbound_rx_bytes: AtomicU64,
    outbound_tx_bytes: AtomicU64,
    accounted_bytes_up: AtomicU64,
    accounted_bytes_down: AtomicU64,
    throughput_sampler: TrafficSampler,
    revision: AtomicU64,
    last_emitted_revision: AtomicU64,
    sni: Option<String>,
    source_ip: Option<Address>,
    source_port: Option<u16>,
    process_id: Option<u32>,
    process_name: Option<String>,
    process_path: Option<String>,
    cancelled: AtomicBool,
    cancellation_reason: Mutex<Option<String>>,
    cancellation: CancellationSlot,
    _device_registration: Option<PrincipalDeviceRegistration>,
    quota_registration: Option<PrincipalQuotaRegistration>,
}

struct CancellationSlot(Mutex<Option<Box<dyn FnOnce() + Send>>>);

impl std::fmt::Debug for CancellationSlot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_tuple("CancellationSlot").finish()
    }
}

impl ActiveSessionEntry {
    fn new(
        session: &Session,
        mode: &str,
        device_registration: Option<PrincipalDeviceRegistration>,
        quota_registration: Option<PrincipalQuotaRegistration>,
    ) -> Self {
        let started_at_unix_ms = unix_timestamp_ms();

        Self {
            id: session.id,
            inbound_tag: session.inbound_tag.clone(),
            outbound_tag: Mutex::new(session.outbound_tag.clone()),
            route: Mutex::new(None),
            path: Mutex::new(FlowPathObservation::default()),
            target: session.target.clone(),
            port: session.port,
            protocol: session.protocol,
            auth: session.auth.clone(),
            network: session.network,
            mode: mode.to_owned(),
            started_at: Instant::now(),
            started_at_unix_ms,
            last_activity_at_unix_ms: AtomicU64::new(started_at_unix_ms),
            inbound_rx_bytes: AtomicU64::new(0),
            inbound_tx_bytes: AtomicU64::new(0),
            outbound_rx_bytes: AtomicU64::new(0),
            outbound_tx_bytes: AtomicU64::new(0),
            accounted_bytes_up: AtomicU64::new(0),
            accounted_bytes_down: AtomicU64::new(0),
            throughput_sampler: TrafficSampler::new(started_at_unix_ms),
            revision: AtomicU64::new(1),
            last_emitted_revision: AtomicU64::new(1),
            sni: session.sni.clone(),
            source_ip: session.source_ip.clone(),
            source_port: session.source_port,
            process_id: session.process_id,
            process_name: session.process_name.clone(),
            process_path: session.process_path.clone(),
            cancelled: AtomicBool::new(false),
            cancellation_reason: Mutex::new(None),
            cancellation: CancellationSlot(Mutex::new(None)),
            _device_registration: device_registration,
            quota_registration,
        }
    }

    fn register_cancellation(&self, cancel: Box<dyn FnOnce() + Send>) {
        if self.cancelled.load(Ordering::Acquire) {
            cancel();
            return;
        }
        let mut slot = self
            .cancellation
            .0
            .lock()
            .expect("session cancellation lock poisoned");
        if self.cancelled.load(Ordering::Acquire) {
            drop(slot);
            cancel();
        } else {
            *slot = Some(cancel);
        }
    }

    fn cancel(&self, reason: &str) {
        let mut cancellation_reason = self
            .cancellation_reason
            .lock()
            .expect("session cancellation reason lock poisoned");
        if self.cancelled.load(Ordering::Acquire) {
            return;
        }
        *cancellation_reason = Some(reason.to_owned());
        self.cancelled.store(true, Ordering::Release);
        drop(cancellation_reason);
        let cancel = self
            .cancellation
            .0
            .lock()
            .expect("session cancellation lock poisoned")
            .take();
        if let Some(cancel) = cancel {
            cancel();
        }
    }

    fn cancellation_reason(&self) -> Option<String> {
        self.cancellation_reason
            .lock()
            .expect("session cancellation reason lock poisoned")
            .clone()
    }

    fn update_outbound(
        &self,
        outbound_tag: Option<&str>,
        outbound_protocol: Option<&str>,
        remote: Option<FlowRemoteEndpoint>,
        relay_chain: Vec<(String, String)>,
    ) {
        *self
            .outbound_tag
            .lock()
            .expect("outbound tag lock poisoned") = outbound_tag.map(ToOwned::to_owned);
        {
            let mut path = self.path.lock().expect("session path lock poisoned");
            path.outbound_protocol = outbound_protocol.map(ToOwned::to_owned);
            path.remote = remote;
            path.relay_chain = relay_chain;
        }
        if let Some(outbound_tag) = outbound_tag {
            if let Some(route) = self
                .route
                .lock()
                .expect("session route lock poisoned")
                .as_mut()
            {
                if route.selection_chain.last().map(String::as_str) != Some(outbound_tag) {
                    route.selection_chain.push(outbound_tag.to_owned());
                }
            }
        }
        self.bump_revision();
    }

    fn update_route(&self, route: FlowRouteObservation) {
        *self.route.lock().expect("session route lock poisoned") = Some(route);
        self.bump_revision();
    }

    fn record_upload(&self, bytes: u64) -> Option<String> {
        let exhausted = self.record_inbound_rx(bytes);
        self.record_outbound_tx(bytes);
        exhausted
    }

    fn record_download(&self, bytes: u64) -> Option<String> {
        self.record_outbound_rx(bytes);
        self.record_inbound_tx(bytes)
    }

    fn record_inbound_rx(&self, bytes: u64) -> Option<String> {
        if bytes == 0 {
            return None;
        }

        self.inbound_rx_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.touch();
        self.quota_registration
            .as_ref()
            .and_then(|quota| quota.consume(bytes))
    }

    fn record_inbound_tx(&self, bytes: u64) -> Option<String> {
        if bytes == 0 {
            return None;
        }

        self.inbound_tx_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.touch();
        self.quota_registration
            .as_ref()
            .and_then(|quota| quota.consume(bytes))
    }

    fn record_outbound_rx(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }

        self.outbound_rx_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.touch();
    }

    fn record_outbound_tx(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }

        self.outbound_tx_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.touch();
    }

    fn traffic_update(&self, quota_exhausted_principal: Option<String>) -> SessionTrafficUpdate {
        SessionTrafficUpdate {
            delta: self.claim_traffic_delta(),
            quota_exhausted_principal,
        }
    }

    fn claim_traffic_delta(&self) -> SessionTrafficDelta {
        let bytes_up = self
            .inbound_rx_bytes
            .load(Ordering::Relaxed)
            .max(self.outbound_tx_bytes.load(Ordering::Relaxed));
        let bytes_down = self
            .outbound_rx_bytes
            .load(Ordering::Relaxed)
            .max(self.inbound_tx_bytes.load(Ordering::Relaxed));
        self.claim_traffic_delta_for(bytes_up, bytes_down)
    }

    fn claim_traffic_delta_for(&self, bytes_up: u64, bytes_down: u64) -> SessionTrafficDelta {
        let accounted_up = self
            .accounted_bytes_up
            .fetch_max(bytes_up, Ordering::Relaxed);
        let accounted_down = self
            .accounted_bytes_down
            .fetch_max(bytes_down, Ordering::Relaxed);
        SessionTrafficDelta {
            bytes_up: bytes_up.saturating_sub(accounted_up),
            bytes_down: bytes_down.saturating_sub(accounted_down),
        }
    }

    fn snapshot(&self) -> ActiveSession {
        let now_unix_ms = unix_timestamp_ms();
        let inbound_rx_bytes = self.inbound_rx_bytes.load(Ordering::Relaxed);
        let inbound_tx_bytes = self.inbound_tx_bytes.load(Ordering::Relaxed);
        let outbound_rx_bytes = self.outbound_rx_bytes.load(Ordering::Relaxed);
        let outbound_tx_bytes = self.outbound_tx_bytes.load(Ordering::Relaxed);
        let bytes_up = inbound_rx_bytes.max(outbound_tx_bytes);
        let bytes_down = outbound_rx_bytes.max(inbound_tx_bytes);
        let throughput = self.throughput_sampler.snapshot(
            now_unix_ms,
            inbound_rx_bytes.max(outbound_tx_bytes),
            outbound_rx_bytes.max(inbound_tx_bytes),
        );

        ActiveSession {
            id: self.id,
            revision: self.revision.load(Ordering::Relaxed),
            inbound_tag: self.inbound_tag.clone(),
            outbound_tag: self
                .outbound_tag
                .lock()
                .expect("outbound tag lock poisoned")
                .clone(),
            route: self
                .route
                .lock()
                .expect("session route lock poisoned")
                .clone(),
            path: self
                .path
                .lock()
                .expect("session path lock poisoned")
                .clone(),
            target: self.target.clone(),
            port: self.port,
            protocol: self.protocol,
            auth: self.auth.clone(),
            network: self.network,
            mode: self.mode.clone(),
            started_at_unix_ms: self.started_at_unix_ms,
            last_activity_at_unix_ms: self.last_activity_at_unix_ms.load(Ordering::Relaxed),
            bytes_up,
            bytes_down,
            inbound_rx_bytes,
            inbound_tx_bytes,
            outbound_rx_bytes,
            outbound_tx_bytes,
            throughput_up_bps: throughput.up_bps,
            throughput_down_bps: throughput.down_bps,
            process_id: self.process_id,
            process_name: self.process_name.clone(),
            process_path: self.process_path.clone(),
            sni: self.sni.clone(),
            source_ip: self.source_ip.clone(),
            source_port: self.source_port,
        }
    }

    fn snapshot_and_mark_emitted(&self) -> ActiveSession {
        let snapshot = self.snapshot();
        self.last_emitted_revision
            .store(snapshot.revision, Ordering::Relaxed);
        snapshot
    }

    fn snapshot_if_dirty(&self) -> Option<ActiveSession> {
        let revision = self.revision.load(Ordering::Relaxed);
        let last_emitted = self.last_emitted_revision.load(Ordering::Relaxed);
        if revision == last_emitted {
            return None;
        }
        let snapshot = self.snapshot();
        self.last_emitted_revision
            .store(snapshot.revision, Ordering::Relaxed);
        Some(snapshot)
    }

    fn finish(
        &self,
        outcome: SessionOutcome,
        close_reason: Option<String>,
        failure: Option<FlowFailureObservation>,
    ) -> CompletedSessionRecord {
        let finished_at_unix_ms = unix_timestamp_ms();
        let duration_ms = self.started_at.elapsed().as_millis() as u64;
        let inbound_rx_bytes = self.inbound_rx_bytes.load(Ordering::Relaxed);
        let inbound_tx_bytes = self.inbound_tx_bytes.load(Ordering::Relaxed);
        let outbound_rx_bytes = self.outbound_rx_bytes.load(Ordering::Relaxed);
        let outbound_tx_bytes = self.outbound_tx_bytes.load(Ordering::Relaxed);
        let bytes_up = inbound_rx_bytes.max(outbound_tx_bytes);
        let bytes_down = outbound_rx_bytes.max(inbound_tx_bytes);
        let throughput = self.throughput_sampler.snapshot(
            finished_at_unix_ms,
            inbound_rx_bytes.max(outbound_tx_bytes),
            outbound_rx_bytes.max(inbound_tx_bytes),
        );

        CompletedSessionRecord {
            id: self.id,
            revision: self.revision.load(Ordering::Relaxed).saturating_add(1),
            inbound_tag: self.inbound_tag.clone(),
            outbound_tag: self
                .outbound_tag
                .lock()
                .expect("outbound tag lock poisoned")
                .clone(),
            route: self
                .route
                .lock()
                .expect("session route lock poisoned")
                .clone(),
            path: self
                .path
                .lock()
                .expect("session path lock poisoned")
                .clone(),
            target: self.target.clone(),
            port: self.port,
            protocol: self.protocol,
            auth: self.auth.clone(),
            network: self.network,
            mode: self.mode.clone(),
            started_at_unix_ms: self.started_at_unix_ms,
            last_activity_at_unix_ms: self.last_activity_at_unix_ms.load(Ordering::Relaxed),
            finished_at_unix_ms,
            duration_ms,
            bytes_up,
            bytes_down,
            inbound_rx_bytes,
            inbound_tx_bytes,
            outbound_rx_bytes,
            outbound_tx_bytes,
            throughput_up_bps: throughput.up_bps,
            throughput_down_bps: throughput.down_bps,
            process_id: self.process_id,
            process_name: self.process_name.clone(),
            process_path: self.process_path.clone(),
            sni: self.sni.clone(),
            source_ip: self.source_ip.clone(),
            source_port: self.source_port,
            outcome,
            close_reason,
            failure,
        }
    }

    fn bump_revision(&self) {
        self.revision.fetch_add(1, Ordering::Relaxed);
    }

    fn touch(&self) {
        self.last_activity_at_unix_ms
            .store(unix_timestamp_ms(), Ordering::Relaxed);
        self.bump_revision();
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveSession {
    pub id: u64,
    pub revision: u64,
    pub inbound_tag: Option<String>,
    pub outbound_tag: Option<String>,
    pub route: Option<FlowRouteObservation>,
    pub path: FlowPathObservation,
    pub target: Address,
    pub port: u16,
    pub protocol: ProtocolType,
    pub auth: Option<SessionAuth>,
    pub network: Network,
    pub mode: String,
    pub started_at_unix_ms: u64,
    pub last_activity_at_unix_ms: u64,
    pub bytes_up: u64,
    pub bytes_down: u64,
    pub inbound_rx_bytes: u64,
    pub inbound_tx_bytes: u64,
    pub outbound_rx_bytes: u64,
    pub outbound_tx_bytes: u64,
    pub throughput_up_bps: u64,
    pub throughput_down_bps: u64,
    pub process_id: Option<u32>,
    pub process_name: Option<String>,
    pub process_path: Option<String>,
    pub sni: Option<String>,
    pub source_ip: Option<Address>,
    pub source_port: Option<u16>,
}

fn unix_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis() as u64
}
