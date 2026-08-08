//! Engine event history, replay, subscription, and event projection.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc::SyncSender, mpsc::TrySendError, Arc, Mutex};

use serde_json::{json, Value};
use std::time::{SystemTime, UNIX_EPOCH};
use zero_api::{
    event_type, ApiEvent, AuthInfo, EndpointRef, EventFilter, EventReplay, FlowEventPayload,
    FlowFailureInfo, FlowOutcome, FlowPath, FlowRecord, FlowRecordTiming, FlowResult, FlowRoute,
    FlowSource, FlowState, FlowTarget, FlowThroughput, FlowTiming, MatchedRuleInfo,
    Network as ApiNetwork, PassiveRelayHealthChangedPayload, PassiveRelayHealthState,
    PolicyDecision, PolicyProbeCompletedPayload, PolicySelectedPayload, RawApiEvent, RouteDecision,
    TargetAddress, TrafficStats,
};
use zero_core::{Address, Network, ProtocolType, SessionAuth};

use crate::session::{ActiveSession, CompletedSessionRecord};

use super::SessionOutcome;

#[derive(Debug)]
pub struct EngineEventLog {
    capacity: AtomicUsize,
    event_epoch: u128,
    core_instance_id: String,
    config_revision: Arc<AtomicU64>,
    next_sequence: AtomicU64,
    inner: Mutex<VecDeque<RawApiEvent>>,
    subscribers: Mutex<Vec<SyncSender<RawApiEvent>>>,
}

impl Default for EngineEventLog {
    fn default() -> Self {
        Self::with_capacity(zero_config::DEFAULT_EVENT_LOG_CAPACITY)
    }
}

impl EngineEventLog {
    fn with_capacity(capacity: usize) -> Self {
        let event_epoch = rand::random();
        Self {
            capacity: AtomicUsize::new(capacity),
            event_epoch,
            core_instance_id: format!("{event_epoch:032x}"),
            config_revision: Arc::new(AtomicU64::new(1)),
            next_sequence: AtomicU64::new(1),
            inner: Mutex::new(VecDeque::with_capacity(capacity)),
            subscribers: Mutex::new(Vec::new()),
        }
    }

    pub fn shared(capacity: usize, config_revision: Arc<AtomicU64>) -> Arc<Self> {
        let mut log = Self::with_capacity(capacity);
        log.config_revision = config_revision;
        Arc::new(log)
    }

    pub(crate) fn core_instance_id(&self) -> &str {
        &self.core_instance_id
    }

    pub(crate) fn set_capacity(&self, capacity: usize) {
        let mut events = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        self.capacity.store(capacity, Ordering::Relaxed);
        while events.len() > capacity {
            events.pop_front();
        }
        events.shrink_to(capacity);
    }

    pub fn push_engine_started(&self, build_id: &str) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let payload = json!({
            "build_id": build_id,
            "started_at_unix_ms": now_ms,
        });
        let event = ApiEvent::new("engine-1", event_type::ENGINE_STARTED, now_ms, payload);
        self.push_generated(event);
    }

    pub fn push_engine_stopped(&self, reason: &str) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let payload = json!({
            "stopped_at_unix_ms": now_ms,
            "reason": reason,
        });
        let event = ApiEvent::new(
            format!("engine-stop-{}", now_ms),
            event_type::ENGINE_STOPPED,
            now_ms,
            payload,
        );
        self.push_generated(event);
    }

    pub fn push_policy_selected(
        &self,
        policy_tag: &str,
        policy_kind: &str,
        selected: &str,
        previous: Option<&str>,
    ) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let payload = PolicySelectedPayload {
            policy_tag: policy_tag.to_owned(),
            policy_kind: policy_kind.to_owned(),
            selected: selected.to_owned(),
            previous: previous.map(str::to_owned),
        };
        let payload = serde_json::to_value(payload)
            .expect("policy selected event payload should be serializable");
        let event = ApiEvent::new(
            format!("policy-select-{}-{}", policy_tag, now_ms),
            event_type::POLICY_SELECTED,
            now_ms,
            payload,
        );
        self.push_generated(event);
    }

    pub fn push_policy_probe_completed(
        &self,
        policy_tag: &str,
        payload: PolicyProbeCompletedPayload,
    ) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let config_revision = payload.config_revision;
        let payload = serde_json::to_value(payload)
            .expect("policy probe completed event payload should be serializable");
        let mut event = ApiEvent::new(
            format!("probe-{}-{}", policy_tag, now_ms),
            event_type::POLICY_PROBE_COMPLETED,
            now_ms,
            payload,
        );
        event.config_revision = Some(config_revision);
        self.push_generated(event);
    }

    pub fn push_passive_relay_health_changed(
        &self,
        policy_tag: &str,
        member_tag: &str,
        target: &Address,
        port: u16,
        state: PassiveRelayHealthState,
        quarantine_duration_ms: Option<u64>,
    ) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let payload = PassiveRelayHealthChangedPayload {
            policy_tag: policy_tag.to_owned(),
            member_tag: member_tag.to_owned(),
            target: passive_health_target(target),
            port,
            state,
            quarantine_duration_ms,
        };
        let payload = serde_json::to_value(payload)
            .expect("passive relay health payload should be serializable");
        let event = ApiEvent::new(
            format!("passive-relay-health-{policy_tag}-{member_tag}-{now_ms}"),
            event_type::POLICY_PASSIVE_RELAY_HEALTH_CHANGED,
            now_ms,
            payload,
        );
        self.push_generated(event);
    }

    pub fn push_config_changed(&self, config_revision: u64) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let payload = json!({
            "changed_at_unix_ms": now_ms,
            "config_revision": config_revision,
        });
        let event = ApiEvent::new(
            format!("config-{}", now_ms),
            event_type::CONFIG_CHANGED,
            now_ms,
            payload,
        );
        self.push_generated(event);
    }

    pub fn push_warning(&self, code: &str, message: &str) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let payload = json!({
            "code": code,
            "message": message,
        });
        let event = ApiEvent::new(
            format!("warn-{}", now_ms),
            event_type::ENGINE_WARNING,
            now_ms,
            payload,
        );
        self.push_generated(event);
    }

    pub fn push_stats_sampled(&self, stats: &zero_api::StatsSnapshot) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let payload =
            serde_json::to_value(stats).expect("stats sampled payload should be serializable");
        let event = ApiEvent::new(
            format!("stats-{}", now_ms),
            event_type::STATS_SAMPLED,
            now_ms,
            payload,
        );
        self.push_generated(event);
    }

    pub fn push_flow_updated(&self, session: &ActiveSession) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let traffic = traffic_stats_active(session);
        let payload = json!({
            "flow_id": session.id.to_string(),
            "network": match session.network {
                Network::Tcp => "tcp",
                Network::Udp => "udp",
            },
            "inbound_tag": session.inbound_tag,
            "outbound_tag": session.outbound_tag,
            "bytes_up": traffic.bytes_up,
            "bytes_down": traffic.bytes_down,
            "inbound_rx_bytes": session.inbound_rx_bytes,
            "inbound_tx_bytes": session.inbound_tx_bytes,
            "outbound_rx_bytes": session.outbound_rx_bytes,
            "outbound_tx_bytes": session.outbound_tx_bytes,
            "throughput_up_bps": session.throughput_up_bps,
            "throughput_down_bps": session.throughput_down_bps,
            "snapshot_at_unix_ms": now_ms,
            "record": active_flow_record(session, FlowState::Active, now_ms),
        });
        let event = ApiEvent::new(
            format!("{}:{}:{}", event_type::FLOW_UPDATED, session.id, now_ms),
            event_type::FLOW_UPDATED,
            now_ms,
            payload,
        );
        self.push_generated(event);
    }

    pub fn push_flow_started(&self, session: &ActiveSession) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let auth = session.auth.as_ref().map(auth_info);
        let principal_key = auth.as_ref().and_then(|a| a.principal_key.clone());

        let payload = FlowEventPayload {
            flow_id: session.id.to_string(),
            network: api_network(session.network),
            inbound: EndpointRef {
                tag: session.inbound_tag.clone().unwrap_or_default(),
                protocol: protocol_name(session.protocol).to_owned(),
            },
            auth,
            target: TargetAddress {
                host: address_host(&session.target),
                port: session.port,
            },
            route: RouteDecision {
                mode: session.mode.clone(),
                target: None,
            },
            policy: None::<PolicyDecision>,
            outbound: session.outbound_tag.as_ref().map(|tag| EndpointRef {
                tag: tag.clone(),
                protocol: "unknown".to_owned(),
            }),
            traffic: TrafficStats::default(),
            timing: FlowTiming {
                started_at_unix_ms: now_ms,
                ended_at_unix_ms: None,
                duration_ms: None,
            },
            outcome: FlowOutcome::DirectRelayed, // placeholder; overwritten at completion
            close_reason: None,
            record: Some(active_flow_record(session, FlowState::Opening, now_ms)),
        };

        let payload = serde_json::to_value(payload)
            .expect("flow started event payload should be serializable");
        let mut event = ApiEvent::new(
            format!("{}:{}:{}", event_type::FLOW_STARTED, session.id, now_ms,),
            event_type::FLOW_STARTED,
            now_ms,
            payload,
        );
        event.principal_key = principal_key;
        self.push_generated(event);
    }

    pub fn push_flow_routed(&self, session: &ActiveSession) {
        let now_ms = unix_timestamp_ms();
        let auth = session.auth.as_ref().map(auth_info);
        let principal_key = auth.as_ref().and_then(|item| item.principal_key.clone());
        let route_target = session
            .route
            .as_ref()
            .and_then(|route| route.target.clone());
        let outbound = session.outbound_tag.as_ref().map(|tag| EndpointRef {
            tag: tag.clone(),
            protocol: session
                .path
                .outbound_protocol
                .clone()
                .unwrap_or_else(|| "unknown".to_owned()),
        });
        let payload = FlowEventPayload {
            flow_id: session.id.to_string(),
            network: api_network(session.network),
            inbound: EndpointRef {
                tag: session.inbound_tag.clone().unwrap_or_default(),
                protocol: protocol_name(session.protocol).to_owned(),
            },
            auth,
            target: TargetAddress {
                host: address_host(&session.target),
                port: session.port,
            },
            route: RouteDecision {
                mode: session.mode.clone(),
                target: route_target,
            },
            policy: None::<PolicyDecision>,
            outbound,
            traffic: traffic_stats_active(session),
            timing: FlowTiming {
                started_at_unix_ms: session.started_at_unix_ms,
                ended_at_unix_ms: None,
                duration_ms: None,
            },
            outcome: FlowOutcome::DirectRelayed,
            close_reason: None,
            record: Some(active_flow_record(session, FlowState::Active, now_ms)),
        };
        let payload =
            serde_json::to_value(payload).expect("flow routed event payload should serialize");
        let mut event = ApiEvent::new(
            format!(
                "{}:{}:{}",
                event_type::FLOW_ROUTED,
                session.id,
                session.revision
            ),
            event_type::FLOW_ROUTED,
            now_ms,
            payload,
        );
        event.principal_key = principal_key;
        self.push_generated(event);
    }

    pub(crate) fn flow_snapshot_event(&self, sessions: &[ActiveSession]) -> RawApiEvent {
        let now_ms = unix_timestamp_ms();
        let watermark = self.latest_sequence();
        let records = sessions
            .iter()
            .map(|session| active_flow_record(session, FlowState::Active, now_ms))
            .collect::<Vec<_>>();
        let mut event = ApiEvent::new(
            format!("{}:{}", event_type::FLOW_SNAPSHOT, watermark),
            event_type::FLOW_SNAPSHOT,
            now_ms,
            json!({
                "watermark": watermark,
                "records": records,
            }),
        );
        event.sequence = Some(watermark);
        self.qualify_generated(event)
    }

    pub(crate) fn prepare_flow_completed(
        &self,
        record: &CompletedSessionRecord,
        outbound_protocol: impl FnOnce(&str) -> Option<&'static str>,
    ) -> RawApiEvent {
        self.qualify_generated(flow_completed_event(record, outbound_protocol))
    }

    pub(crate) fn push_prepared_generated(&self, event: RawApiEvent) {
        self.push(event);
    }

    pub fn snapshot(&self, filter: &EventFilter) -> Vec<RawApiEvent> {
        self.inner
            .lock()
            .expect("engine event log lock poisoned")
            .iter()
            .filter(|event| matches_filter(event, filter))
            .cloned()
            .collect()
    }

    /// Return events with `sequence > since` that match the filter, along with
    /// the actual first sequence number available to this replay.
    ///
    /// If `actual_from > since + 1`, some events were evicted from the ring
    /// buffer and the consumer has a gap.  Used for SSE `Last-Event-ID` / `?since=` resumption.
    pub fn events_since(&self, since: u64, limit: usize, filter: &EventFilter) -> EventReplay {
        let requested_next = since.saturating_add(1);
        let retained = self.inner.lock().expect("engine event log lock poisoned");
        let retained_from = retained
            .front()
            .and_then(|event| event.sequence)
            .unwrap_or(requested_next);
        let has_gap = retained_from > requested_next;
        let events: Vec<RawApiEvent> = retained
            .iter()
            .filter(|event| {
                event.sequence.map(|s| s > since).unwrap_or(false) && matches_filter(event, filter)
            })
            .take(limit)
            .cloned()
            .collect();

        let replay_start = if has_gap {
            retained_from
        } else {
            requested_next
        };
        let actual_from = events
            .first()
            .and_then(|event| event.sequence)
            .unwrap_or(replay_start);

        EventReplay {
            core_instance_id: self.core_instance_id.clone(),
            requested_after: since,
            actual_from,
            has_gap,
            events,
        }
    }

    /// Highest sequence number currently in the log, or 0 if empty.
    pub fn latest_sequence(&self) -> u64 {
        self.inner
            .lock()
            .expect("engine event log lock poisoned")
            .back()
            .and_then(|e| e.sequence)
            .unwrap_or(0)
    }

    pub(crate) fn subscribe(&self, subscriber: SyncSender<RawApiEvent>) {
        self.subscribers
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(subscriber);
    }

    pub(crate) fn push_external(&self, event: RawApiEvent) {
        self.push(event);
    }

    fn qualify_generated(&self, mut event: RawApiEvent) -> RawApiEvent {
        event.event_id = format!("{:032x}:{}", self.event_epoch, event.event_id);
        self.qualify_metadata(&mut event);
        event
    }

    fn qualify_metadata(&self, event: &mut RawApiEvent) {
        event.core_instance_id = Some(self.core_instance_id.clone());
        if event.config_revision.is_none() {
            event.config_revision = Some(self.config_revision.load(Ordering::Acquire));
        }
    }

    fn push_generated(&self, event: RawApiEvent) {
        self.push(self.qualify_generated(event));
    }

    fn push(&self, mut event: RawApiEvent) {
        self.qualify_metadata(&mut event);
        let sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        event.sequence = Some(sequence);

        {
            let mut events = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            events.push_back(event.clone());

            let capacity = self.capacity.load(Ordering::Relaxed);
            while events.len() > capacity {
                events.pop_front();
            }
        }

        self.subscribers
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .retain(|subscriber| match subscriber.try_send(event.clone()) {
                Ok(()) | Err(TrySendError::Full(_)) => true,
                Err(TrySendError::Disconnected(_)) => false,
            });
    }
}

fn passive_health_target(target: &Address) -> String {
    match target {
        Address::Domain(domain) => domain.clone(),
        Address::Ipv4(octets) => std::net::Ipv4Addr::from(*octets).to_string(),
        Address::Ipv6(octets) => std::net::Ipv6Addr::from(*octets).to_string(),
    }
}

fn flow_completed_event(
    record: &CompletedSessionRecord,
    outbound_protocol: impl FnOnce(&str) -> Option<&'static str>,
) -> RawApiEvent {
    let auth = record.auth.as_ref().map(|auth| AuthInfo {
        scheme: auth.scheme.clone(),
        principal_key: auth.principal_key.clone(),
        attributes: BTreeMap::new(),
    });
    let principal_key = auth.as_ref().and_then(|auth| auth.principal_key.clone());

    let payload = FlowEventPayload {
        flow_id: record.id.to_string(),
        network: api_network(record.network),
        inbound: EndpointRef {
            tag: record.inbound_tag.clone().unwrap_or_default(),
            protocol: protocol_name(record.protocol).to_owned(),
        },
        auth,
        target: TargetAddress {
            host: address_host(&record.target),
            port: record.port,
        },
        route: RouteDecision {
            mode: record.mode.clone(),
            target: None,
        },
        policy: None::<PolicyDecision>,
        outbound: record.outbound_tag.as_ref().map(|tag| EndpointRef {
            tag: tag.clone(),
            protocol: outbound_protocol(tag).unwrap_or("unknown").to_owned(),
        }),
        traffic: traffic_stats_completed(record),
        timing: FlowTiming {
            started_at_unix_ms: record.started_at_unix_ms,
            ended_at_unix_ms: Some(record.finished_at_unix_ms),
            duration_ms: Some(record.duration_ms),
        },
        outcome: api_outcome(record.outcome),
        close_reason: record.close_reason.clone(),
        record: Some(completed_flow_record(record)),
    };

    let payload =
        serde_json::to_value(payload).expect("flow completed event payload should be serializable");
    let mut event = ApiEvent::new(
        format!(
            "{}:{}:{}",
            event_type::FLOW_COMPLETED,
            record.id,
            record.finished_at_unix_ms
        ),
        event_type::FLOW_COMPLETED,
        record.finished_at_unix_ms,
        payload,
    );
    event.labels = BTreeMap::new();
    event.principal_key = principal_key;
    event
}

pub(crate) fn active_flow_record(
    session: &ActiveSession,
    state: FlowState,
    sampled_at_unix_ms: u64,
) -> FlowRecord {
    FlowRecord {
        flow_id: session.id.to_string(),
        revision: session.revision,
        state,
        network: api_network(session.network),
        inbound: EndpointRef {
            tag: session.inbound_tag.clone().unwrap_or_default(),
            protocol: protocol_name(session.protocol).to_owned(),
        },
        auth: session.auth.as_ref().map(auth_info),
        source: flow_source(
            session.source_ip.as_ref(),
            session.source_port,
            session.process_id,
            session.process_name.as_ref(),
            session.process_path.as_ref(),
        ),
        target: flow_target(
            &session.target,
            session.port,
            session.sni.as_ref(),
            &session.path,
        ),
        route: flow_route(session.route.as_ref(), &session.mode),
        path: flow_path(session.outbound_tag.as_ref(), &session.path),
        traffic: traffic_stats_active(session),
        throughput: FlowThroughput {
            upload_bps: session.throughput_up_bps,
            download_bps: session.throughput_down_bps,
            sampled_at_unix_ms,
        },
        timing: FlowRecordTiming {
            started_at_unix_ms: session.started_at_unix_ms,
            last_activity_at_unix_ms: session.last_activity_at_unix_ms,
            ended_at_unix_ms: None,
            duration_ms: None,
        },
        result: None,
    }
}

pub(crate) fn completed_flow_record(record: &CompletedSessionRecord) -> FlowRecord {
    FlowRecord {
        flow_id: record.id.to_string(),
        revision: record.revision,
        state: FlowState::Completed,
        network: api_network(record.network),
        inbound: EndpointRef {
            tag: record.inbound_tag.clone().unwrap_or_default(),
            protocol: protocol_name(record.protocol).to_owned(),
        },
        auth: record.auth.as_ref().map(auth_info),
        source: flow_source(
            record.source_ip.as_ref(),
            record.source_port,
            record.process_id,
            record.process_name.as_ref(),
            record.process_path.as_ref(),
        ),
        target: flow_target(
            &record.target,
            record.port,
            record.sni.as_ref(),
            &record.path,
        ),
        route: flow_route(record.route.as_ref(), &record.mode),
        path: flow_path(record.outbound_tag.as_ref(), &record.path),
        traffic: traffic_stats_completed(record),
        throughput: FlowThroughput {
            upload_bps: record.throughput_up_bps,
            download_bps: record.throughput_down_bps,
            sampled_at_unix_ms: record.finished_at_unix_ms,
        },
        timing: FlowRecordTiming {
            started_at_unix_ms: record.started_at_unix_ms,
            last_activity_at_unix_ms: record.last_activity_at_unix_ms,
            ended_at_unix_ms: Some(record.finished_at_unix_ms),
            duration_ms: Some(record.duration_ms),
        },
        result: Some(FlowResult {
            outcome: api_outcome(record.outcome),
            close_reason: record.close_reason.clone(),
            failure: record.failure.as_ref().map(|failure| FlowFailureInfo {
                stage: failure.stage.clone(),
                code: failure.code.clone(),
                message: failure.message.clone(),
                remote: failure.remote.as_ref().map(|remote| TargetAddress {
                    host: remote.host.clone(),
                    port: remote.port,
                }),
            }),
        }),
    }
}

fn auth_info(auth: &SessionAuth) -> AuthInfo {
    AuthInfo {
        scheme: auth.scheme.clone(),
        principal_key: auth.principal_key.clone(),
        attributes: BTreeMap::new(),
    }
}

fn flow_source(
    source_ip: Option<&Address>,
    source_port: Option<u16>,
    process_id: Option<u32>,
    process_name: Option<&String>,
    process_path: Option<&String>,
) -> Option<FlowSource> {
    if source_ip.is_none()
        && process_id.is_none()
        && process_name.is_none()
        && process_path.is_none()
    {
        return None;
    }
    Some(FlowSource {
        ip: source_ip.map(address_host).unwrap_or_default(),
        port: source_port,
        process_id,
        process_name: process_name.cloned(),
        process_path: process_path.cloned(),
    })
}

fn flow_target(
    target: &Address,
    port: u16,
    sni: Option<&String>,
    path: &crate::FlowPathObservation,
) -> FlowTarget {
    let resolved_ip = match target {
        Address::Domain(_) if path.outbound_protocol.as_deref() == Some("direct") => path
            .remote
            .as_ref()
            .filter(|remote| remote.host.parse::<std::net::IpAddr>().is_ok())
            .map(|remote| remote.host.clone()),
        Address::Domain(_) => None,
        Address::Ipv4(_) | Address::Ipv6(_) => Some(address_host(target)),
    };
    FlowTarget {
        host: address_host(target),
        port,
        resolved_ip,
        sniffed_host: sni.cloned(),
    }
}

fn flow_route(route: Option<&crate::FlowRouteObservation>, fallback_mode: &str) -> FlowRoute {
    let Some(route) = route else {
        return FlowRoute {
            mode: fallback_mode.to_owned(),
            action: "pending".to_owned(),
            ..FlowRoute::default()
        };
    };
    FlowRoute {
        mode: route.mode.clone(),
        action: route.action.clone(),
        target: route.target.clone(),
        matched_rule: route.matched_rule.as_ref().map(|matched| MatchedRuleInfo {
            index: matched.index,
            condition: matched.condition.clone(),
        }),
        selection_chain: route.selection_chain.clone(),
    }
}

fn flow_path(outbound_tag: Option<&String>, path: &crate::FlowPathObservation) -> FlowPath {
    FlowPath {
        outbound: outbound_tag.map(|tag| EndpointRef {
            tag: tag.clone(),
            protocol: path
                .outbound_protocol
                .clone()
                .unwrap_or_else(|| "unknown".to_owned()),
        }),
        remote: path.remote.as_ref().map(|remote| TargetAddress {
            host: remote.host.clone(),
            port: remote.port,
        }),
        relay_chain: path
            .relay_chain
            .iter()
            .map(|(tag, protocol)| EndpointRef {
                tag: tag.clone(),
                protocol: protocol.clone(),
            })
            .collect(),
    }
}

fn traffic_stats_active(session: &ActiveSession) -> TrafficStats {
    TrafficStats {
        bytes_up: session.inbound_rx_bytes.max(session.outbound_tx_bytes),
        bytes_down: session.outbound_rx_bytes.max(session.inbound_tx_bytes),
        inbound_rx_bytes: Some(session.inbound_rx_bytes),
        inbound_tx_bytes: Some(session.inbound_tx_bytes),
        outbound_rx_bytes: Some(session.outbound_rx_bytes),
        outbound_tx_bytes: Some(session.outbound_tx_bytes),
        packets_up: None,
        packets_down: None,
    }
}

fn traffic_stats_completed(record: &CompletedSessionRecord) -> TrafficStats {
    TrafficStats {
        bytes_up: record.inbound_rx_bytes.max(record.outbound_tx_bytes),
        bytes_down: record.outbound_rx_bytes.max(record.inbound_tx_bytes),
        inbound_rx_bytes: Some(record.inbound_rx_bytes),
        inbound_tx_bytes: Some(record.inbound_tx_bytes),
        outbound_rx_bytes: Some(record.outbound_rx_bytes),
        outbound_tx_bytes: Some(record.outbound_tx_bytes),
        packets_up: None,
        packets_down: None,
    }
}

fn unix_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn matches_filter(event: &RawApiEvent, filter: &EventFilter) -> bool {
    if !filter.event_types.is_empty()
        && !filter
            .event_types
            .iter()
            .any(|event_type| event_type == &event.event_type)
    {
        return false;
    }

    if !filter.principal_keys.is_empty() {
        let principal_key = event.principal_key.as_deref();
        if !filter
            .principal_keys
            .iter()
            .any(|expected| Some(expected.as_str()) == principal_key)
        {
            return false;
        }
    }

    if !filter.inbound_tags.is_empty() {
        let inbound_tag = payload_inbound_tag(&event.payload);
        if !filter
            .inbound_tags
            .iter()
            .any(|expected| Some(expected.as_str()) == inbound_tag)
        {
            return false;
        }
    }

    true
}

fn payload_inbound_tag(payload: &Value) -> Option<&str> {
    payload.get("inbound")?.get("tag")?.as_str()
}

fn api_network(network: Network) -> ApiNetwork {
    match network {
        Network::Tcp => ApiNetwork::Tcp,
        Network::Udp => ApiNetwork::Udp,
    }
}

fn api_outcome(outcome: SessionOutcome) -> FlowOutcome {
    match outcome {
        SessionOutcome::DirectRelayed => FlowOutcome::DirectRelayed,
        SessionOutcome::ChainedRelayed => FlowOutcome::ChainedRelayed,
        SessionOutcome::Blocked => FlowOutcome::Blocked,
        SessionOutcome::Failed => FlowOutcome::Failed,
        SessionOutcome::Cancelled => FlowOutcome::Cancelled,
    }
}

fn protocol_name(protocol: ProtocolType) -> &'static str {
    protocol.as_str()
}

fn address_host(address: &Address) -> String {
    match address {
        Address::Domain(domain) => domain.clone(),
        Address::Ipv4(addr) => format!("{}.{}.{}.{}", addr[0], addr[1], addr[2], addr[3]),
        Address::Ipv6(addr) => std::net::Ipv6Addr::from(*addr).to_string(),
    }
}
