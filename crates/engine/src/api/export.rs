//! Engine export --converts internal types to `zero-api` snapshot types.
//!
//! Free conversion functions are used instead of `From` impls because the
//! snapshot types live in `zero-api` (foreign crate). This satisfies the
//! orphan rule while keeping all conversion logic in one file.

use zero_api::{
    AddressSnapshot, AuthSnapshot, CompletedFlowSnapshot, ConfigSnapshot, FlowSnapshot,
    ListenerSnapshot, ModeSnapshot, OutboundTargetSnapshot, PolicyMemberSnapshot, PolicySnapshot,
    RuntimeSnapshot, StatusSnapshot, UrlTestSelectionSnapshot,
};
use zero_config::{InboundConfig, ModeConfig, OutboundConfig};

use crate::groups::{OutboundGroupStateStore, UrlTestGroupState};
use crate::observability::SessionOutcome;
use crate::plan::{resolve_target_chains, EnginePlan, TargetId};
use crate::runtime::{Engine, EngineRuntimeSnapshot};
use crate::session::{ActiveSession, CompletedSessionRecord};

use super::view::PlanView;
use zero_core::{Address, Network, ProtocolType};

// ── Engine export methods ────────────────────────────────────────────

impl Engine {
    pub fn export_config(&self) -> ConfigSnapshot {
        let snapshot = self.runtime_snapshot();
        self.export_config_from_snapshot(&snapshot)
    }

    fn export_config_from_snapshot(&self, snapshot: &EngineRuntimeSnapshot) -> ConfigSnapshot {
        let config = snapshot.config();
        let plan = snapshot.plan();
        ConfigSnapshot {
            config_revision: snapshot.config_revision(),
            mode: mode_to_snapshot(&config.mode),
            rule_count: config.route.rules.len(),
            listeners: config.inbounds.iter().map(inbound_to_listener).collect(),
            outbounds: config.outbounds.iter().map(outbound_to_snapshot).collect(),
            outbound_groups: config
                .outbound_groups
                .iter()
                .map(|group| {
                    let group_id = plan
                        .target_id(group.tag())
                        .expect("engine plan should resolve outbound group");
                    build_policy_snapshot(plan, &self.outbound_group_state, group_id)
                })
                .collect(),
        }
    }

    pub fn export_runtime(&self) -> RuntimeSnapshot {
        let snapshot = self.runtime_snapshot();
        self.export_runtime_from_snapshot(&snapshot)
    }

    fn export_runtime_from_snapshot(&self, snapshot: &EngineRuntimeSnapshot) -> RuntimeSnapshot {
        let config = snapshot.config();
        RuntimeSnapshot {
            core_instance_id: self.core_instance_id().to_owned(),
            config_revision: snapshot.config_revision(),
            stats: self.stats_snapshot(),
            udp_upstream_idle_timeout_seconds: self.udp_upstream_idle_timeout().as_secs(),
            udp_enabled: config.runtime.udp.enabled,
            log_level: config.runtime.log.level.clone(),
            log_files: config
                .runtime
                .log
                .files
                .iter()
                .map(|f| f.path.clone())
                .collect(),
            pid: self.pid,
            config_path: self.config_path().map(|p| p.display().to_string()),
            active_sessions: self.active_sessions().iter().map(session_to_flow).collect(),
            recent_completed_sessions: self
                .completed_sessions()
                .iter()
                .map(completed_to_flow)
                .collect(),
            started_at_unix_ms: self.started_at_unix_ms(),
        }
    }

    pub fn export_status(&self) -> StatusSnapshot {
        let snapshot = self.runtime_snapshot();
        StatusSnapshot {
            config: self.export_config_from_snapshot(&snapshot),
            runtime: self.export_runtime_from_snapshot(&snapshot),
        }
    }
}

// ── Session conversions ─────────────────────────────────────────────

pub(crate) fn session_to_flow(session: &ActiveSession) -> FlowSnapshot {
    FlowSnapshot {
        record: Some(Box::new(crate::observability::active_flow_record(
            session,
            zero_api::FlowState::Active,
            unix_timestamp_ms(),
        ))),
        id: session.id,
        inbound_tag: session.inbound_tag.clone(),
        outbound_tag: session.outbound_tag.clone(),
        target: address_to_snapshot(&session.target),
        port: session.port,
        protocol: protocol_name(session.protocol).to_owned(),
        auth: session.auth.as_ref().map(auth_to_snapshot),
        network: network_name(session.network).to_owned(),
        mode: session.mode.clone(),
        started_at_unix_ms: session.started_at_unix_ms,
        last_activity_at_unix_ms: session.last_activity_at_unix_ms,
        bytes_up: session.inbound_rx_bytes.max(session.outbound_tx_bytes),
        bytes_down: session.outbound_rx_bytes.max(session.inbound_tx_bytes),
        inbound_rx_bytes: session.inbound_rx_bytes,
        inbound_tx_bytes: session.inbound_tx_bytes,
        outbound_rx_bytes: session.outbound_rx_bytes,
        outbound_tx_bytes: session.outbound_tx_bytes,
        throughput_up_bps: session.throughput_up_bps,
        throughput_down_bps: session.throughput_down_bps,
        process_id: session.process_id,
        process_name: session.process_name.clone(),
    }
}

pub(crate) fn completed_to_flow(session: &CompletedSessionRecord) -> CompletedFlowSnapshot {
    CompletedFlowSnapshot {
        record: Some(Box::new(crate::observability::completed_flow_record(
            session,
        ))),
        id: session.id,
        inbound_tag: session.inbound_tag.clone(),
        outbound_tag: session.outbound_tag.clone(),
        target: address_to_snapshot(&session.target),
        port: session.port,
        protocol: protocol_name(session.protocol).to_owned(),
        auth: session.auth.as_ref().map(auth_to_snapshot),
        network: network_name(session.network).to_owned(),
        mode: session.mode.clone(),
        started_at_unix_ms: session.started_at_unix_ms,
        last_activity_at_unix_ms: session.last_activity_at_unix_ms,
        finished_at_unix_ms: session.finished_at_unix_ms,
        duration_ms: session.duration_ms,
        bytes_up: session.inbound_rx_bytes.max(session.outbound_tx_bytes),
        bytes_down: session.outbound_rx_bytes.max(session.inbound_tx_bytes),
        inbound_rx_bytes: session.inbound_rx_bytes,
        inbound_tx_bytes: session.inbound_tx_bytes,
        outbound_rx_bytes: session.outbound_rx_bytes,
        outbound_tx_bytes: session.outbound_tx_bytes,
        process_id: session.process_id,
        process_name: session.process_name.clone(),
        outcome: outcome_name(session.outcome).to_owned(),
        close_reason: session.close_reason.clone(),
    }
}

fn unix_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub(crate) fn address_to_snapshot(address: &Address) -> AddressSnapshot {
    match address {
        Address::Domain(domain) => AddressSnapshot {
            family: "domain".to_owned(),
            value: domain.clone(),
        },
        Address::Ipv4(addr) => AddressSnapshot {
            family: "ipv4".to_owned(),
            value: format!("{}.{}.{}.{}", addr[0], addr[1], addr[2], addr[3]),
        },
        Address::Ipv6(addr) => AddressSnapshot {
            family: "ipv6".to_owned(),
            value: std::net::Ipv6Addr::from(*addr).to_string(),
        },
    }
}

pub(crate) fn auth_to_snapshot(auth: &zero_core::SessionAuth) -> AuthSnapshot {
    AuthSnapshot {
        scheme: auth.scheme.clone(),
        principal_key: auth.principal_key.clone(),
    }
}

// ── Config conversions ──────────────────────────────────────────────

fn inbound_to_listener(inbound: &InboundConfig) -> ListenerSnapshot {
    ListenerSnapshot {
        tag: inbound.tag.clone(),
        protocol: inbound.protocol.protocol_name().to_owned(),
        listen_address: inbound.listen.address.clone(),
        listen_port: inbound.listen.port,
        udp_enabled: inbound.udp.enabled,
    }
}

fn outbound_to_snapshot(outbound: &OutboundConfig) -> OutboundTargetSnapshot {
    let endpoint = outbound.protocol.endpoint();
    OutboundTargetSnapshot {
        tag: outbound.tag.clone(),
        protocol: outbound.protocol.protocol_name().to_owned(),
        server: endpoint.map(|(server, _)| server.to_owned()),
        port: endpoint.map(|(_, port)| port),
        udp_enabled: outbound.udp.enabled,
    }
}

fn mode_to_snapshot(mode: &ModeConfig) -> ModeSnapshot {
    ModeSnapshot {
        kind: mode.kind().to_owned(),
        outbound: mode.outbound().map(str::to_owned),
    }
}

// ── Policy conversion ───────────────────────────────────────────────

fn build_policy_snapshot(
    plan: &EnginePlan,
    state: &OutboundGroupStateStore,
    group_id: TargetId,
) -> PolicySnapshot {
    let view = PlanView::new(plan);
    let group = plan
        .target(group_id)
        .expect("engine plan should resolve outbound group");
    let effective_chains = view.render_target_chains(&resolve_target_chains(plan, state, group_id));

    if let Some(selector) = group.as_selector() {
        PolicySnapshot {
            tag: group.tag().to_owned(),
            kind: "selector".to_owned(),
            outbounds: view.target_tags(selector.members()),
            selected: state
                .selector_selected_target(group_id)
                .map(|selected_id| view.target_tag_owned(selected_id))
                .or_else(|| Some(view.target_tag_owned(selector.initial_member()))),
            latency_ms: None,
            last_checked_unix_ms: None,
            effective_chains,
            url_test_members: Vec::new(),
            url_test_selection: None,
        }
    } else if let Some(fallback) = group.as_fallback() {
        PolicySnapshot {
            tag: group.tag().to_owned(),
            kind: "fallback".to_owned(),
            outbounds: view.target_tags(fallback.members()),
            selected: fallback
                .members()
                .first()
                .map(|member_id| view.target_tag_owned(*member_id)),
            latency_ms: None,
            last_checked_unix_ms: None,
            effective_chains,
            url_test_members: Vec::new(),
            url_test_selection: None,
        }
    } else if let Some(urltest) = group.as_urltest() {
        let runtime = state.urltest_state(group_id);
        PolicySnapshot {
            tag: group.tag().to_owned(),
            kind: "url_test".to_owned(),
            outbounds: view.target_tags(urltest.members()),
            selected: runtime
                .as_ref()
                .map(|current| view.target_tag_owned(current.selected))
                .or_else(|| Some(view.target_tag_owned(urltest.initial_member()))),
            latency_ms: runtime.as_ref().and_then(|current| current.latency_ms),
            last_checked_unix_ms: runtime
                .as_ref()
                .and_then(|current| current.last_checked_unix_ms),
            effective_chains,
            url_test_members: urltest
                .members()
                .iter()
                .map(|member_id| build_policy_member_snapshot(plan, *member_id, runtime.as_ref()))
                .collect(),
            url_test_selection: Some(
                match runtime
                    .as_ref()
                    .and_then(|runtime| runtime.selection.as_ref())
                {
                    Some(selection) => UrlTestSelectionSnapshot {
                        previous_selected: selection
                            .previous
                            .map(|target| view.target_tag_owned(target)),
                        selected: view.target_tag_owned(selection.selected),
                        best_candidate: selection.best.map(|target| view.target_tag_owned(target)),
                        current_latency_ms: selection.current_latency_ms,
                        best_latency_ms: selection.best_latency_ms,
                        tolerance_ms: selection.tolerance_ms,
                        switched: selection.switched,
                        reason: selection.reason.as_str().to_owned(),
                    },
                    None => UrlTestSelectionSnapshot {
                        selected: view.target_tag_owned(urltest.initial_member()),
                        tolerance_ms: urltest.tolerance_ms(),
                        reason: "initial".to_owned(),
                        ..UrlTestSelectionSnapshot::default()
                    },
                },
            ),
        }
    } else if let Some(relay) = group.as_relay() {
        PolicySnapshot {
            tag: group.tag().to_owned(),
            kind: "relay".to_owned(),
            outbounds: view.target_tags(relay.chain()),
            selected: relay.chain().first().map(|id| view.target_tag_owned(*id)),
            latency_ms: None,
            last_checked_unix_ms: None,
            effective_chains,
            url_test_members: Vec::new(),
            url_test_selection: None,
        }
    } else if let Some(lb) = group.as_loadbalance() {
        PolicySnapshot {
            tag: group.tag().to_owned(),
            kind: "load_balance".to_owned(),
            outbounds: view.target_tags(lb.members()),
            selected: lb
                .members()
                .first()
                .map(|member_id| view.target_tag_owned(*member_id)),
            latency_ms: None,
            last_checked_unix_ms: None,
            effective_chains,
            url_test_members: Vec::new(),
            url_test_selection: None,
        }
    } else {
        unreachable!("outbound group export requires a group target")
    }
}

fn build_policy_member_snapshot(
    plan: &EnginePlan,
    member_id: TargetId,
    runtime: Option<&UrlTestGroupState>,
) -> PolicyMemberSnapshot {
    let view = PlanView::new(plan);
    let member_tag = view.target_tag(member_id);
    let member_state = runtime.and_then(|runtime| {
        runtime
            .members
            .iter()
            .find(|member| member.member_id == member_id)
    });

    PolicyMemberSnapshot {
        member_tag: member_tag.to_owned(),
        healthy: member_state.map(|member| member.healthy).unwrap_or(false),
        latency_ms: member_state.and_then(|member| member.latency_ms),
        last_checked_unix_ms: member_state.and_then(|member| member.last_checked_unix_ms),
        last_error: member_state.and_then(|member| member.last_error.clone()),
        effective_chains: member_state
            .map(|member| view.render_target_chains(&member.effective_chains))
            .unwrap_or_default(),
    }
}

// ── Name helpers ────────────────────────────────────────────────────

fn protocol_name(protocol: ProtocolType) -> &'static str {
    protocol.as_str()
}

fn network_name(network: Network) -> &'static str {
    match network {
        Network::Tcp => "tcp",
        Network::Udp => "udp",
    }
}

fn outcome_name(outcome: SessionOutcome) -> &'static str {
    outcome.kind()
}
