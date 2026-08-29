use std::time::Duration;

use tracing::{info, warn};
use zero_core::{Network, ProtocolType, Session};

use zero_engine::{
    CompletedSessionRecord, EngineError, FlowFailureObservation, FlowRemoteEndpoint,
};

mod rate_limit;
use rate_limit::{environmental_failure_log_decision, FailureLogDecision};

pub(crate) fn session_failure_observation(
    stage: &'static str,
    error: &EngineError,
    upstream: Option<(&str, u16)>,
) -> FlowFailureObservation {
    FlowFailureObservation {
        stage: stage.to_owned(),
        code: Some(error.code().to_owned()),
        message: error.to_string(),
        remote: upstream.map(|(host, port)| FlowRemoteEndpoint {
            host: host.to_owned(),
            port,
        }),
    }
}

pub(crate) fn log_session_accepted(
    session: &Session,
    route_action: &impl std::fmt::Debug,
    mode: &str,
) {
    info!(
        session_id = session.id,
        inbound_tag = session.inbound_tag.as_deref().unwrap_or("-"),
        protocol = protocol_name(session.protocol),
        network = network_name(session.network),
        mode = mode,
        target = ?session.target,
        port = session.port,
        route_action = ?route_action,
        "session accepted"
    );
}

pub(crate) fn log_session_finished(record: &CompletedSessionRecord, upstream: Option<(&str, u16)>) {
    let bytes_up = record.inbound_rx_bytes.max(record.outbound_tx_bytes);
    let bytes_down = record.outbound_rx_bytes.max(record.inbound_tx_bytes);
    match upstream {
        Some((server, port)) => info!(
            session_id = record.id,
            inbound_tag = record.inbound_tag.as_deref().unwrap_or("-"),
            outbound_tag = record.outbound_tag.as_deref().unwrap_or("-"),
            protocol = protocol_name(record.protocol),
            network = network_name(record.network),
            mode = record.mode.as_str(),
            target = ?record.target,
            port = record.port,
            upstream_server = server,
            upstream_port = port,
            outcome = record.outcome.kind(),
            duration_ms = record.duration_ms,
            bytes_up,
            bytes_down,
            inbound_rx_bytes = record.inbound_rx_bytes,
            inbound_tx_bytes = record.inbound_tx_bytes,
            outbound_rx_bytes = record.outbound_rx_bytes,
            outbound_tx_bytes = record.outbound_tx_bytes,
            "session finished"
        ),
        None => info!(
            session_id = record.id,
            inbound_tag = record.inbound_tag.as_deref().unwrap_or("-"),
            outbound_tag = record.outbound_tag.as_deref().unwrap_or("-"),
            protocol = protocol_name(record.protocol),
            network = network_name(record.network),
            mode = record.mode.as_str(),
            target = ?record.target,
            port = record.port,
            outcome = record.outcome.kind(),
            duration_ms = record.duration_ms,
            bytes_up,
            bytes_down,
            inbound_rx_bytes = record.inbound_rx_bytes,
            inbound_tx_bytes = record.inbound_tx_bytes,
            outbound_rx_bytes = record.outbound_rx_bytes,
            outbound_tx_bytes = record.outbound_tx_bytes,
            "session finished"
        ),
    }
}

pub(crate) fn log_session_failed(
    session: &Session,
    record: Option<&CompletedSessionRecord>,
    stage: &'static str,
    duration: Duration,
    error: &impl std::fmt::Display,
    upstream: Option<(&str, u16)>,
) {
    let suppressed_failures = match environmental_failure_log_decision(&error.to_string()) {
        Some(FailureLogDecision::Suppress) => {
            tracing::debug!(
                session_id = session.id,
                inbound_tag = session.inbound_tag.as_deref().unwrap_or("-"),
                outbound_tag = session.outbound_tag.as_deref().unwrap_or("-"),
                network = network_name(session.network),
                target = ?session.target,
                port = session.port,
                stage,
                error = %error,
                "repeated local TUN egress failure warning suppressed"
            );
            return;
        }
        Some(FailureLogDecision::Warn { suppressed }) => suppressed,
        None => 0,
    };
    let mode = record.map(|item| item.mode.as_str()).unwrap_or("-");
    let duration_ms = record
        .map(|item| item.duration_ms)
        .unwrap_or(duration.as_millis() as u64);
    let bytes_up = record
        .map(|item| item.inbound_rx_bytes.max(item.outbound_tx_bytes))
        .unwrap_or(0);
    let bytes_down = record
        .map(|item| item.outbound_rx_bytes.max(item.inbound_tx_bytes))
        .unwrap_or(0);
    let inbound_rx_bytes = record.map(|item| item.inbound_rx_bytes).unwrap_or(0);
    let inbound_tx_bytes = record.map(|item| item.inbound_tx_bytes).unwrap_or(0);
    let outbound_rx_bytes = record.map(|item| item.outbound_rx_bytes).unwrap_or(0);
    let outbound_tx_bytes = record.map(|item| item.outbound_tx_bytes).unwrap_or(0);

    match upstream {
        Some((server, port)) => warn!(
            session_id = session.id,
            inbound_tag = session.inbound_tag.as_deref().unwrap_or("-"),
            outbound_tag = session.outbound_tag.as_deref().unwrap_or("-"),
            protocol = protocol_name(session.protocol),
            network = network_name(session.network),
            mode = mode,
            target = ?session.target,
            port = session.port,
            stage = stage,
            error = %error,
            upstream_server = server,
            upstream_port = port,
            duration_ms = duration_ms,
            bytes_up = bytes_up,
            bytes_down = bytes_down,
            inbound_rx_bytes = inbound_rx_bytes,
            inbound_tx_bytes = inbound_tx_bytes,
            outbound_rx_bytes = outbound_rx_bytes,
            outbound_tx_bytes = outbound_tx_bytes,
            suppressed_failures,
            "session failed"
        ),
        None => warn!(
            session_id = session.id,
            inbound_tag = session.inbound_tag.as_deref().unwrap_or("-"),
            outbound_tag = session.outbound_tag.as_deref().unwrap_or("-"),
            protocol = protocol_name(session.protocol),
            network = network_name(session.network),
            mode = mode,
            target = ?session.target,
            port = session.port,
            stage = stage,
            error = %error,
            duration_ms = duration_ms,
            bytes_up = bytes_up,
            bytes_down = bytes_down,
            inbound_rx_bytes = inbound_rx_bytes,
            inbound_tx_bytes = inbound_tx_bytes,
            outbound_rx_bytes = outbound_rx_bytes,
            outbound_tx_bytes = outbound_tx_bytes,
            suppressed_failures,
            "session failed"
        ),
    }
}

fn protocol_name(protocol: ProtocolType) -> &'static str {
    protocol.as_str()
}

fn network_name(network: Network) -> &'static str {
    match network {
        Network::Tcp => "tcp",
        Network::Udp => "udp",
    }
}
