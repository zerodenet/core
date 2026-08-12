use std::time::Instant;

use zero_core::Session;
use zero_engine::{CompletedSessionRecord, EngineError, SessionHandle, SessionOutcome};

use crate::logging::{log_session_failed, log_session_finished, session_failure_observation};

pub(crate) fn finish_relay_success(
    handle: &mut SessionHandle,
    outcome: SessionOutcome,
    upstream_endpoint: Option<&(String, u16)>,
) -> Option<CompletedSessionRecord> {
    if let Some(record) = handle.finish(outcome) {
        log_session_finished(
            &record,
            upstream_endpoint.map(|(server, port)| (server.as_str(), *port)),
        );
        Some(record)
    } else {
        None
    }
}

pub(crate) fn finish_relay_failure(
    handle: &mut SessionHandle,
    session: &Session,
    started_at: Instant,
    error: &EngineError,
    upstream_endpoint: Option<&(String, u16)>,
) -> Option<CompletedSessionRecord> {
    let upstream = upstream_endpoint.map(|(server, port)| (server.as_str(), *port));
    let record = handle.finish_with_failure(
        "upstream_error",
        session_failure_observation("relay", error, upstream),
    );
    log_session_failed(
        session,
        record.as_ref(),
        "relay",
        started_at.elapsed(),
        error,
        upstream,
    );
    record
}

pub(crate) fn finish_relay_idle_timeout(
    handle: &mut SessionHandle,
    outcome: SessionOutcome,
    upstream_endpoint: Option<&(String, u16)>,
) -> Option<CompletedSessionRecord> {
    if let Some(record) = handle.finish_with_reason(outcome, Some("idle_timeout".to_owned())) {
        log_session_finished(
            &record,
            upstream_endpoint.map(|(server, port)| (server.as_str(), *port)),
        );
        Some(record)
    } else {
        None
    }
}

pub(crate) fn finish_blocked(handle: &mut SessionHandle) {
    let record = handle.finish(SessionOutcome::Blocked);
    if let Some(ref record) = record {
        log_session_finished(record, None);
    }
}

pub(crate) fn finish_route_or_establish_failure(
    handle: &mut SessionHandle,
    session: &Session,
    started_at: Instant,
    error: &EngineError,
) {
    let record = handle.finish_with_failure(
        "upstream_error",
        session_failure_observation("route_or_establish", error, None),
    );
    log_session_failed(
        session,
        record.as_ref(),
        "route_or_establish",
        started_at.elapsed(),
        error,
        None,
    );
}
