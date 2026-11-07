use std::io;

use zero_api::{
    ApiError, ApiErrorCode, AuthContext, CommandRequest, CommandService, EventFilter,
    FlowListQuery, QueryRequest, QueryResponse, QueryService,
};
use zero_proxy::ProxyHandle;

use super::{body::HttpBody, response::api_error_status};
use zero_api::ApiResponse;

type HttpApiError = (&'static str, HttpBody);
type EventStream = (super::sse::Subscription, zero_engine::RetainedReplay);

mod endpoint;

/// Handle GET /api/v1/events (snapshot).
pub fn events_snapshot(handle: &ProxyHandle) -> io::Result<HttpBody> {
    let events = handle
        .engine_handle()
        .inner()
        .events_snapshot(&EventFilter::default());
    let body = ApiResponse::ok(events);
    Ok(HttpBody::Events(body))
}

/// Handle POST /api/v1/commands.
pub async fn commands(
    handle: &ProxyHandle,
    body: &[u8],
    auth_ctx: &AuthContext,
) -> Result<HttpBody, (&'static str, HttpBody)> {
    let command = serde_json::from_slice::<CommandRequest>(body).map_err(|error| {
        let api_error = ApiError {
            code: ApiErrorCode::InvalidArgument,
            message: "invalid command request".to_owned(),
            field_path: None,
            cause: Some(error.to_string()),
            details: Vec::new(),
        };
        let status = api_error_status(&api_error);
        let body = HttpBody::Error(api_error);
        (status, body)
    })?;

    // Enforce permission check.
    let required = command.required_permission();
    if !auth_ctx.allows(required) {
        let error = ApiError::permission_denied(required);
        let status = api_error_status(&error);
        let body = HttpBody::Error(error);
        return Err((status, body));
    }

    match handle.execute_acknowledged(command).await {
        Ok(response) => {
            let body = ApiResponse::ok(response);
            Ok(HttpBody::Command(body))
        }
        Err(error) => {
            let status = api_error_status(&error);
            let body = HttpBody::Error(error);
            Err((status, body))
        }
    }
}

/// Handle GET /api/v1/events/stream (SSE).
///
/// Returns `(subscriber, catch_up_events)`.  `catch_up_events` contains
/// missed events when the client provides `?since=<seq>` or
/// `Last-Event-ID: <seq>`.
pub fn events_stream(
    handle: &ProxyHandle,
    query: &str,
    headers: &[(String, String)],
    budget: &std::sync::Arc<zero_runtime::configuration::ControlBudget>,
    local: &zero_runtime::configuration::ControlConnectionBudget,
) -> Result<EventStream, HttpApiError> {
    let mut filter = EventFilter::default();
    if let Some(types) = parse_query_param(query, "types") {
        filter.event_types = types.split(',').map(|t| t.trim().to_owned()).collect();
    }

    // Resolve `since` from query param or Last-Event-ID header.
    let since = parse_query_param(query, "since")
        .and_then(|v| v.parse::<u64>().ok())
        .or_else(|| {
            headers
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case("last-event-id"))
                .and_then(|(_, value)| value.parse::<u64>().ok())
        });

    let catch_up = match since {
        Some(s) => {
            let credit = budget
                .reserve(zero_runtime::configuration::ControlCharge::ResponseBytes(0))
                .map_err(replay_capacity)?;
            let result = handle
                .engine_handle()
                .inner()
                .retained_event_replay(s, 256, &filter, credit)
                .map_err(replay_capacity)?;
            if result.has_gap {
                tracing::warn!(
                    requested_since = s,
                    actual_from = result.actual_from,
                    "SSE catch-up has gap — events were evicted from ring buffer"
                );
            }
            result
        }
        None => zero_engine::RetainedReplay::empty(),
    };

    let catchup_bytes = (0..catch_up.len())
        .try_fold(0usize, |maximum, index| {
            super::sse::frame_len(catch_up.event(index)).map(|bytes| maximum.max(bytes))
        })
        .map_err(replay_capacity)?;
    let (live, wants_snapshot) = handle
        .engine_handle()
        .subscribe_control_live(filter)
        .map_err(|error| (api_error_status(&error), HttpBody::BareError(error)))?;
    let ordered_slot = if wants_snapshot || catchup_bytes != 0 {
        Some(
            local
                .reserve(zero_runtime::configuration::ControlConnectionCharge::SseFrame)
                .map_err(replay_capacity)?,
        )
    } else {
        None
    };
    let (initial, catchup_credit) = if wants_snapshot {
        let (initial, credit) = crate::ipc::owner::prepare_initial_snapshot(
            handle.engine_handle(),
            budget,
            crate::ipc::owner::EventFraming::Sse,
            catchup_bytes,
        )
        .map_err(replay_capacity)?;
        (Some(initial), credit)
    } else {
        let credit = if catchup_bytes == 0 {
            None
        } else {
            Some(
                budget
                    .reserve(
                        zero_runtime::configuration::ControlCharge::QueuedEventBytes(catchup_bytes),
                    )
                    .map_err(replay_capacity)?,
            )
        };
        (None, credit)
    };
    Ok((
        super::sse::Subscription {
            live,
            initial,
            catchup_credit,
            ordered_slot,
            _connection: None,
        },
        catch_up,
    ))
}

fn replay_capacity(error: io::Error) -> HttpApiError {
    let code = if error.kind() == io::ErrorKind::WouldBlock {
        ApiErrorCode::Conflict
    } else {
        ApiErrorCode::Internal
    };
    let error = ApiError::new(code, error.to_string());
    (api_error_status(&error), HttpBody::BareError(error))
}

// ── helpers ──────────────────────────────────────────────────────────

/// Serialize a QueryResponse into a flat HTTP response.
///
/// HTTP responses unwrap the `QueryResponse` enum so that `result`
/// contains the inner data directly (no outer variant key).
/// This keeps HTTP responses flat and predictable:
///
/// ```json
/// {"api_id":"zero.api.v1","ok":true,"result":{"engine_build_id":"0.0.9",...}}
/// ```
///
/// IPC responses preserve the externally-tagged enum for typed dispatch.
pub(super) fn serialize_query(resp: Result<QueryResponse, ApiError>) -> io::Result<HttpBody> {
    Ok(HttpBody::query(resp))
}

pub(super) fn parse_flow_list_params(query: &str) -> FlowListQuery {
    let limit = parse_query_param(query, "limit")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(100);
    let inbound_tag = parse_query_param(query, "inbound_tag").map(|v| v.to_owned());
    let principal_key = parse_query_param(query, "principal_key").map(|v| v.to_owned());

    FlowListQuery {
        limit: Some(limit),
        filter: zero_api::FlowFilter {
            inbound_tag,
            principal_key,
            network: None,
        },
    }
}

fn parse_query_param<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    let prefix = format!("{key}=");
    query.split('&').find_map(|pair| {
        if pair.starts_with(&prefix) {
            Some(pair[prefix.len()..].trim())
        } else if pair == key {
            Some("") // boolean flag
        } else {
            None
        }
    })
}

pub(super) use endpoint::{endpoint_get_request, endpoint_list_request};
