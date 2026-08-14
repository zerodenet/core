use std::io;

use zero_api::{
    ApiError, ApiErrorCode, AuthContext, CommandRequest, CommandService, EventFilter, EventSource,
    FlowGetQuery, FlowListQuery, QueryRequest, QueryResponse, QueryService,
};
use zero_proxy::ProxyHandle;

use super::response::api_error_status;
use zero_api::ApiResponse;

type HttpApiError = (&'static str, Vec<u8>);
type EventStream = (zero_engine::EventSubscriber, Vec<zero_api::RawApiEvent>);

/// Handle GET /api/v1/capabilities.
pub fn capabilities(handle: &ProxyHandle) -> io::Result<Vec<u8>> {
    let resp = handle.query(QueryRequest::Capabilities(Default::default()));
    serialize_query(resp)
}

/// Handle GET /api/v1/health.
pub fn health(handle: &ProxyHandle) -> io::Result<Vec<u8>> {
    let resp = handle.query(QueryRequest::Health(Default::default()));
    serialize_query(resp)
}

/// Handle GET /api/v1/config.
pub fn config(handle: &ProxyHandle) -> io::Result<Vec<u8>> {
    let resp = handle.query(QueryRequest::Config(Default::default()));
    serialize_query(resp)
}

/// Handle GET /api/v1/runtime.
pub fn runtime(handle: &ProxyHandle) -> io::Result<Vec<u8>> {
    let resp = handle.query(QueryRequest::Runtime(Default::default()));
    serialize_query(resp)
}

/// Handle GET /api/v1/stats.
pub fn stats(handle: &ProxyHandle) -> io::Result<Vec<u8>> {
    let resp = handle.query(QueryRequest::Stats(Default::default()));
    serialize_query(resp)
}

/// Handle GET /api/v1/principal_flows.
pub fn principal_flows(handle: &ProxyHandle) -> io::Result<Vec<u8>> {
    let resp = handle.query(QueryRequest::PrincipalFlows(Default::default()));
    serialize_query(resp)
}

/// Handle GET /api/v1/flows (active flows list).
pub fn flows_list(handle: &ProxyHandle, query: &str) -> io::Result<Vec<u8>> {
    let params = parse_flow_list_params(query);
    let resp = handle.query(QueryRequest::ActiveFlows(params));
    serialize_query(resp)
}

/// Handle GET /api/v1/flows/{flow_id}.
pub fn flow_get(handle: &ProxyHandle, flow_id: &str) -> io::Result<Vec<u8>> {
    let resp = handle.query(QueryRequest::Flow(FlowGetQuery {
        flow_id: flow_id.to_owned(),
    }));
    serialize_query(resp)
}

/// Handle GET /api/v1/policies.
pub fn policies_list(handle: &ProxyHandle) -> io::Result<Vec<u8>> {
    let resp = handle.query(QueryRequest::Policies(Default::default()));
    serialize_query(resp)
}

/// Handle GET /api/v1/policies/{policy_tag}.
pub fn policy_get(handle: &ProxyHandle, policy_tag: &str) -> io::Result<Vec<u8>> {
    let resp = handle.query(QueryRequest::Policy(zero_api::PolicyGetQuery {
        policy_tag: policy_tag.to_owned(),
    }));
    serialize_query(resp)
}

/// Handle GET /api/v1/sinks.
pub fn sinks(handle: &ProxyHandle) -> io::Result<Vec<u8>> {
    let resp = handle.query(QueryRequest::Sinks(Default::default()));
    serialize_query(resp)
}

/// Handle GET /api/v1/tun_status.
pub fn tun_status(handle: &ProxyHandle) -> io::Result<Vec<u8>> {
    let resp = handle.query(QueryRequest::TunStatus(Default::default()));
    serialize_query(resp)
}

/// Handle GET /api/v1/events (snapshot).
pub fn events_snapshot(handle: &ProxyHandle) -> io::Result<Vec<u8>> {
    let events = handle
        .engine_handle()
        .inner()
        .events_snapshot(&EventFilter::default());
    let body = ApiResponse::ok(events);
    serde_json::to_vec_pretty(&body).map_err(io::Error::other)
}

/// Handle POST /api/v1/commands.
pub async fn commands(
    handle: &ProxyHandle,
    body: &[u8],
    auth_ctx: &AuthContext,
) -> Result<Vec<u8>, (&'static str, Vec<u8>)> {
    let command = serde_json::from_slice::<CommandRequest>(body).map_err(|error| {
        let api_error = ApiError {
            code: ApiErrorCode::InvalidArgument,
            message: "invalid command request".to_owned(),
            field_path: None,
            cause: Some(error.to_string()),
            details: Vec::new(),
        };
        let status = api_error_status(&api_error);
        let body = serde_json::to_vec_pretty(&ApiResponse::<()>::from_api_error(&api_error))
            .unwrap_or_default();
        (status, body)
    })?;

    // Enforce permission check.
    let required = command.required_permission();
    if !auth_ctx.allows(required) {
        let error = ApiError::permission_denied(required);
        let status = api_error_status(&error);
        let body = serde_json::to_vec_pretty(&ApiResponse::<()>::from_api_error(&error))
            .unwrap_or_default();
        return Err((status, body));
    }

    match handle.execute_acknowledged(command).await {
        Ok(response) => {
            let body = ApiResponse::ok(response);
            Ok(serde_json::to_vec_pretty(&body).unwrap_or_default())
        }
        Err(error) => {
            let status = api_error_status(&error);
            let body = serde_json::to_vec_pretty(&ApiResponse::<()>::from_api_error(&error))
                .unwrap_or_default();
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
            let result = handle.since(s, 256, filter.clone()).map_err(|error| {
                let status = api_error_status(&error);
                let body = serde_json::to_vec_pretty(&error).unwrap_or_default();
                (status, body)
            })?;
            if result.has_gap {
                tracing::warn!(
                    requested_since = s,
                    actual_from = result.actual_from,
                    "SSE catch-up has gap — events were evicted from ring buffer"
                );
            }
            result.events
        }
        None => Vec::new(),
    };

    match handle.subscribe(filter) {
        Ok(subscriber) => Ok((subscriber, catch_up)),
        Err(error) => {
            let status = api_error_status(&error);
            let body = serde_json::to_vec_pretty(&error).unwrap_or_default();
            Err((status, body))
        }
    }
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
fn serialize_query(resp: Result<QueryResponse, ApiError>) -> io::Result<Vec<u8>> {
    match resp {
        Ok(result) => {
            let value = unwrap_query_response(result);
            let body = ApiResponse::ok(value);
            serde_json::to_vec_pretty(&body).map_err(io::Error::other)
        }
        Err(error) => {
            let body = ApiResponse::<()>::from_api_error(&error);
            serde_json::to_vec_pretty(&body).map_err(io::Error::other)
        }
    }
}

/// Extract the inner value from a QueryResponse as a JSON Value.
///
/// This is the HTTP-specific serialization: the variant tag is stripped
/// so consumers get the data directly without wrapping.
fn unwrap_query_response(resp: QueryResponse) -> serde_json::Value {
    match resp {
        QueryResponse::Capabilities(v) => serde_json::to_value(v),
        QueryResponse::Health(v) => serde_json::to_value(v),
        QueryResponse::Config(v) => serde_json::to_value(v),
        QueryResponse::Runtime(v) => serde_json::to_value(v),
        QueryResponse::Stats(v) => serde_json::to_value(v),
        QueryResponse::PrincipalFlows(v) => serde_json::to_value(v),
        QueryResponse::ActiveFlows(v) => serde_json::to_value(v),
        QueryResponse::RecentFlows(v) => serde_json::to_value(v),
        QueryResponse::Flow(v) => serde_json::to_value(v),
        QueryResponse::Policies(v) => serde_json::to_value(v),
        QueryResponse::Policy(v) => serde_json::to_value(v),
        QueryResponse::Diagnostics(v) => Ok(v),
        QueryResponse::Sinks(v) => serde_json::to_value(v),
        QueryResponse::TunStatus(v) => serde_json::to_value(v),
        QueryResponse::Unknown(v) => Ok(v),
    }
    .unwrap_or(serde_json::Value::Null)
}

fn parse_flow_list_params(query: &str) -> FlowListQuery {
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
