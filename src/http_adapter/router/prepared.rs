//! The production config/policy path holds guards only during synchronous encode.
use super::*;
use zero_api::{ApiResponse, QueryRequest, QueryService};
use zero_runtime::configuration::{ControlBudget, ControlCharge};
enum Selected<'a> {
    Config,
    Policies,
    Policy(&'a str),
    Typed(&'a [u8]),
    Runtime,
    Stats,
    PrincipalFlows,
    Sinks,
    TunStatus,
    Health,
    Endpoints(&'a str),
    Endpoint(&'a str, bool),
    Capabilities,
    Events,
    Flows(&'a str),
    Flow(&'a str),
}
pub(super) fn route(
    request: &HttpRequest,
    handle: &ProxyHandle,
    auth: &AuthContext,
    budget: &Arc<ControlBudget>,
) -> Option<RouteResult> {
    let path = path_path_part(&request.path);
    if !path.starts_with("/api/v1/") || request.method == "OPTIONS" {
        return None;
    }
    let path = path.strip_prefix("/api/v1").unwrap();
    let selected = match (request.method.as_str(), path) {
        ("GET", "/config") => Selected::Config,
        ("GET", "/runtime") => Selected::Runtime,
        ("GET", "/stats") => Selected::Stats,
        ("GET", "/principal_flows") => Selected::PrincipalFlows,
        ("GET", "/sinks") => Selected::Sinks,
        ("GET", "/tun_status") => Selected::TunStatus,
        ("GET", "/health") => Selected::Health,
        ("GET", "/endpoints") => Selected::Endpoints(path_query_part(&request.path)),
        ("GET", path) if path.starts_with("/endpoints/") => {
            let id = &path["/endpoints/".len()..];
            let (id, details) = id
                .strip_suffix("/details")
                .map_or((id, false), |id| (id, true));
            Selected::Endpoint(id, details)
        }
        ("GET", "/capabilities") => Selected::Capabilities,
        ("GET", "/events") => Selected::Events,
        ("GET", "/flows") => Selected::Flows(path_query_part(&request.path)),
        (_, path) if path_segments(path) == 2 && path.starts_with("/flows/") => {
            Selected::Flow(&path["/flows/".len()..])
        }
        ("GET", "/policies") => Selected::Policies,
        ("POST", "/query") => Selected::Typed(&request.body),
        (_, path) if path_segments(path) == 2 && path.starts_with("/policies/") => {
            Selected::Policy(&path["/policies/".len()..])
        }
        _ => return None,
    };
    if let Some(denied) = require_permission(auth, Permission::Read) {
        return Some(denied);
    }
    Some(match prepare(handle, selected, budget) {
        Ok(body) => read_json(Ok(body), auth),
        Err(error) => RouteResult::Reject(super::super::input::capacity(error)),
    })
}
fn prepare(
    handle: &ProxyHandle,
    selected: Selected<'_>,
    budget: &Arc<ControlBudget>,
) -> io::Result<HttpBody> {
    // This original extra parsed request/tag copy has independent credit.
    // Declare before query so its real strings disappear before refund/wake.
    let _request_storage = match &selected {
        Selected::Policy(tag)
        | Selected::Flow(tag)
        | Selected::Flows(tag)
        | Selected::Endpoints(tag)
        | Selected::Endpoint(tag, _) => {
            Some(budget.reserve(ControlCharge::RequestBytes(tag.len()))?)
        }
        Selected::Typed(body) => Some(budget.reserve(ControlCharge::RequestBytes(body.len()))?),
        _ => None,
    };
    let request = match selected {
        Selected::Events => return history::prepare(handle, budget),
        Selected::Config => QueryRequest::Config(Default::default()),
        Selected::Runtime => QueryRequest::Runtime(Default::default()),
        Selected::Stats => QueryRequest::Stats(Default::default()),
        Selected::PrincipalFlows => QueryRequest::PrincipalFlows(Default::default()),
        Selected::Sinks => QueryRequest::Sinks(Default::default()),
        Selected::TunStatus => QueryRequest::TunStatus(zero_api::TunStatusQuery),
        Selected::Health => QueryRequest::Health(Default::default()),
        Selected::Endpoints(query) => match handlers::endpoint_list_request(query) {
            Ok(request) => request,
            Err(error) => {
                return encode_failure(&error, zero_api::ApiErrorCode::InvalidArgument, budget)
            }
        },
        Selected::Endpoint(id, details) => match handlers::endpoint_get_request(id, details) {
            Ok(request) => request,
            Err(error) => {
                return encode_failure(&error, zero_api::ApiErrorCode::InvalidArgument, budget)
            }
        },
        Selected::Capabilities => QueryRequest::Capabilities(Default::default()),
        Selected::Flows(query) => {
            QueryRequest::ActiveFlows(handlers::parse_flow_list_params(query))
        }
        Selected::Flow(id) => QueryRequest::Flow(zero_api::FlowGetQuery {
            flow_id: id.to_owned(),
        }),
        Selected::Policies => QueryRequest::Policies(Default::default()),
        Selected::Policy(tag) => QueryRequest::Policy(zero_api::PolicyGetQuery {
            policy_tag: tag.to_owned(),
        }),
        Selected::Typed(body) => match serde_json::from_slice::<QueryRequest>(body) {
            Ok(query) => query,
            Err(error) => {
                return encode_failure(&error, zero_api::ApiErrorCode::InvalidArgument, budget);
            }
        },
    };
    if endpoint::handles(&request) {
        return endpoint::prepare(handle, &request, budget);
    }
    if matches!(
        &request,
        QueryRequest::TunStatus(_) | QueryRequest::Health(_) | QueryRequest::Capabilities(_)
    ) {
        return proxy::prepare(handle, &request, budget);
    }
    if matches!(
        &request,
        QueryRequest::TrafficStat(_) | QueryRequest::TrafficStats(_)
    ) {
        return traffic::prepare(handle, &request, budget);
    }
    if runtime::handles(&request) {
        return runtime::prepare(handle, &request, budget);
    }
    if !matches!(
        &request,
        QueryRequest::Config(_) | QueryRequest::Policies(_) | QueryRequest::Policy(_)
    ) {
        // This request is another real producer, parsed only once. Config and
        // policy requests never re-query a different/current generation here.
        return handlers::serialize_query(handle.query(request));
    }
    let mut storage = None;
    let bytes = {
        let snapshot = handle.engine_handle().inner().runtime_snapshot();
        let selected = snapshot
            .control_config_query(&request)
            .expect("selected Engine query");
        match selected {
            Ok(view) => crate::ipc::owner::encode_response(
                &ApiResponse::ok(view.body()),
                budget,
                &mut storage,
                true,
                false,
            ),
            Err(missing) => {
                let message = missing.message();
                crate::ipc::owner::encode_response(
                    &crate::ipc::owner::BorrowedMessageResponse::new(
                        None,
                        zero_api::ApiErrorCode::NotFound,
                        &message,
                    ),
                    budget,
                    &mut storage,
                    true,
                    false,
                )
            }
        }
    }?;
    Ok(HttpBody::Prepared(crate::ipc::owner::EncodedResponse::new(
        bytes,
        storage.take().expect("same original response storage"),
    )))
}

fn encode_failure(
    message: &impl std::fmt::Display,
    code: zero_api::ApiErrorCode,
    budget: &Arc<ControlBudget>,
) -> io::Result<HttpBody> {
    let mut storage = None;
    let bytes = crate::ipc::owner::encode_response(
        &crate::ipc::owner::BorrowedMessageResponse::new(None, code, message),
        budget,
        &mut storage,
        true,
        false,
    )?;
    Ok(HttpBody::Prepared(crate::ipc::owner::EncodedResponse::new(
        bytes,
        storage.take().expect("same actual error envelope"),
    )))
}

#[cfg(test)]
mod tests;

mod runtime;

mod traffic;

mod history;
mod proxy;

mod endpoint;
