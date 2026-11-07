use std::{io, sync::Arc};
mod prepared;

use zero_api::{AuthContext, Permission};
use zero_proxy::ProxyHandle;

use super::response::api_error_status;
use super::{body::HttpBody, handlers};

/// A parsed HTTP request ready for routing.
pub struct HttpRequest {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// Result of routing: either an immediate response body, or an SSE stream.
pub enum RouteResult {
    Reject(io::Error),
    Respond(String, HttpBody),
    Sse {
        subscriber: super::sse::Subscription,
        catch_up: zero_engine::RetainedReplay,
    },
}

/// Production uses the same concrete Engine projection before allocating DTOs.
pub async fn route_with_budget(
    request: &HttpRequest,
    handle: &ProxyHandle,
    auth_ctx: &AuthContext,
    budget: &Arc<zero_runtime::configuration::ControlBudget>,
    local: &zero_runtime::configuration::ControlConnectionBudget,
) -> RouteResult {
    if let Some(result) = prepared::route(request, handle, auth_ctx, budget) {
        return result;
    }
    route_inner(request, handle, auth_ctx, budget, local).await
}

/// Route the request to the appropriate handler, enforcing permissions.
#[cfg(test)]
pub async fn route(
    request: &HttpRequest,
    handle: &ProxyHandle,
    auth_ctx: &AuthContext,
) -> RouteResult {
    let budget = zero_runtime::configuration::ControlBudget::new(
        zero_runtime::configuration::ControlBudgetLimits::from_runtime(
            &zero_config::RuntimeOptionsConfig::default(),
        ),
    );
    let connection = budget
        .reserve(zero_runtime::configuration::ControlCharge::Connection)
        .unwrap();
    let local = connection.connection_budget().unwrap();
    let mut result = route_with_budget(request, handle, auth_ctx, &budget, &local).await;
    if let RouteResult::Sse { subscriber, .. } = &mut result {
        subscriber._connection = Some(connection);
    }
    result
}
async fn route_inner(
    request: &HttpRequest,
    handle: &ProxyHandle,
    auth_ctx: &AuthContext,
    budget: &Arc<zero_runtime::configuration::ControlBudget>,
    local: &zero_runtime::configuration::ControlConnectionBudget,
) -> RouteResult {
    let method = request.method.as_str();
    let path = request.path.as_str();
    let route_path = path_path_part(path);
    let query = path_query_part(path);

    // Match endpoint paths inside the /api/v1 namespace.
    let api_path = route_path.strip_prefix("/api/v1").unwrap_or(route_path);

    // Handle CORS preflight.
    if method == "OPTIONS" {
        return RouteResult::Respond("HTTP/1.1 204 No Content\r\n".to_owned(), HttpBody::Raw(b""));
    }

    match (method, api_path) {
        // ── /api/v1/* ──────────────────────────────────────────
        (m, p) if route_path.starts_with("/api/v1/") => match (m, p) {
            ("POST", "/commands") => {
                command_permission(handlers::commands(handle, &request.body, auth_ctx).await)
            }
            ("GET", "/events/stream") => {
                if let Some(denied) = require_permission(auth_ctx, Permission::Read) {
                    return denied;
                }
                match handlers::events_stream(handle, query, &request.headers, budget, local) {
                    Ok((subscriber, catch_up)) => RouteResult::Sse {
                        subscriber,
                        catch_up,
                    },
                    Err((status, body)) => RouteResult::Respond(status.to_owned(), body),
                }
            }
            _ => not_found_response(),
        },

        _ if method == "GET" => not_found_response(),
        _ => method_not_allowed_response(),
    }
}

fn read_json(result: io::Result<HttpBody>, auth_ctx: &AuthContext) -> RouteResult {
    if let Some(denied) = require_permission(auth_ctx, Permission::Read) {
        return denied;
    }
    json_response(result)
}

fn json_response(result: io::Result<HttpBody>) -> RouteResult {
    match result {
        Ok(body) => RouteResult::Respond("HTTP/1.1 200 OK\r\n".to_owned(), body),
        Err(err) => {
            let body = format!(r#"{{"error":"{}"}}"#, err);
            RouteResult::Respond(
                "HTTP/1.1 500 Internal Server Error\r\n".to_owned(),
                HttpBody::Text(body),
            )
        }
    }
}

fn command_permission(result: Result<HttpBody, (&'static str, HttpBody)>) -> RouteResult {
    match result {
        Ok(body) => RouteResult::Respond("HTTP/1.1 200 OK\r\n".to_owned(), body),
        Err((status, body)) => RouteResult::Respond(status.to_owned(), body),
    }
}

/// Return a 403 response if the auth context lacks the required permission.
fn require_permission(auth_ctx: &AuthContext, required: Permission) -> Option<RouteResult> {
    if auth_ctx.allows(required) {
        None
    } else {
        let error = zero_api::ApiError::permission_denied(required);
        let status = api_error_status(&error);
        let body = HttpBody::Error(error);
        Some(RouteResult::Respond(status.to_owned(), body))
    }
}

fn not_found_response() -> RouteResult {
    RouteResult::Respond(
        "HTTP/1.1 404 Not Found\r\n".to_owned(),
        HttpBody::Raw(br#"{"error":"not found"}"#),
    )
}

fn method_not_allowed_response() -> RouteResult {
    RouteResult::Respond(
        "HTTP/1.1 405 Method Not Allowed\r\n".to_owned(),
        HttpBody::Raw(br#"{"error":"method not allowed"}"#),
    )
}

fn path_segments(path: &str) -> usize {
    path.split('/').filter(|seg| !seg.is_empty()).count()
}

fn path_query_part(path: &str) -> &str {
    path.split_once('?').map(|(_, q)| q).unwrap_or("")
}

fn path_path_part(path: &str) -> &str {
    path.split_once('?').map(|(p, _)| p).unwrap_or(path)
}

#[cfg(all(test, feature = "wireguard"))]
mod tests;
