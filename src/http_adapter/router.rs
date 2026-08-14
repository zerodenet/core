use std::io;

use zero_api::{AuthContext, Permission};
use zero_proxy::ProxyHandle;

use super::handlers;
use super::response::api_error_status;
use zero_api::ApiResponse;

/// A parsed HTTP request ready for routing.
pub struct HttpRequest {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// Result of routing: either an immediate response body, or an SSE stream.
pub enum RouteResult {
    Respond(String, Vec<u8>),
    Sse {
        subscriber: zero_engine::EventSubscriber,
        catch_up: Vec<zero_api::RawApiEvent>,
    },
}

/// Route the request to the appropriate handler, enforcing permissions.
pub async fn route(
    request: &HttpRequest,
    handle: &ProxyHandle,
    auth_ctx: &AuthContext,
) -> RouteResult {
    let method = request.method.as_str();
    let path = request.path.as_str();
    let route_path = path_path_part(path);
    let query = path_query_part(path);

    // Match endpoint paths inside the /api/v1 namespace.
    let api_path = route_path.strip_prefix("/api/v1").unwrap_or(route_path);

    // Handle CORS preflight.
    if method == "OPTIONS" {
        return RouteResult::Respond("HTTP/1.1 204 No Content\r\n".to_owned(), Vec::new());
    }

    match (method, api_path) {
        // ── /api/v1/* ──────────────────────────────────────────
        (m, p) if route_path.starts_with("/api/v1/") => match (m, p) {
            ("GET", "/capabilities") => read_json(handlers::capabilities(handle), auth_ctx),
            ("GET", "/health") => read_json(handlers::health(handle), auth_ctx),
            ("GET", "/config") => read_json(handlers::config(handle), auth_ctx),
            ("GET", "/runtime") => read_json(handlers::runtime(handle), auth_ctx),
            ("GET", "/stats") => read_json(handlers::stats(handle), auth_ctx),
            ("GET", "/principal_flows") => read_json(handlers::principal_flows(handle), auth_ctx),
            ("GET", "/flows") => read_json(handlers::flows_list(handle, query), auth_ctx),
            ("POST", "/commands") => {
                command_permission(handlers::commands(handle, &request.body, auth_ctx).await)
            }
            ("GET", "/events/stream") => {
                if let Some(denied) = require_permission(auth_ctx, Permission::Read) {
                    return denied;
                }
                match handlers::events_stream(handle, query, &request.headers) {
                    Ok((subscriber, catch_up)) => RouteResult::Sse {
                        subscriber,
                        catch_up,
                    },
                    Err((status, body)) => RouteResult::Respond(status.to_owned(), body),
                }
            }
            ("GET", "/events") => read_json(handlers::events_snapshot(handle), auth_ctx),
            _ if path_segments(p) == 2 && p.starts_with("/flows/") => {
                let flow_id = &p["/flows/".len()..];
                read_json(handlers::flow_get(handle, flow_id), auth_ctx)
            }
            ("GET", "/policies") => read_json(handlers::policies_list(handle), auth_ctx),
            _ if path_segments(p) == 2 && p.starts_with("/policies/") => {
                let policy_tag = &p["/policies/".len()..];
                read_json(handlers::policy_get(handle, policy_tag), auth_ctx)
            }
            ("GET", "/sinks") => read_json(handlers::sinks(handle), auth_ctx),
            ("GET", "/tun_status") => read_json(handlers::tun_status(handle), auth_ctx),
            _ => not_found_response(),
        },

        _ if method == "GET" => not_found_response(),
        _ => method_not_allowed_response(),
    }
}

fn read_json(result: io::Result<Vec<u8>>, auth_ctx: &AuthContext) -> RouteResult {
    if let Some(denied) = require_permission(auth_ctx, Permission::Read) {
        return denied;
    }
    json_response(result)
}

fn json_response(result: io::Result<Vec<u8>>) -> RouteResult {
    match result {
        Ok(body) => RouteResult::Respond("HTTP/1.1 200 OK\r\n".to_owned(), body),
        Err(err) => {
            let body = format!(r#"{{"error":"{}"}}"#, err);
            RouteResult::Respond(
                "HTTP/1.1 500 Internal Server Error\r\n".to_owned(),
                body.into_bytes(),
            )
        }
    }
}

fn command_permission(result: Result<Vec<u8>, (&'static str, Vec<u8>)>) -> RouteResult {
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
        let body = serde_json::to_vec_pretty(&ApiResponse::<()>::from_api_error(&error))
            .unwrap_or_default();
        Some(RouteResult::Respond(status.to_owned(), body))
    }
}

fn not_found_response() -> RouteResult {
    RouteResult::Respond(
        "HTTP/1.1 404 Not Found\r\n".to_owned(),
        br#"{"error":"not found"}"#.to_vec(),
    )
}

fn method_not_allowed_response() -> RouteResult {
    RouteResult::Respond(
        "HTTP/1.1 405 Method Not Allowed\r\n".to_owned(),
        br#"{"error":"method not allowed"}"#.to_vec(),
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
