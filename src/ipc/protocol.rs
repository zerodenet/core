use serde::{Deserialize, Serialize};

use zero_api::{ApiResponse, RawResponse};

/// A request frame sent by the client to the UDS server.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IpcRequest {
    /// Execute a query.
    Query {
        #[serde(default)]
        id: Option<serde_json::Value>,
        request: zero_api::QueryRequest,
    },
    /// Execute a command.
    Command {
        #[serde(default)]
        id: Option<serde_json::Value>,
        method: String,
        params: serde_json::Value,
    },
    /// Subscribe to events (keeps the connection open).
    Subscribe {
        #[serde(default)]
        id: Option<serde_json::Value>,
        events: Option<Vec<String>>,
    },
    /// Ping to verify the connection is alive.
    Ping {
        #[serde(default)]
        id: Option<serde_json::Value>,
    },
}

/// An event frame pushed by the server to subscribed clients.
///
/// Regular events are sent as `zero_api::ApiEvent<serde_json::Value>` JSON
/// directly (the same format as SSE).  The `Goodbye` frame is IPC-specific.
#[cfg(test)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IpcEvent {
    /// Server is shutting down; clients should reconnect.
    Goodbye { message: String },
}

// ── IPC server response helpers ─────────────────────────────────────

/// Construct a success response with a raw JSON value and optional request id.
pub fn ipc_ok(
    id: Option<serde_json::Value>,
    result: impl Serialize,
) -> ApiResponse<serde_json::Value> {
    let value = serde_json::to_value(result).unwrap_or(serde_json::Value::Null);
    ApiResponse::ok_with_id(id, value)
}

/// Construct an error response with a code, message, and optional request id.
pub fn ipc_error(
    id: Option<serde_json::Value>,
    code: impl Into<String>,
    message: impl Into<String>,
) -> ApiResponse<()> {
    ApiResponse::error_msg(code, message).with_id(id)
}

/// Construct an error response from an `ApiError`, with optional request id.
#[cfg(test)]
pub fn ipc_api_error(id: Option<serde_json::Value>, error: &zero_api::ApiError) -> ApiResponse<()> {
    ApiResponse::<()>::from_api_error(error).with_id(id)
}

/// Serialize a frame to a JSON line (with trailing newline).
pub fn serialize_frame(frame: &impl Serialize) -> Result<Vec<u8>, serde_json::Error> {
    let mut bytes = serde_json::to_vec(frame)?;
    bytes.push(b'\n');
    Ok(bytes)
}

// ── Re-export for IPC client deserialization ─────────────────────────

/// The response type returned by IPC client operations.
/// Shares the same wire format as `ApiResponse<T>`.
pub type IpcResponse = RawResponse;

#[cfg(test)]
mod tests {
    use super::*;

    // ── Helpers: ipc_ok / ipc_error / ipc_api_error ──────────────────

    #[test]
    fn ipc_ok_no_id() {
        let resp = ipc_ok(None, serde_json::json!({"status": "ok"}));
        assert!(resp.ok);
        assert_eq!(resp.id, None);
        assert!(resp.error.is_none());
        assert_eq!(resp.result, Some(serde_json::json!({"status": "ok"})));
    }

    #[test]
    fn ipc_ok_with_string_id() {
        let resp = ipc_ok(Some(serde_json::json!("req-1")), "pong");
        assert!(resp.ok);
        assert_eq!(resp.id, Some(serde_json::json!("req-1")));
        assert_eq!(resp.result, Some(serde_json::json!("pong")));
    }

    #[test]
    fn ipc_ok_with_numeric_id() {
        let resp = ipc_ok(Some(serde_json::json!(42)), serde_json::Value::Null);
        assert!(resp.ok);
        assert_eq!(resp.id, Some(serde_json::json!(42)));
        assert_eq!(resp.result, Some(serde_json::Value::Null));
    }

    #[test]
    fn ipc_error_no_id() {
        let resp = ipc_error(None, "invalid_argument", "bad input");
        assert!(!resp.ok);
        assert_eq!(resp.id, None);
        assert!(resp.result.is_none());
        let err = resp.error.as_ref().unwrap();
        assert_eq!(err.code, "invalid_argument");
        assert_eq!(err.message, "bad input");
    }

    #[test]
    fn ipc_error_with_id_echo() {
        // The server echoes the request id back even in error responses
        // so the client can correlate it.
        let resp = ipc_error(Some(serde_json::json!("req-9")), "timeout", "too slow");
        assert!(!resp.ok);
        assert_eq!(resp.id, Some(serde_json::json!("req-9")));
        let err = resp.error.as_ref().unwrap();
        assert_eq!(err.code, "timeout");
    }

    #[test]
    fn ipc_api_error_with_id() {
        let api_err = zero_api::ApiError::permission_denied(zero_api::Permission::Admin);
        let resp = ipc_api_error(Some(serde_json::json!(1)), &api_err);
        assert!(!resp.ok);
        assert_eq!(resp.id, Some(serde_json::json!(1)));
        let err = resp.error.as_ref().unwrap();
        assert_eq!(err.code, "permission_denied");
    }

    // ── serialize_frame ────────────────────────────────────────────────

    #[test]
    fn serialize_frame_adds_newline() {
        let resp = ipc_ok(Some(serde_json::json!(1)), "pong");
        let bytes = serialize_frame(&resp).unwrap();
        let text = String::from_utf8(bytes).unwrap();
        assert!(text.ends_with('\n'));
        // Should parse back as valid JSON line
        let parsed: RawResponse = serde_json::from_str(text.trim()).unwrap();
        assert!(parsed.ok);
    }

    // ── IpcRequest deser: Ping ─────────────────────────────────────────

    #[test]
    fn ping_bare() {
        let req: IpcRequest = serde_json::from_str(r#"{"type":"ping"}"#).unwrap();
        assert!(matches!(req, IpcRequest::Ping { id } if id.is_none()));
    }

    #[test]
    fn ping_type_first() {
        let req: IpcRequest = serde_json::from_str(r#"{"type":"ping","id":1}"#).unwrap();
        assert!(matches!(req, IpcRequest::Ping { id } if id == Some(serde_json::json!(1))));
    }

    #[test]
    fn ping_type_last() {
        let req: IpcRequest = serde_json::from_str(r#"{"id":1,"type":"ping"}"#).unwrap();
        assert!(matches!(req, IpcRequest::Ping { id } if id == Some(serde_json::json!(1))));
    }

    #[test]
    fn ping_type_middle() {
        let req: IpcRequest =
            serde_json::from_str(r#"{"id":"x","type":"ping","extra":1}"#).unwrap();
        assert!(matches!(req, IpcRequest::Ping { id } if id == Some(serde_json::json!("x"))));
    }

    #[test]
    fn ping_string_id_echo() {
        let req: IpcRequest = serde_json::from_str(r#"{"type":"ping","id":"my-ping-1"}"#).unwrap();
        assert!(
            matches!(req, IpcRequest::Ping { id } if id == Some(serde_json::json!("my-ping-1")))
        );
    }

    // ── IpcRequest deser: Command ──────────────────────────────────────

    #[test]
    fn command_type_first() {
        let json = r#"{"type":"command","id":"c1","method":"policies.select","params":{"policy_tag":"p","target_tag":"d"}}"#;
        let req: IpcRequest = serde_json::from_str(json).unwrap();
        match req {
            IpcRequest::Command { id, method, params } => {
                assert_eq!(id, Some(serde_json::json!("c1")));
                assert_eq!(method, "policies.select");
                assert_eq!(
                    params,
                    serde_json::json!({"policy_tag":"p","target_tag":"d"})
                );
            }
            _ => panic!("expected Command"),
        }
    }

    #[test]
    fn command_type_last() {
        let json = r#"{"id":"c1","method":"policies.select","params":{"policy_tag":"p","target_tag":"d"},"type":"command"}"#;
        let req: IpcRequest = serde_json::from_str(json).unwrap();
        assert!(matches!(req, IpcRequest::Command { .. }));
    }

    #[test]
    fn command_no_id() {
        let req: IpcRequest =
            serde_json::from_str(r#"{"type":"command","method":"probe","params":{}}"#).unwrap();
        match req {
            IpcRequest::Command { id, method, .. } => {
                assert!(id.is_none());
                assert_eq!(method, "probe");
            }
            _ => panic!("expected Command"),
        }
    }

    // ── IpcRequest deser: Subscribe ────────────────────────────────────

    #[test]
    fn subscribe_type_first() {
        let json = r#"{"type":"subscribe","id":"sub-1","events":["flow","policy"]}"#;
        let req: IpcRequest = serde_json::from_str(json).unwrap();
        match req {
            IpcRequest::Subscribe { id, events } => {
                assert_eq!(id, Some(serde_json::json!("sub-1")));
                assert_eq!(events, Some(vec!["flow".into(), "policy".into()]));
            }
            _ => panic!("expected Subscribe"),
        }
    }

    #[test]
    fn subscribe_type_last() {
        let json = r#"{"id":"sub-1","events":["flow"],"type":"subscribe"}"#;
        let req: IpcRequest = serde_json::from_str(json).unwrap();
        assert!(matches!(req, IpcRequest::Subscribe { .. }));
    }

    #[test]
    fn subscribe_no_events() {
        let req: IpcRequest = serde_json::from_str(r#"{"type":"subscribe","id":"s"}"#).unwrap();
        match req {
            IpcRequest::Subscribe { events, .. } => assert!(events.is_none()),
            _ => panic!("expected Subscribe"),
        }
    }

    // ── IpcRequest deser: Query (unit-struct types) ────────────────────
    //
    // These exercise the fixed QueryRequest::deserialize that constructs
    // unit structs directly (avoids serde_json::from_value on {}).

    #[test]
    fn query_capabilities_type_first() {
        let json = r#"{"type":"query","id":1,"request":{"capabilities":{}}}"#;
        let req: IpcRequest = serde_json::from_str(json).unwrap();
        assert!(matches!(req, IpcRequest::Query { id, .. } if id == Some(serde_json::json!(1))));
    }

    #[test]
    fn query_capabilities_type_last() {
        let json = r#"{"id":1,"request":{"capabilities":{}},"type":"query"}"#;
        let req: IpcRequest = serde_json::from_str(json).unwrap();
        assert!(matches!(req, IpcRequest::Query { id, .. } if id == Some(serde_json::json!(1))));
    }

    #[test]
    fn query_capabilities_type_middle() {
        let json = r#"{"id":"q","type":"query","request":{"capabilities":{}}}"#;
        let req: IpcRequest = serde_json::from_str(json).unwrap();
        assert!(matches!(req, IpcRequest::Query { .. }));
    }

    #[test]
    fn query_all_unit_struct_types_type_last() {
        // Every unit-struct query type works with type at the end.
        let unit_types = [
            "capabilities",
            "health",
            "config",
            "runtime",
            "stats",
            "policies",
            "diagnostics",
            "sinks",
            "tun_status",
        ];
        for key in unit_types {
            let json = format!(r#"{{"id":1,"request":{{"{}":{{}}}},"type":"query"}}"#, key);
            let req: IpcRequest = serde_json::from_str(&json)
                .unwrap_or_else(|e| panic!("failed for '{}': {}", key, e));
            assert!(
                matches!(&req, IpcRequest::Query { .. }),
                "expected Query for key '{}', got {:?}",
                key,
                req
            );
        }
    }

    #[test]
    fn query_no_id() {
        let req: IpcRequest =
            serde_json::from_str(r#"{"type":"query","request":{"capabilities":{}}}"#).unwrap();
        match req {
            IpcRequest::Query { id, .. } => assert!(id.is_none()),
            _ => panic!("expected Query"),
        }
    }

    // ── IpcRequest deser: Query (types with fields) ────────────────────

    #[test]
    fn query_flow_type_last() {
        let json = r#"{"id":1,"request":{"flow":{"flow_id":"abc"}},"type":"query"}"#;
        let req: IpcRequest = serde_json::from_str(json).unwrap();
        assert!(matches!(req, IpcRequest::Query { .. }));
    }

    #[test]
    fn query_policy_type_last() {
        let json = r#"{"id":2,"request":{"policy":{"policy_tag":"proxy"}},"type":"query"}"#;
        let req: IpcRequest = serde_json::from_str(json).unwrap();
        assert!(matches!(req, IpcRequest::Query { .. }));
    }

    // ── IpcRequest deser: error / edge cases ───────────────────────────

    #[test]
    fn bad_json_is_error() {
        // Trash input that isn't valid JSON at all.
        let err = serde_json::from_str::<IpcRequest>("not json").unwrap_err();
        assert!(err.is_syntax());
    }

    #[test]
    fn empty_object_is_error() {
        let err = serde_json::from_str::<IpcRequest>(r#"{}"#).unwrap_err();
        // internally-tagged: missing 'type' field
        assert!(err.to_string().contains("type"));
    }

    #[test]
    fn unknown_type_is_error() {
        let err = serde_json::from_str::<IpcRequest>(r#"{"type":"bogus"}"#).unwrap_err();
        assert!(err.to_string().contains("unknown variant"));
    }

    #[test]
    fn missing_type_field_is_error() {
        let err = serde_json::from_str::<IpcRequest>(r#"{"id":1}"#).unwrap_err();
        assert!(err.to_string().contains("type"));
    }

    #[test]
    fn empty_line_not_tested_here() {
        // Empty string – serde_json rejects it; connection.rs filters
        // empty/blank lines before parsing.
        assert!(serde_json::from_str::<IpcRequest>(r#""#).is_err());
        assert!(serde_json::from_str::<IpcRequest>(r#"  "#).is_err());
    }

    // ── Numeric id preservation (no coercion to float) ─────────────────

    #[test]
    fn large_integer_id_is_preserved() {
        let req: IpcRequest =
            serde_json::from_str(r#"{"type":"ping","id":9007199254740991}"#).unwrap();
        match req {
            IpcRequest::Ping { id } => {
                assert_eq!(id, Some(serde_json::json!(9007199254740991u64)));
            }
            _ => panic!("expected Ping"),
        }
    }

    // ── IpcEvent serialization ─────────────────────────────────────────

    #[test]
    fn goodbye_event_serializes() {
        let ev = IpcEvent::Goodbye {
            message: "shutting down".into(),
        };
        let json = serde_json::to_string(&ev).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["type"], "goodbye");
        assert_eq!(parsed["message"], "shutting down");
    }

    // ── RawResponse (client-side) deserialization ──────────────────────

    #[test]
    fn raw_response_success_with_id() {
        let wire = r#"{"api_id":"zero","id":42,"ok":true,"result":"pong"}"#;
        let r: RawResponse = serde_json::from_str(wire).unwrap();
        assert_eq!(r.id, Some(serde_json::json!(42)));
        assert!(r.ok);
        assert_eq!(r.result, Some(serde_json::json!("pong")));
        assert!(r.error.is_none());
    }

    #[test]
    fn raw_response_error_with_no_id() {
        let wire = r#"{"api_id":"zero.api.v1","ok":false,"error":{"code":"invalid_argument","message":"bad"}}"#;
        let r: RawResponse = serde_json::from_str(wire).unwrap();
        assert!(!r.ok);
        assert_eq!(r.id, None);
        let err = r.error.as_ref().unwrap();
        assert_eq!(err.code, "invalid_argument");
        assert_eq!(err.message, "bad");
    }

    #[test]
    fn raw_response_missing_optional_fields() {
        // Minimal valid response
        let r: RawResponse = serde_json::from_str(r#"{"ok":true}"#).unwrap();
        assert!(r.ok);
        assert_eq!(r.api_id, None);
        assert_eq!(r.id, None);
        assert_eq!(r.result, None);
        assert_eq!(r.error, None);
    }

    // ── IpcRequest round-trip (serialize → deserialize) ────────────────

    #[test]
    fn round_trip_ping() {
        let original = IpcRequest::Ping {
            id: Some(serde_json::json!("ping-1")),
        };
        let json = serde_json::to_string(&original).unwrap();
        let rt: IpcRequest = serde_json::from_str(&json).unwrap();
        match rt {
            IpcRequest::Ping { id } => assert_eq!(id, Some(serde_json::json!("ping-1"))),
            _ => panic!("round-trip type mismatch"),
        }
    }

    #[test]
    fn round_trip_command() {
        let original = IpcRequest::Command {
            id: Some(serde_json::json!(1)),
            method: "policies.select".into(),
            params: serde_json::json!({"policy_tag": "p", "target_tag": "d"}),
        };
        let json = serde_json::to_string(&original).unwrap();
        let rt: IpcRequest = serde_json::from_str(&json).unwrap();
        match rt {
            IpcRequest::Command { id, method, params } => {
                assert_eq!(id, Some(serde_json::json!(1)));
                assert_eq!(method, "policies.select");
                assert_eq!(
                    params,
                    serde_json::json!({"policy_tag": "p", "target_tag": "d"})
                );
            }
            _ => panic!("round-trip type mismatch"),
        }
    }

    #[test]
    fn round_trip_subscribe() {
        let original = IpcRequest::Subscribe {
            id: Some(serde_json::json!("sub-1")),
            events: Some(vec!["flow".into()]),
        };
        let json = serde_json::to_string(&original).unwrap();
        let rt: IpcRequest = serde_json::from_str(&json).unwrap();
        assert!(matches!(rt, IpcRequest::Subscribe { .. }));
    }

    // ── End-to-end: IpcRequest → response with id echo ─────────────────

    #[test]
    fn request_id_echoed_in_success_response() {
        // Simulate the server-side: parse request, construct response with
        // the same id.
        let req: IpcRequest = serde_json::from_str(r#"{"type":"ping","id":"client-1"}"#).unwrap();
        let id = match &req {
            IpcRequest::Ping { id } => id.clone(),
            _ => None,
        };
        let resp = ipc_ok(id, "pong");
        let wire = serde_json::to_string(&resp).unwrap();
        let parsed: RawResponse = serde_json::from_str(&wire).unwrap();
        assert_eq!(parsed.id, Some(serde_json::json!("client-1")));
        assert!(parsed.ok);
    }

    #[test]
    fn request_id_echoed_in_error_response() {
        // If the request had an id, the error response MUST echo it.
        // The "invalid request frame" case in connection.rs passes None
        // only because the frame could not be parsed at all.
        let req: IpcRequest =
            serde_json::from_str(r#"{"type":"query","id":"q-7","request":{"bogus":{}}}"#).unwrap();
        let id = match &req {
            IpcRequest::Query { id, .. } => id.clone(),
            _ => None,
        };
        let resp = ipc_error(id, "unsupported", "unknown query type");
        let wire = serde_json::to_string(&resp).unwrap();
        let parsed: RawResponse = serde_json::from_str(&wire).unwrap();
        assert_eq!(parsed.id, Some(serde_json::json!("q-7")));
        assert!(!parsed.ok);
    }
}
