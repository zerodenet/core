//! Zero API query, command, event, snapshot, and in-process handle boundary.

mod export;
mod handle;
mod view;

pub use handle::{EngineHandle, EventSubscriber};

use std::sync::OnceLock;

use serde_json::json;
use zero_api::{
    AdapterCapability, ApiCapabilities, ApiError, ApiErrorCode, CommandRequest, CommandResponse,
    CommandService, ConfigApplyCommand, ConfigValidateCommand, EventFilter, EventSource,
    FlowFilter, FlowSnapshot, Network as ApiNetwork, Permission, QueryRequest, QueryResponse,
    QueryService, RawApiEvent, SinkCapability,
};
use zero_config::{ModeConfig, RuntimeConfig};

use crate::error::EngineError;
use crate::runtime::Engine;

use self::export::{completed_to_flow, session_to_flow};

// ── Build features registry (injected from the top-level binary) ───

static BUILD_FEATURES: OnceLock<Vec<String>> = OnceLock::new();

/// Register compiled cargo features. Called once at startup from the
/// top-level binary crate that controls feature gating.
pub fn register_build_features(features: Vec<String>) {
    let _ = BUILD_FEATURES.set(features);
}

fn build_features() -> Vec<String> {
    BUILD_FEATURES.get().cloned().unwrap_or_default()
}

impl QueryService for Engine {
    fn query(&self, request: QueryRequest) -> zero_api::ApiResult<QueryResponse> {
        query_engine(self, request)
    }
}

impl CommandService for Engine {
    fn execute(&self, command: CommandRequest) -> zero_api::ApiResult<CommandResponse> {
        execute_engine_command(self, command)
    }
}

impl EventSource for Engine {
    type Stream = EventSubscriber;

    fn subscribe(&self, filter: EventFilter) -> zero_api::ApiResult<Self::Stream> {
        Ok(EventSubscriber::subscribe(self, filter))
    }

    fn latest(&self, limit: usize, filter: EventFilter) -> zero_api::ApiResult<Vec<RawApiEvent>> {
        let mut events = self.events_snapshot(&filter);
        events.truncate(limit);
        Ok(events)
    }

    fn since(
        &self,
        sequence: u64,
        limit: usize,
        filter: EventFilter,
    ) -> zero_api::ApiResult<zero_api::EventReplay> {
        Ok(self.events_since(sequence, limit, &filter))
    }
}

fn query_engine(engine: &Engine, request: QueryRequest) -> zero_api::ApiResult<QueryResponse> {
    match request {
        QueryRequest::Capabilities(_) => Ok(QueryResponse::Capabilities(capabilities())),
        QueryRequest::Health(_) => Ok(QueryResponse::Health(zero_api::HealthSnapshot {
            engine_build_id: env!("CARGO_PKG_VERSION").to_owned(),
            started_at_unix_ms: Some(engine.started_at_unix_ms()),
            healthy: true,
        })),
        QueryRequest::Config(_) => Ok(QueryResponse::Config(engine.export_config())),
        QueryRequest::Runtime(_) => Ok(QueryResponse::Runtime(engine.export_runtime())),
        QueryRequest::Stats(_) => Ok(QueryResponse::Stats(engine.stats_snapshot())),
        QueryRequest::ActiveFlows(query) => {
            let flows = engine
                .active_sessions()
                .iter()
                .map(session_to_flow)
                .filter(|flow| matches_flow_filter(flow, &query.filter))
                .take(query.limit.unwrap_or(usize::MAX))
                .collect();
            Ok(QueryResponse::ActiveFlows(flows))
        }
        QueryRequest::RecentFlows(query) => {
            let flows = engine
                .completed_sessions()
                .iter()
                .map(completed_to_flow)
                .filter(|flow| matches_flow_filter(flow, &query.filter))
                .take(query.limit.unwrap_or(usize::MAX))
                .collect();
            Ok(QueryResponse::RecentFlows(flows))
        }
        QueryRequest::Flow(query) => {
            let active = engine.active_sessions();
            if let Some(flow) = active
                .iter()
                .map(session_to_flow)
                .find(|flow| flow.id.to_string() == query.flow_id)
            {
                return Ok(QueryResponse::Flow(flow));
            }

            let completed = engine.completed_sessions();
            if let Some(record) = completed.iter().find(|s| s.id.to_string() == query.flow_id) {
                // Flow variant holds FlowSnapshot; for completed flows we
                // convert the common fields. The consumer can check the
                // `outcome` field on the diagnostics endpoint for completion.
                let flow = session_to_flow_from_completed(record);
                return Ok(QueryResponse::Flow(flow));
            }

            Err(ApiError::new(
                ApiErrorCode::NotFound,
                format!("flow `{}` was not found", query.flow_id),
            ))
        }
        QueryRequest::Policies(_) => {
            let config = engine.export_config();
            Ok(QueryResponse::Policies(config.outbound_groups))
        }
        QueryRequest::Policy(query) => {
            let config = engine.export_config();
            let policy = config
                .outbound_groups
                .into_iter()
                .find(|policy| policy.tag == query.policy_tag)
                .ok_or_else(|| {
                    ApiError::new(
                        ApiErrorCode::NotFound,
                        format!("policy `{}` was not found", query.policy_tag),
                    )
                })?;
            Ok(QueryResponse::Policy(policy))
        }
        QueryRequest::Diagnostics(_) => {
            let stats = engine.stats_snapshot();
            Ok(QueryResponse::Diagnostics(json!({
                "healthy": true,
                "active_sessions": stats.active_sessions,
                "completed_sessions": stats.completed_sessions,
                "failed_sessions": stats.failed_sessions,
                "udp_upstream": stats.udp_upstream,
            })))
        }
        QueryRequest::Sinks(_) => {
            let sinks = engine.sink_status_snapshot();
            Ok(QueryResponse::Sinks(sinks))
        }
        QueryRequest::TunStatus(_) => Ok(QueryResponse::TunStatus(
            zero_api::TunStatusSnapshot::default(),
        )),
        QueryRequest::Unknown(_) => Err(ApiError::new(
            ApiErrorCode::Unsupported,
            "unknown query type",
        )),
    }
}

/// Convert a completed session to a FlowSnapshot for the `Flow` query variant.
/// The completed-specific fields (outcome, duration) are lost here; consumers
/// should use the RecentFlows query for full completed-session details.
fn session_to_flow_from_completed(record: &crate::session::CompletedSessionRecord) -> FlowSnapshot {
    use self::export::address_to_snapshot;
    use self::export::auth_to_snapshot;
    FlowSnapshot {
        id: record.id,
        inbound_tag: record.inbound_tag.clone(),
        outbound_tag: record.outbound_tag.clone(),
        target: address_to_snapshot(&record.target),
        port: record.port,
        protocol: protocol_name(record.protocol).to_owned(),
        auth: record.auth.as_ref().map(auth_to_snapshot),
        network: network_name(record.network).to_owned(),
        mode: record.mode.clone(),
        started_at_unix_ms: record.started_at_unix_ms,
        last_activity_at_unix_ms: record.last_activity_at_unix_ms,
        bytes_up: record.inbound_rx_bytes.max(record.outbound_tx_bytes),
        bytes_down: record.outbound_rx_bytes.max(record.inbound_tx_bytes),
        inbound_rx_bytes: record.inbound_rx_bytes,
        inbound_tx_bytes: record.inbound_tx_bytes,
        outbound_rx_bytes: record.outbound_rx_bytes,
        outbound_tx_bytes: record.outbound_tx_bytes,
        throughput_up_bps: 0,
        throughput_down_bps: 0,
        process_id: record.process_id,
        process_name: record.process_name.clone(),
    }
}

// ── Command dispatch ────────────────────────────────────────────────

fn execute_engine_command(
    engine: &Engine,
    command: CommandRequest,
) -> zero_api::ApiResult<CommandResponse> {
    match command {
        CommandRequest::ConfigValidate(command) => validate_config_command(engine, command),
        CommandRequest::ConfigApply(command) => apply_config_command(engine, command),
        CommandRequest::ConfigApplyRuntime(command) => {
            apply_runtime_config_command(engine, command)
        }
        CommandRequest::PolicySelect(command) => {
            engine
                .set_selector_target(&command.policy_tag, &command.target_tag)
                .map_err(engine_error_to_api)?;
            Ok(CommandResponse {
                accepted: true,
                result: Some(json!({
                    "policy_tag": command.policy_tag,
                    "selected": command.target_tag,
                })),
            })
        }
        CommandRequest::FlowClose(command) => match engine.close_flow(&command.flow_id) {
            Ok(()) => Ok(CommandResponse {
                accepted: true,
                result: Some(json!({
                    "flow_id": command.flow_id,
                    "closed": true,
                })),
            }),
            Err(error) => Err(engine_error_to_api(error)),
        },
        CommandRequest::PolicyProbe(command) => {
            match engine.trigger_urltest_probe(&command.policy_tag) {
                Ok(()) => Ok(CommandResponse {
                    accepted: true,
                    result: Some(json!({
                        "policy_tag": command.policy_tag,
                        "probe_triggered": true,
                    })),
                }),
                Err(error) => Err(engine_error_to_api(error)),
            }
        }
        CommandRequest::DiagnosticsTraceRoute(cmd) => {
            let protocol = cmd.protocol.as_deref().unwrap_or("tcp");
            match engine.trace_route(&cmd.target, cmd.port, protocol, cmd.inbound_tag.as_deref()) {
                Ok(result) => Ok(CommandResponse {
                    accepted: true,
                    result: Some(result),
                }),
                Err(error) => Err(engine_error_to_api(error)),
            }
        }
        CommandRequest::ModeSet(cmd) => {
            let mode = match cmd.mode.as_str() {
                "rule" => ModeConfig::Rule,
                "direct" => ModeConfig::Direct,
                "global" => {
                    let outbound = cmd.outbound.ok_or_else(|| {
                        ApiError::new(
                            ApiErrorCode::InvalidArgument,
                            "mode `global` requires `outbound` field",
                        )
                    })?;
                    ModeConfig::Global { outbound }
                }
                other => {
                    return Err(ApiError::new(
                        ApiErrorCode::InvalidArgument,
                        format!("unknown mode `{other}`; expected `rule`, `direct`, or `global`"),
                    ));
                }
            };
            engine.set_mode(mode);
            Ok(CommandResponse::accepted())
        }
        CommandRequest::TunStart(_) | CommandRequest::TunStop(_) => Err(ApiError::new(
            ApiErrorCode::Internal,
            "TUN commands are handled by the proxy runtime, not the engine",
        )),
        CommandRequest::DiagnosticsProbeTarget(_) => Err(ApiError::new(
            ApiErrorCode::Internal,
            "probe_target is handled by the proxy runtime, not the engine",
        )),
        CommandRequest::DiagnosticsProbeOutbound(_) => Err(ApiError::new(
            ApiErrorCode::Internal,
            "probe_outbound is handled by the proxy runtime, not the engine",
        )),
        CommandRequest::DiagnosticsDnsLookup(_) => Err(ApiError::new(
            ApiErrorCode::Internal,
            "dns_lookup is handled by the proxy runtime, not the engine",
        )),
        CommandRequest::DiagnosticsDnsCache(_) => Err(ApiError::new(
            ApiErrorCode::Internal,
            "dns_cache is handled by the proxy runtime, not the engine",
        )),
        CommandRequest::DiagnosticsFakeipLookup(_) => Err(ApiError::new(
            ApiErrorCode::Internal,
            "fakeip_lookup is handled by the proxy runtime, not the engine",
        )),
    }
}

fn apply_config_command(
    engine: &Engine,
    command: ConfigApplyCommand,
) -> zero_api::ApiResult<CommandResponse> {
    let new_config = parse_config_apply(engine, command)?;
    engine
        .reload_config(new_config)
        .map_err(engine_error_to_api)?;

    Ok(CommandResponse {
        accepted: true,
        result: Some(json!({ "applied": true })),
    })
}

fn apply_runtime_config_command(
    engine: &Engine,
    command: ConfigApplyCommand,
) -> zero_api::ApiResult<CommandResponse> {
    let new_config = parse_config_apply(engine, command)?;
    engine
        .reload_runtime_config(new_config)
        .map_err(engine_error_to_api)?;

    Ok(CommandResponse {
        accepted: true,
        result: Some(json!({ "applied": true, "persistence": "runtime_only" })),
    })
}

fn parse_config_apply(
    engine: &Engine,
    command: ConfigApplyCommand,
) -> zero_api::ApiResult<RuntimeConfig> {
    let raw = serde_json::to_string(&command.config).map_err(to_internal_error)?;
    RuntimeConfig::parse_with_source_dir(&raw, engine.config().source_dir.clone())
        .map_err(config_error_to_api)
}

fn validate_config_command(
    engine: &Engine,
    command: ConfigValidateCommand,
) -> zero_api::ApiResult<CommandResponse> {
    let raw = serde_json::to_string(&command.config).map_err(to_internal_error)?;
    let _config = RuntimeConfig::parse_with_source_dir(&raw, engine.config().source_dir.clone())
        .map_err(config_error_to_api)?;

    Ok(CommandResponse {
        accepted: true,
        result: Some(json!({ "valid": true })),
    })
}

fn capabilities() -> ApiCapabilities {
    let mut capabilities = ApiCapabilities::new();
    let build_features = build_features();
    capabilities.adapters = vec![AdapterCapability {
        kind: "in_process".to_owned(),
        enabled: true,
    }];
    capabilities.sinks = vec![SinkCapability {
        kind: "none".to_owned(),
        enabled: false,
    }];
    capabilities.features = vec![
        "query".to_owned(),
        "config_snapshot".to_owned(),
        "runtime_snapshot".to_owned(),
        "flow_snapshot".to_owned(),
        "policy_snapshot".to_owned(),
    ];
    capabilities.build_features = build_features;
    capabilities.permissions = vec![Permission::Read];
    capabilities
}

// ── Flow filter ─────────────────────────────────────────────────────

trait FlowFilterView {
    fn inbound_tag(&self) -> Option<&str>;
    fn network(&self) -> &str;
    fn principal_key(&self) -> Option<&str>;
}

impl FlowFilterView for FlowSnapshot {
    fn inbound_tag(&self) -> Option<&str> {
        self.inbound_tag.as_deref()
    }

    fn network(&self) -> &str {
        &self.network
    }

    fn principal_key(&self) -> Option<&str> {
        self.auth
            .as_ref()
            .and_then(|auth| auth.principal_key.as_deref())
    }
}

impl FlowFilterView for zero_api::CompletedFlowSnapshot {
    fn inbound_tag(&self) -> Option<&str> {
        self.inbound_tag.as_deref()
    }

    fn network(&self) -> &str {
        &self.network
    }

    fn principal_key(&self) -> Option<&str> {
        self.auth
            .as_ref()
            .and_then(|auth| auth.principal_key.as_deref())
    }
}

fn matches_flow_filter(flow: &impl FlowFilterView, filter: &FlowFilter) -> bool {
    if let Some(expected_inbound) = &filter.inbound_tag {
        if flow.inbound_tag() != Some(expected_inbound.as_str()) {
            return false;
        }
    }

    if let Some(expected_principal) = &filter.principal_key {
        if flow.principal_key() != Some(expected_principal.as_str()) {
            return false;
        }
    }

    if let Some(expected_network) = filter.network {
        if flow.network() != api_network_name(expected_network) {
            return false;
        }
    }

    true
}

fn api_network_name(network: ApiNetwork) -> &'static str {
    match network {
        ApiNetwork::Tcp => "tcp",
        ApiNetwork::Udp => "udp",
    }
}

// ── Name helpers (shared with export) ────────────────────────────────

fn protocol_name(protocol: zero_core::ProtocolType) -> &'static str {
    protocol.as_str()
}

fn network_name(network: zero_core::Network) -> &'static str {
    match network {
        zero_core::Network::Tcp => "tcp",
        zero_core::Network::Udp => "udp",
    }
}

// ── Error conversions ───────────────────────────────────────────────

fn to_internal_error(error: serde_json::Error) -> ApiError {
    ApiError {
        code: ApiErrorCode::Internal,
        message: "failed to serialize engine query snapshot".to_owned(),
        field_path: None,
        cause: Some(error.to_string()),
        details: Vec::new(),
    }
}

fn config_error_to_api(error: zero_config::ConfigError) -> ApiError {
    use zero_config::ConfigError;
    // Map each ConfigError variant to a structured, field-level diagnostic
    // so GUIs can highlight the offending config field. The variant's
    // scope/path/tag data provides the `field_path`; the message is the
    // human-readable explanation.
    let detail = |field_path: Option<&str>, message: String| {
        Some(zero_api::ErrorDetail::new(field_path, message))
    };
    let field_detail = match &error {
        ConfigError::EmptyTag { scope } => detail(Some(scope), format!("`{scope}` tag must not be empty")),
        ConfigError::DuplicateTag { scope, tag } => detail(Some(scope), format!("duplicate `{scope}` tag `{tag}`")),
        ConfigError::DuplicateInboundListen { address, port } => detail(Some("inbounds"), format!("duplicate listen endpoint `{address}:{port}`; use an inbound multiplexor for multi-protocol same-port listening")),
        ConfigError::UndefinedRouteTargetTag { tag } => detail(Some("route"), format!("route or mode references undefined target tag `{tag}`")),
        ConfigError::UndefinedRuleSetTag { tag } => detail(Some("route.rules"), format!("route references undefined rule set tag `{tag}`")),
        ConfigError::DuplicateRouteTargetTag { tag } => detail(Some("route"), format!("duplicate route target tag `{tag}` across outbounds and outbound groups")),
        ConfigError::ParseConfig(serde_err) => {
            let line = serde_err.line();
            let col = serde_err.column();
            let loc = if line > 0 || col > 0 {
                format!(" (line {line}, column {col})")
            } else {
                String::new()
            };
            detail(Some("config"), format!("failed to parse config{loc}: {serde_err}"))
        }
        ConfigError::ReadConfig { path, .. } => detail(Some("config"), format!("failed to read config `{path}`")),
        ConfigError::ReadRuleSet { path, .. } => detail(Some("rule_sets"), format!("failed to read rule set `{path}`")),
        // The `Invalid*` string variants already embed a scope prefix like
        // `inbounds[0] \`tag\`: ...`; use the prefix (up to the first space
        // or backtick) as the field-path hint.
        ConfigError::InvalidRuleCondition(msg)
        | ConfigError::InvalidInbound(msg)
        | ConfigError::InvalidOutbound(msg)
        | ConfigError::InvalidRuleSet(msg)
        | ConfigError::InvalidRouteAction(msg)
        | ConfigError::InvalidOutboundGroup(msg)
        | ConfigError::InvalidRuntime(msg)
        | ConfigError::InvalidApi(msg)
        | ConfigError::InvalidMode(msg)
        | ConfigError::InvalidDns(msg) => {
            let field = invalid_config_field_path(msg);
            detail(field.as_deref(), msg.clone())
        }
    };
    let mut api = ApiError {
        code: ApiErrorCode::InvalidArgument,
        message: "config validation failed".to_owned(),
        field_path: None,
        cause: Some(error.to_string()),
        details: Vec::new(),
    };
    if let Some(d) = field_detail {
        api.details.push(d);
    }
    api
}

/// Extract a top-level field-path hint from an `Invalid*` config message.
///
/// Two recognized formats:
/// 1. `"<path-prefix>: <detail>"` (e.g. `inbounds[0] \`tag\`: ...`) → leading
///    token of the prefix.
/// 2. A backtick-wrapped field name anywhere in the message (e.g.
///    `` `runtime.udp_upstream_idle_timeout_seconds` must be ... ``) → the
///    wrapped name.
///
/// `None` when neither yields a usable token.
fn invalid_config_field_path(message: &str) -> Option<String> {
    if let Some((prefix, _)) = message.split_once(':') {
        let token = prefix.split_whitespace().next()?.trim_matches('`');
        if !token.is_empty() {
            return Some(token.to_owned());
        }
    }
    let mut parts = message.split('`');
    parts.next()?; // text before the first backtick
    let inner = parts.next()?.trim();
    if inner.is_empty() || inner.contains(' ') {
        None
    } else {
        Some(inner.to_owned())
    }
}

fn engine_error_to_api(error: EngineError) -> ApiError {
    match error {
        EngineError::CompiledFeatureDisabled { .. } => ApiError {
            code: ApiErrorCode::FeatureDisabled,
            message: "requested feature is not enabled in this build".to_owned(),
            field_path: None,
            cause: Some(error.to_string()),
            details: Vec::new(),
        },
        EngineError::Config(error) => config_error_to_api(error),
        EngineError::SelectorGroupNotFound { .. } => ApiError {
            code: ApiErrorCode::NotFound,
            message: "policy was not found".to_owned(),
            field_path: Some("policy_tag".to_owned()),
            cause: Some(error.to_string()),
            details: Vec::new(),
        },
        EngineError::SelectorGroupTypeMismatch { .. } => ApiError {
            code: ApiErrorCode::InvalidArgument,
            message: "target policy is not selectable".to_owned(),
            field_path: Some("policy_tag".to_owned()),
            cause: Some(error.to_string()),
            details: Vec::new(),
        },
        EngineError::SelectorTargetNotFound { .. } | EngineError::MissingRouteTarget { .. } => {
            ApiError {
                code: ApiErrorCode::InvalidArgument,
                message: "policy target is invalid".to_owned(),
                field_path: Some("target_tag".to_owned()),
                cause: Some(error.to_string()),
                details: Vec::new(),
            }
        }
        EngineError::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::NotFound => ApiError {
            code: ApiErrorCode::NotFound,
            message: io_err.to_string(),
            field_path: Some("flow_id".to_owned()),
            cause: Some(error.to_string()),
            details: Vec::new(),
        },
        EngineError::Io(_) => ApiError {
            code: ApiErrorCode::InvalidArgument,
            message: error.to_string(),
            field_path: None,
            cause: None,
            details: Vec::new(),
        },
        error => ApiError {
            code: ApiErrorCode::Internal,
            message: "command execution failed".to_owned(),
            field_path: None,
            cause: Some(error.to_string()),
            details: Vec::new(),
        },
    }
}

#[cfg(test)]
mod tests;
