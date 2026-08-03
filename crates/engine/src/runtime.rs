use std::net::IpAddr;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use tracing::info;
use zero_config::{ModeConfig, RuntimeConfig};
use zero_core::Address;
use zero_router::{RouteAction, RouteContext};

use super::error::EngineError;
use super::groups::OutboundGroupStateStore;
use super::health::{OutboundHealth, PassiveRelayHealth, ProbeTriggerRegistry};
use super::observability::EngineEventLog;
use super::observability::EngineStats;
use super::plan::{
    resolve_target_chains, resolve_target_id, EnginePlan, ResolvedLeafOutbound, ResolvedOutbound,
    TargetId,
};
use super::principal::{
    PrincipalCancellationRegistry, PrincipalDeviceRegistry, PrincipalPolicyRegistry,
    PrincipalQuotaRegistry,
};
use super::session::{CompletedSessionHistory, FlowHook, FlowHookChain, SessionRegistry};

mod configuration;
mod diagnostics;
mod observability;
mod passive_health;
mod policy;
mod session;
mod snapshot;

pub use snapshot::EngineRuntimeSnapshot;

#[derive(Debug, Clone)]
pub struct Engine {
    runtime_snapshot: Arc<std::sync::RwLock<Arc<EngineRuntimeSnapshot>>>,
    mode: Arc<std::sync::Mutex<ModeConfig>>,
    next_session_id: Arc<AtomicU64>,
    session_registry: Arc<SessionRegistry>,
    principal_cancellations: Arc<PrincipalCancellationRegistry>,
    principal_devices: Arc<PrincipalDeviceRegistry>,
    principal_policies: Arc<PrincipalPolicyRegistry>,
    principal_quotas: Arc<PrincipalQuotaRegistry>,
    completed_sessions: Arc<CompletedSessionHistory>,
    event_log: Arc<EngineEventLog>,
    stats: Arc<EngineStats>,
    pub(crate) outbound_group_state: Arc<OutboundGroupStateStore>,
    pub(crate) probe_trigger_registry: Arc<ProbeTriggerRegistry>,
    flow_hook: Arc<std::sync::RwLock<Option<Arc<FlowHookChain>>>>,
    flow_completion_sink: Arc<std::sync::RwLock<Option<FlowCompletionSink>>>,
    pub(crate) outbound_health: Arc<OutboundHealth>,
    pub(crate) passive_relay_health: Arc<PassiveRelayHealth>,
    udp_upstream_idle_timeout: Duration,
    /// Reload notification channel: wakes the proxy's main loop when
    /// `reload_config` atomically swaps the plan / router / config.
    reload_notify: Arc<std::sync::Mutex<Vec<std::sync::mpsc::Sender<()>>>>,
    /// Source path of the running config.  When set, `reload_config`
    /// writes the new config back to this path so it survives restarts.
    config_path: Option<std::path::PathBuf>,
    /// Process start time (UNIX epoch milliseconds), captured on Engine::new.
    pub(crate) started_at_unix_ms: u64,
    /// ID of the OS process hosting this engine.
    pub(crate) pid: u32,
    /// External sink status injected by the event dispatcher.
    /// Updated via `update_sink_status()` when the dispatcher runs.
    sink_status: Arc<std::sync::Mutex<Vec<zero_api::SinkStatus>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteDecision {
    Route(String),
    Direct,
    Reject,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteTrace {
    pub decision: RouteDecision,
    pub mode: String,
    pub matched_rule: Option<crate::MatchedRouteRule>,
}

impl RouteDecision {
    fn into_route_action(self) -> RouteAction {
        match self {
            Self::Route(tag) => RouteAction::Route(tag),
            Self::Direct => RouteAction::Direct,
            Self::Reject => RouteAction::Reject,
        }
    }
}

impl From<&RouteAction> for RouteDecision {
    fn from(value: &RouteAction) -> Self {
        match value {
            RouteAction::Route(tag) => Self::Route(tag.clone()),
            RouteAction::Direct => Self::Direct,
            RouteAction::Reject => Self::Reject,
        }
    }
}

/// Current UNIX epoch in milliseconds.
fn started_at_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl Engine {
    pub fn new(config: RuntimeConfig) -> Result<Self, EngineError> {
        let router = Arc::new(config.route.compile(config.source_dir())?);
        let plan = Arc::new(EnginePlan::build(&config)?);
        let plan_inner = plan.clone();
        let udp_upstream_idle_timeout =
            Duration::from_secs(config.runtime.udp_upstream_idle_timeout_seconds);
        let outbound_group_state = OutboundGroupStateStore::shared();

        for &group_id in plan_inner.selector_groups() {
            let group = plan_inner
                .target(group_id)
                .expect("engine plan should resolve selector group");
            let Some(selector) = group.as_selector() else {
                continue;
            };
            outbound_group_state.initialize_selector(group_id, selector.initial_member());
        }

        for &group_id in plan_inner.urltest_groups() {
            let group = plan_inner
                .target(group_id)
                .expect("engine plan should resolve urltest group");
            let Some(urltest) = group.as_urltest() else {
                continue;
            };
            if !urltest.members().is_empty() {
                outbound_group_state.initialize_urltest(
                    group_id,
                    urltest.initial_member(),
                    urltest.members(),
                );
            }
        }

        for &group_id in plan_inner.loadbalance_groups() {
            outbound_group_state.initialize_loadbalance(group_id);
        }

        let event_log_capacity = config.runtime.event_log_capacity;
        let event_log = EngineEventLog::shared(event_log_capacity);

        info!(
            build_id = env!("CARGO_PKG_VERSION"),
            event_log_capacity, "engine started"
        );
        event_log.push_engine_started(env!("CARGO_PKG_VERSION"));

        let mode = Arc::new(std::sync::Mutex::new(config.mode.clone()));
        let principal_policies = Arc::new(PrincipalPolicyRegistry::from_config(&config));
        let principal_quota_state_path = config
            .runtime
            .principal_quota_state_path
            .as_deref()
            .map(std::path::PathBuf::from)
            .map(|path| {
                if path.is_absolute() {
                    path
                } else {
                    config
                        .source_dir()
                        .unwrap_or_else(|| std::path::Path::new("."))
                        .join(path)
                }
            });
        let principal_quotas = Arc::new(PrincipalQuotaRegistry::open(principal_quota_state_path)?);
        Ok(Self {
            runtime_snapshot: Arc::new(std::sync::RwLock::new(Arc::new(EngineRuntimeSnapshot {
                config: Arc::new(config),
                plan,
                router,
            }))),
            mode,
            next_session_id: Arc::new(AtomicU64::new(1)),
            session_registry: SessionRegistry::shared(),
            principal_cancellations: Arc::new(PrincipalCancellationRegistry::default()),
            principal_devices: Arc::new(PrincipalDeviceRegistry::default()),
            principal_policies,
            principal_quotas,
            completed_sessions: CompletedSessionHistory::shared(),
            event_log,
            stats: EngineStats::shared(),
            outbound_group_state,
            probe_trigger_registry: ProbeTriggerRegistry::shared(),
            outbound_health: Arc::new(OutboundHealth::new()),
            passive_relay_health: Arc::new(PassiveRelayHealth::default()),
            flow_hook: Arc::new(std::sync::RwLock::new(None)),
            flow_completion_sink: Arc::new(std::sync::RwLock::new(None)),
            udp_upstream_idle_timeout,
            reload_notify: Arc::new(std::sync::Mutex::new(Vec::new())),
            config_path: None,
            started_at_unix_ms: started_at_unix_ms(),
            pid: std::process::id(),
            sink_status: Arc::new(std::sync::Mutex::new(Vec::new())),
        })
    }

    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, EngineError> {
        let config = RuntimeConfig::load_from_path(path.as_ref())?;
        Self::new_with_config_path(config, path)
    }

    pub fn new_with_config_path(
        config: RuntimeConfig,
        path: impl AsRef<Path>,
    ) -> Result<Self, EngineError> {
        let mut engine = Self::new(config)?;
        engine.config_path = Some(path.as_ref().to_owned());
        Ok(engine)
    }

    pub fn config(&self) -> Arc<RuntimeConfig> {
        self.runtime_snapshot().config.clone()
    }

    pub fn runtime_snapshot(&self) -> Arc<EngineRuntimeSnapshot> {
        self.runtime_snapshot
            .read()
            .expect("runtime snapshot lock poisoned")
            .clone()
    }

    /// The config file path used to start or reload this engine.
    pub fn config_path(&self) -> Option<&std::path::Path> {
        self.config_path.as_deref()
    }

    /// UNIX epoch milliseconds when this engine was created.
    pub fn started_at_unix_ms(&self) -> u64 {
        self.started_at_unix_ms
    }

    pub fn plan(&self) -> Arc<EnginePlan> {
        self.runtime_snapshot().plan.clone()
    }

    pub fn with_udp_upstream_idle_timeout(mut self, timeout: Duration) -> Self {
        self.udp_upstream_idle_timeout = timeout;
        self
    }

    pub fn with_flow_hook(self, hook: impl FlowHook + 'static) -> Self {
        let mut chain = FlowHookChain::empty();
        chain.push(Arc::new(hook));
        *self.flow_hook.write().expect("flow hook lock poisoned") = Some(Arc::new(chain));
        self
    }

    pub fn with_flow_hook_chain(self, chain: FlowHookChain) -> Self {
        self.replace_flow_hook_chain((!chain.is_empty()).then_some(chain));
        self
    }

    /// Replace the active flow-hook chain for future lifecycle callbacks.
    ///
    /// Existing callbacks already in progress retain their cloned chain.
    pub fn replace_flow_hook_chain(&self, chain: Option<FlowHookChain>) {
        *self.flow_hook.write().expect("flow hook lock poisoned") = chain.map(Arc::new);
    }

    /// Persist completed-flow events synchronously before they are exposed to
    /// the in-memory event log. The sink must return `delivered=true` only
    /// after its durable write has completed.
    pub fn with_flow_completion_sink(
        self,
        sink: Arc<dyn zero_api::EventSink + Send + Sync>,
    ) -> Self {
        self.replace_flow_completion_sink(Some(sink));
        self
    }

    /// Replace the synchronous completed-flow persistence sink.
    ///
    /// The replacement is visible to subsequently completed sessions. A
    /// caller that manages a reporter must prepare the new sink before
    /// publishing this pointer and keep the previous sink alive until its
    /// reporter has drained.
    pub fn replace_flow_completion_sink(
        &self,
        sink: Option<Arc<dyn zero_api::EventSink + Send + Sync>>,
    ) {
        *self
            .flow_completion_sink
            .write()
            .expect("flow completion sink lock poisoned") = sink.map(FlowCompletionSink);
    }

    pub fn udp_upstream_idle_timeout(&self) -> Duration {
        self.udp_upstream_idle_timeout
    }

    pub fn mode_kind(&self) -> &'static str {
        self.mode.lock().unwrap_or_else(|e| e.into_inner()).kind()
    }

    pub fn current_mode(&self) -> ModeConfig {
        self.mode.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Atomically switch the global proxy mode at runtime.
    pub fn set_mode(&self, new_mode: ModeConfig) {
        let mut mode = self.mode.lock().unwrap_or_else(|e| e.into_inner());
        *mode = new_mode.clone();
        self.event_log.push_config_changed();
        info!(mode = new_mode.kind(), "proxy mode switched");
    }

    pub fn route_for(&self, address: &Address) -> RouteAction {
        self.route_decision(address, None).into_route_action()
    }

    pub fn route_decision(&self, address: &Address, sni: Option<&str>) -> RouteDecision {
        self.route_decision_with_inbound(address, sni, None)
    }

    pub fn route_decision_with_inbound(
        &self,
        address: &Address,
        sni: Option<&str>,
        inbound_tag: Option<&str>,
    ) -> RouteDecision {
        self.route_trace_with_inbound(address, sni, inbound_tag)
            .decision
    }

    pub fn route_trace_with_inbound(
        &self,
        address: &Address,
        sni: Option<&str>,
        inbound_tag: Option<&str>,
    ) -> RouteTrace {
        self.route_trace_with_inbound_and_resolved_ips(address, sni, inbound_tag, &[])
    }

    pub fn route_trace_with_inbound_and_resolved_ips(
        &self,
        address: &Address,
        sni: Option<&str>,
        inbound_tag: Option<&str>,
        resolved_ips: &[IpAddr],
    ) -> RouteTrace {
        let snapshot = self.runtime_snapshot();
        self.route_trace_in_snapshot_with_inbound_and_resolved_ips(
            &snapshot,
            address,
            sni,
            inbound_tag,
            resolved_ips,
        )
    }

    pub fn route_trace_in_snapshot_with_inbound_and_resolved_ips(
        &self,
        snapshot: &EngineRuntimeSnapshot,
        address: &Address,
        sni: Option<&str>,
        inbound_tag: Option<&str>,
        resolved_ips: &[IpAddr],
    ) -> RouteTrace {
        let mode = self.mode.lock().unwrap_or_else(|e| e.into_inner()).clone();
        match &mode {
            ModeConfig::Rule => {
                let trace = snapshot.router.decide_trace_with_context_and_resolved_ips(
                    RouteContext {
                        address,
                        sni,
                        inbound_tag,
                    },
                    resolved_ips,
                );
                let decision = match trace.action {
                    RouteAction::Route(tag) => RouteDecision::Route(tag),
                    RouteAction::Direct => RouteDecision::Direct,
                    RouteAction::Reject => RouteDecision::Reject,
                };
                RouteTrace {
                    decision,
                    mode: mode.kind().to_owned(),
                    matched_rule: trace.matched_rule.map(|matched| crate::MatchedRouteRule {
                        index: matched.index,
                        condition: matched.condition,
                    }),
                }
            }
            ModeConfig::Direct => RouteTrace {
                decision: RouteDecision::Direct,
                mode: mode.kind().to_owned(),
                matched_rule: None,
            },
            ModeConfig::Global { outbound } => RouteTrace {
                decision: RouteDecision::Route(outbound.clone()),
                mode: mode.kind().to_owned(),
                matched_rule: None,
            },
        }
    }

    pub fn route_requires_resolved_ip(&self) -> bool {
        let snapshot = self.runtime_snapshot();
        self.route_requires_resolved_ip_in_snapshot(&snapshot)
    }

    pub fn route_requires_resolved_ip_in_snapshot(&self, snapshot: &EngineRuntimeSnapshot) -> bool {
        if !matches!(self.current_mode(), ModeConfig::Rule) {
            return false;
        }

        snapshot.router.requires_resolved_ip()
    }

    pub fn resolve_route_decision(
        &self,
        action: RouteDecision,
    ) -> Result<(ResolvedOutbound<'static>, Option<Arc<EnginePlan>>), EngineError> {
        let snapshot = self.runtime_snapshot();
        self.resolve_route_decision_in_snapshot(&snapshot, action)
    }

    pub fn resolve_route_decision_in_snapshot(
        &self,
        snapshot: &EngineRuntimeSnapshot,
        action: RouteDecision,
    ) -> Result<(ResolvedOutbound<'static>, Option<Arc<EnginePlan>>), EngineError> {
        match action {
            RouteDecision::Direct => Ok((
                ResolvedOutbound::Single(ResolvedLeafOutbound::Direct { tag: None }),
                None,
            )),
            RouteDecision::Reject => Ok((
                ResolvedOutbound::Single(ResolvedLeafOutbound::Block { tag: None }),
                None,
            )),
            RouteDecision::Route(tag) => {
                let (resolved, plan) = self.resolve_target_in_snapshot(snapshot, &tag)?;
                Ok((resolved, Some(plan)))
            }
        }
    }

    pub fn resolve_route_action(
        &self,
        action: &RouteAction,
    ) -> Result<(ResolvedOutbound<'static>, Option<Arc<EnginePlan>>), EngineError> {
        self.resolve_route_decision(action.into())
    }

    pub fn resolve_target_id(
        &self,
        target_id: TargetId,
    ) -> Option<(ResolvedOutbound<'static>, Arc<EnginePlan>)> {
        let snapshot = self.runtime_snapshot();
        self.resolve_target_id_in_snapshot(&snapshot, target_id)
    }

    pub fn resolve_target_id_in_snapshot(
        &self,
        snapshot: &EngineRuntimeSnapshot,
        target_id: TargetId,
    ) -> Option<(ResolvedOutbound<'static>, Arc<EnginePlan>)> {
        let plan = snapshot.plan.clone();
        // SAFETY: plan is returned in the tuple.  The resolved outbound
        // borrows from data inside `plan`, which stays alive as long as
        // the caller holds the returned `Arc<EnginePlan>`.
        let resolved: ResolvedOutbound<'static> = unsafe {
            std::mem::transmute(resolve_target_id(
                &plan,
                &self.outbound_group_state,
                target_id,
            )?)
        };
        Some((resolved, plan))
    }

    pub fn resolve_target_chains(&self, target_id: TargetId) -> Vec<Vec<TargetId>> {
        let plan = self.plan();
        resolve_target_chains(&plan, &self.outbound_group_state, target_id)
    }

    pub fn resolve_target_chains_in_snapshot(
        &self,
        snapshot: &EngineRuntimeSnapshot,
        target_id: TargetId,
    ) -> Vec<Vec<TargetId>> {
        resolve_target_chains(&snapshot.plan, &self.outbound_group_state, target_id)
    }

    pub fn target_tag(&self, target_id: TargetId) -> Option<String> {
        let plan = self.plan();
        plan.target(target_id).map(|target| target.tag().to_owned())
    }

    pub fn target_tag_in_snapshot(
        &self,
        snapshot: &EngineRuntimeSnapshot,
        target_id: TargetId,
    ) -> Option<String> {
        snapshot
            .plan
            .target(target_id)
            .map(|target| target.tag().to_owned())
    }

    fn resolve_target_in_snapshot(
        &self,
        snapshot: &EngineRuntimeSnapshot,
        tag: &str,
    ) -> Result<(ResolvedOutbound<'static>, Arc<EnginePlan>), EngineError> {
        let plan = snapshot.plan.clone();
        let Some(target_id) = plan.target_id(tag) else {
            return Err(EngineError::MissingRouteTarget {
                tag: tag.to_owned(),
            });
        };
        // SAFETY: plan is returned alongside, keeping data alive.
        let resolved: ResolvedOutbound<'static> = unsafe {
            std::mem::transmute(
                resolve_target_id(&plan, &self.outbound_group_state, target_id).ok_or_else(
                    || EngineError::MissingRouteTarget {
                        tag: tag.to_owned(),
                    },
                )?,
            )
        };
        Ok((resolved, plan))
    }
}

#[derive(Clone)]
struct FlowCompletionSink(Arc<dyn zero_api::EventSink + Send + Sync>);

impl std::fmt::Debug for FlowCompletionSink {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("FlowCompletionSink")
            .field(&self.0.name())
            .finish()
    }
}
