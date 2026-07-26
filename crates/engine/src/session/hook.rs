//! Synchronous in-process flow lifecycle hooks.

use std::sync::Arc;

use zero_core::{Address, Network, ProtocolType};

use crate::observability::SessionOutcome;

// ── Hook trait ────────────────────────────────────────────────────────

/// A synchronous, in-process extension point for flow lifecycle events.
///
/// Implementors can **block** a flow at creation time by returning
/// `Err(BlockReason)` from `on_flow_start`.  All methods are called from
/// within the proxy data-path; implementations must be fast and
/// non-blocking.
///
/// Each method is wrapped in `std::panic::catch_unwind` by the engine, so
/// a panicking hook will not crash the proxy.
pub trait FlowHook: Send + Sync {
    /// Called when a flow is about to be started.
    ///
    /// Return `Ok(())` to allow the flow, or `Err(reason)` to block it
    /// immediately (the peer will see a connection reset / refused).
    fn on_flow_start(&self, ctx: &FlowContext) -> Result<(), BlockReason> {
        let _ = ctx;
        Ok(())
    }

    /// Called after a flow has completed (success, failure, or cancelled).
    fn on_flow_end(&self, ctx: &FlowContext, outcome: SessionOutcome, stats: &FlowTraffic) {
        let _ = (ctx, outcome, stats);
    }
}

// ── Context types ─────────────────────────────────────────────────────

/// Immutable snapshot of a flow at creation time, consumed by hooks.
#[derive(Debug, Clone)]
pub struct FlowContext {
    pub flow_id: u64,
    pub inbound_tag: Option<String>,
    pub outbound_tag: Option<String>,
    pub target_host: String,
    pub target_port: u16,
    pub network: String,
    pub protocol: String,
    pub auth: Option<AuthSnapshot>,
    pub mode: String,
    pub started_at_unix_ms: u64,
    /// Opaque labels for hook-driven decisions (set by earlier hooks).
    pub labels: Vec<(String, String)>,
}

impl FlowContext {
    pub fn from_session(session: &zero_core::Session, mode: &str, started_at_unix_ms: u64) -> Self {
        Self {
            flow_id: session.id,
            inbound_tag: session.inbound_tag.clone(),
            outbound_tag: session.outbound_tag.clone(),
            target_host: address_str(&session.target),
            target_port: session.port,
            network: network_str(session.network).to_owned(),
            protocol: protocol_str(session.protocol).to_owned(),
            auth: session.auth.as_ref().map(|a| AuthSnapshot {
                scheme: a.scheme.clone(),
                principal_key: a.principal_key.clone(),
            }),
            mode: mode.to_owned(),
            started_at_unix_ms,
            labels: Vec::new(),
        }
    }

    pub fn from_completed(record: &super::CompletedSessionRecord) -> Self {
        Self {
            flow_id: record.id,
            inbound_tag: record.inbound_tag.clone(),
            outbound_tag: record.outbound_tag.clone(),
            target_host: address_str(&record.target),
            target_port: record.port,
            network: network_str(record.network).to_owned(),
            protocol: protocol_str(record.protocol).to_owned(),
            auth: record.auth.as_ref().map(|a| AuthSnapshot {
                scheme: a.scheme.clone(),
                principal_key: a.principal_key.clone(),
            }),
            mode: record.mode.clone(),
            started_at_unix_ms: record.started_at_unix_ms,
            labels: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AuthSnapshot {
    pub scheme: String,
    pub principal_key: Option<String>,
}

#[derive(Debug, Clone)]
pub struct FlowTraffic {
    pub bytes_up: u64,
    pub bytes_down: u64,
    pub duration_ms: u64,
}

impl FlowTraffic {
    pub fn from_completed(record: &super::CompletedSessionRecord) -> Self {
        Self {
            bytes_up: record.inbound_rx_bytes.max(record.outbound_tx_bytes),
            bytes_down: record.outbound_rx_bytes.max(record.inbound_tx_bytes),
            duration_ms: record.duration_ms,
        }
    }
}

/// Reason a flow was blocked by a hook.
#[derive(Debug, Clone)]
pub struct BlockReason {
    pub code: String,
    pub message: String,
}

impl BlockReason {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

// ── Composite chain ────────────────────────────────────────────────────

/// A chain of hooks called in registration order.
///
/// `on_flow_start` returns at the **first** hook that blocks; subsequent
/// hooks are not consulted.
pub struct FlowHookChain {
    hooks: Vec<Arc<dyn FlowHook>>,
}

impl std::fmt::Debug for FlowHookChain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlowHookChain")
            .field("hooks", &self.hooks.len())
            .finish()
    }
}

impl FlowHookChain {
    pub fn empty() -> Self {
        Self { hooks: Vec::new() }
    }

    pub fn push(&mut self, hook: Arc<dyn FlowHook>) {
        self.hooks.push(hook);
    }

    pub fn is_empty(&self) -> bool {
        self.hooks.is_empty()
    }
}

impl FlowHook for FlowHookChain {
    fn on_flow_start(&self, ctx: &FlowContext) -> Result<(), BlockReason> {
        for hook in &self.hooks {
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| hook.on_flow_start(ctx)));
            match result {
                Ok(Ok(())) => {}
                Ok(Err(reason)) => return Err(reason),
                Err(payload) => {
                    let msg = payload
                        .downcast_ref::<&str>()
                        .map(|s| s.to_string())
                        .or_else(|| payload.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "unknown panic".to_owned());
                    tracing::warn!(
                        flow_id = ctx.flow_id,
                        panic_msg = %msg,
                        "flow hook panicked in on_flow_start; allowing flow"
                    );
                }
            }
        }
        Ok(())
    }

    fn on_flow_end(&self, ctx: &FlowContext, outcome: SessionOutcome, stats: &FlowTraffic) {
        for hook in &self.hooks {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                hook.on_flow_end(ctx, outcome, stats)
            }));
            if let Err(payload) = result {
                let msg = payload
                    .downcast_ref::<&str>()
                    .map(|s| s.to_string())
                    .or_else(|| payload.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "unknown panic".to_owned());
                tracing::warn!(
                    flow_id = ctx.flow_id,
                    panic_msg = %msg,
                    "flow hook panicked in on_flow_end"
                );
            }
        }
    }
}

// ── helpers ────────────────────────────────────────────────────────────

fn address_str(addr: &Address) -> String {
    match addr {
        Address::Domain(d) => d.clone(),
        Address::Ipv4(a) => format!("{}.{}.{}.{}", a[0], a[1], a[2], a[3]),
        Address::Ipv6(a) => std::net::Ipv6Addr::from(*a).to_string(),
    }
}

fn network_str(net: Network) -> &'static str {
    match net {
        Network::Tcp => "tcp",
        Network::Udp => "udp",
    }
}

fn protocol_str(proto: ProtocolType) -> &'static str {
    proto.as_str()
}
