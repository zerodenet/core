use std::net::IpAddr;
use std::sync::Arc;

use zero_config::ModeConfig;
use zero_core::Address;
use zero_router::{RouteAction, RouteContext};

use super::{Engine, EngineRuntimeSnapshot, RouteDecision, RouteTrace};

mod cache;
pub use cache::IpRouteCache;

/// A route decision and whether real destination IPs are still needed.
/// This is an in-process execution contract; the public trace format is unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteEvaluation {
    pub trace: RouteTrace,
    pub needs_resolution: bool,
}

impl Engine {
    /// Evaluate routing and DNS requirements together against the same mode and
    /// snapshot. A matched bypass is final; an ordinary match can still be
    /// overridden by an unresolved IP bypass. This method never performs I/O.
    pub fn evaluate_route_in_snapshot(
        &self,
        snapshot: &EngineRuntimeSnapshot,
        address: &Address,
        sni: Option<&str>,
        inbound_tag: Option<&str>,
        resolved_ips: &[IpAddr],
    ) -> RouteEvaluation {
        let mode = self.mode.lock().unwrap_or_else(|e| e.into_inner()).clone();
        evaluate(snapshot, &mode, address, sni, inbound_tag, resolved_ips)
    }

    /// Reuse pure IP rule evaluation within one snapshot and proxy mode.
    /// Target resolution, group scheduling, admission and executable resource
    /// preparation remain the caller's responsibility on every use.
    pub fn evaluate_ip_route_cached_in_snapshot(
        &self,
        snapshot: &Arc<EngineRuntimeSnapshot>,
        address: IpAddr,
        inbound_tag: Option<&str>,
        cache: &mut IpRouteCache,
    ) -> RouteEvaluation {
        // Mode is independently mutable while a snapshot is being staged or
        // replaced. The cache identity and evaluation must use this same read.
        let mode = self.mode.lock().unwrap_or_else(|e| e.into_inner()).clone();
        if let Some(evaluation) = cache.get(snapshot, &mode, inbound_tag, address) {
            return evaluation;
        }
        let destination = match address {
            IpAddr::V4(address) => Address::Ipv4(address.octets()),
            IpAddr::V6(address) => Address::Ipv6(address.octets()),
        };
        let evaluation = evaluate(snapshot, &mode, &destination, None, inbound_tag, &[]);
        cache.insert(address, evaluation.clone());
        evaluation
    }
}

fn evaluate(
    snapshot: &EngineRuntimeSnapshot,
    mode: &ModeConfig,
    address: &Address,
    sni: Option<&str>,
    inbound_tag: Option<&str>,
    resolved_ips: &[IpAddr],
) -> RouteEvaluation {
    let exception = snapshot.bypass.decide_trace_with_context_and_resolved_ips(
        RouteContext {
            address,
            sni,
            inbound_tag,
        },
        resolved_ips,
    );
    if matches!(exception.action, RouteAction::Direct) {
        return RouteEvaluation {
            trace: RouteTrace {
                decision: RouteDecision::Direct,
                mode: mode.kind().to_owned(),
                route_mode: zero_router::RouteMode::Auto,
                matched_rule: None,
            },
            needs_resolution: false,
        };
    }
    let trace = match mode {
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
                route_mode: trace.mode,
                matched_rule: trace.matched_rule.map(|matched| crate::MatchedRouteRule {
                    index: matched.index,
                    condition: matched.condition,
                }),
            }
        }
        ModeConfig::Direct => RouteTrace {
            decision: RouteDecision::Direct,
            mode: mode.kind().to_owned(),
            route_mode: zero_router::RouteMode::Auto,
            matched_rule: None,
        },
        ModeConfig::Global { outbound } => RouteTrace {
            decision: RouteDecision::Route(outbound.clone()),
            mode: mode.kind().to_owned(),
            route_mode: zero_router::RouteMode::Auto,
            matched_rule: None,
        },
    };
    let needs_resolution = resolved_ips.is_empty()
        && matches!(address, Address::Domain(_))
        && !matches!(mode, ModeConfig::Direct)
        && (snapshot.bypass.requires_resolved_ip()
            || (matches!(mode, ModeConfig::Rule)
                && trace.matched_rule.is_none()
                && snapshot.router.requires_resolved_ip()));
    RouteEvaluation {
        trace,
        needs_resolution,
    }
}
