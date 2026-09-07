use std::net::IpAddr;

use zero_config::ModeConfig;
use zero_core::Address;
use zero_router::{RouteAction, RouteContext};

use super::{Engine, EngineRuntimeSnapshot, RouteDecision, RouteTrace};

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
                    matched_rule: None,
                },
                needs_resolution: false,
            };
        }
        let trace = match &mode {
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
}
