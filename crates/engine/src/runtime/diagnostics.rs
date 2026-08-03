use zero_core::Address;
use zero_router::{RouteAction, RouteContext};

use super::Engine;
use crate::EngineError;

impl Engine {
    pub fn trace_route(
        &self,
        target: &str,
        port: u16,
        protocol: &str,
        inbound_tag: Option<&str>,
    ) -> Result<serde_json::Value, EngineError> {
        let address = match target.parse::<std::net::IpAddr>() {
            Ok(std::net::IpAddr::V4(value)) => Address::Ipv4(value.octets()),
            Ok(std::net::IpAddr::V6(value)) => Address::Ipv6(value.octets()),
            Err(_) => Address::Domain(target.to_owned()),
        };
        let snapshot = self.runtime_snapshot();
        let decision = snapshot.router.decide_trace_with_context(RouteContext {
            address: &address,
            sni: None,
            inbound_tag,
        });
        let matched_rule = decision.matched_rule.map(
            |matched| serde_json::json!({"index": matched.index, "condition": matched.condition}),
        );

        Ok(serde_json::json!({
            "target": target,
            "port": port,
            "protocol": protocol,
            "inbound_tag": inbound_tag,
            "effective_mode": self.mode_kind(),
            "route_action": match &decision.action {
                RouteAction::Route(tag) => serde_json::json!({"route": tag}),
                RouteAction::Direct => serde_json::json!("direct"),
                RouteAction::Reject => serde_json::json!("reject"),
            },
            "matched_rule": matched_rule,
        }))
    }
}
