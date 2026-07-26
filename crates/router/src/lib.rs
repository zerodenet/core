use std::net::IpAddr;
use std::sync::Arc;

use zero_core::Address;

mod condition;
mod rule_set;

pub use condition::{condition_describe, CompiledRegex, RuleCondition};
pub use rule_set::RuleSetMatcher;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum RouteAction {
    Route(String),
    #[default]
    Direct,
    Reject,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rule {
    pub condition: RuleCondition,
    pub action: RouteAction,
}

pub struct RuleSet {
    pub rules: Vec<Rule>,
    pub final_action: RouteAction,
    pub geoip_db: Option<Arc<maxminddb::Reader<Vec<u8>>>>,
}

impl std::fmt::Debug for RuleSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuleSet")
            .field("rules", &self.rules)
            .field("final_action", &self.final_action)
            .field("geoip_db", &self.geoip_db.is_some())
            .finish()
    }
}

impl RuleSet {
    pub fn new(rules: Vec<Rule>, final_action: RouteAction) -> Self {
        Self {
            rules,
            final_action,
            geoip_db: None,
        }
    }

    pub fn with_geoip(
        rules: Vec<Rule>,
        final_action: RouteAction,
        db: Arc<maxminddb::Reader<Vec<u8>>>,
    ) -> Self {
        Self {
            rules,
            final_action,
            geoip_db: Some(db),
        }
    }

    pub fn decide_ref(&self, address: &Address, sni: Option<&str>) -> &RouteAction {
        self.decide_ref_with_context(RouteContext {
            address,
            sni,
            inbound_tag: None,
        })
    }

    pub fn decide_ref_with_context(&self, context: RouteContext<'_>) -> &RouteAction {
        self.decide_ref_with_context_and_resolved_ips(context, &[])
    }

    pub fn decide_ref_with_context_and_resolved_ips(
        &self,
        context: RouteContext<'_>,
        resolved_ips: &[IpAddr],
    ) -> &RouteAction {
        let route_queries = condition::prepare_route_queries(context.address, resolved_ips);
        self.rules
            .iter()
            .find(|rule| {
                route_queries.iter().any(|query| {
                    condition::condition_matches(
                        &rule.condition,
                        context,
                        self.geoip_db.as_deref(),
                        query.rule_query(),
                        query.resolved_ip(),
                    )
                })
            })
            .map(|rule| &rule.action)
            .unwrap_or(&self.final_action)
    }

    pub fn decide(&self, address: &Address, sni: Option<&str>) -> RouteAction {
        self.decide_ref(address, sni).clone()
    }

    pub fn decide_with_context(&self, context: RouteContext<'_>) -> RouteAction {
        self.decide_ref_with_context(context).clone()
    }

    pub fn decide_with_context_and_resolved_ips(
        &self,
        context: RouteContext<'_>,
        resolved_ips: &[IpAddr],
    ) -> RouteAction {
        self.decide_ref_with_context_and_resolved_ips(context, resolved_ips)
            .clone()
    }

    /// Like [`decide`](Self::decide) but also returns which rule matched
    /// (index + condition summary), for `diagnostics.trace_route`.
    /// `matched_rule` is `None` when the decision came from `final_action`.
    pub fn decide_trace(&self, address: &Address, sni: Option<&str>) -> RouteDecision {
        self.decide_trace_with_context(RouteContext {
            address,
            sni,
            inbound_tag: None,
        })
    }

    pub fn decide_trace_with_context(&self, context: RouteContext<'_>) -> RouteDecision {
        self.decide_trace_with_context_and_resolved_ips(context, &[])
    }

    pub fn decide_trace_with_context_and_resolved_ips(
        &self,
        context: RouteContext<'_>,
        resolved_ips: &[IpAddr],
    ) -> RouteDecision {
        let route_queries = condition::prepare_route_queries(context.address, resolved_ips);
        if let Some((index, rule)) = self.rules.iter().enumerate().find(|(_, rule)| {
            route_queries.iter().any(|query| {
                condition::condition_matches(
                    &rule.condition,
                    context,
                    self.geoip_db.as_deref(),
                    query.rule_query(),
                    query.resolved_ip(),
                )
            })
        }) {
            RouteDecision {
                action: rule.action.clone(),
                matched_rule: Some(MatchedRule {
                    index,
                    condition: condition_describe(&rule.condition),
                }),
            }
        } else {
            RouteDecision {
                action: self.final_action.clone(),
                matched_rule: None,
            }
        }
    }

    pub fn requires_resolved_ip(&self) -> bool {
        self.rules
            .iter()
            .any(|rule| condition::requires_resolved_ip(&rule.condition))
    }
}

/// The rule that produced a routing decision (for `diagnostics.trace_route`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchedRule {
    /// 0-based index into `RuleSet::rules`.
    pub index: usize,
    /// Human-readable condition summary (see [`condition_describe`]).
    pub condition: String,
}

/// A routing decision plus the rule that produced it (if any).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteDecision {
    pub action: RouteAction,
    /// `None` when the decision came from `final_action` (no rule matched).
    pub matched_rule: Option<MatchedRule>,
}

#[derive(Debug, Clone, Copy)]
pub struct RouteContext<'a> {
    pub address: &'a Address,
    pub sni: Option<&'a str>,
    pub inbound_tag: Option<&'a str>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use ipnet::IpNet;

    fn rs(rules: Vec<Rule>, final_action: RouteAction) -> RuleSet {
        RuleSet::new(rules, final_action)
    }

    #[test]
    fn decide_trace_reports_matched_rule_index_and_condition() {
        let rules = vec![
            Rule {
                condition: RuleCondition::Domain(vec!["example.com".to_owned()]),
                action: RouteAction::Reject,
            },
            Rule {
                condition: RuleCondition::Ip(vec!["10.0.0.0/8".parse().unwrap()]),
                action: RouteAction::Route("proxy".to_owned()),
            },
        ];
        let router = rs(rules, RouteAction::Direct);

        // First rule matches.
        let d = router.decide_trace(&Address::Domain("example.com".to_owned()), None);
        assert_eq!(d.action, RouteAction::Reject);
        let matched = d.matched_rule.expect("rule matched");
        assert_eq!(matched.index, 0);
        assert!(matched.condition.contains("domain: example.com"));

        // Second rule matches.
        let d = router.decide_trace(&Address::Ipv4([10, 1, 2, 3]), None);
        assert_eq!(d.action, RouteAction::Route("proxy".to_owned()));
        assert_eq!(d.matched_rule.as_ref().unwrap().index, 1);
        assert!(d
            .matched_rule
            .as_ref()
            .unwrap()
            .condition
            .contains("ip: 10.0.0.0/8"));
    }

    #[test]
    fn decide_trace_final_action_has_no_matched_rule() {
        let router = rs(vec![], RouteAction::Direct);
        let d = router.decide_trace(&Address::Domain("unmatched.example".to_owned()), None);
        assert_eq!(d.action, RouteAction::Direct);
        assert!(d.matched_rule.is_none());
    }

    #[test]
    fn condition_describe_covers_variants() {
        assert_eq!(
            condition_describe(&RuleCondition::Inbound(vec![
                "hk-in".into(),
                "jp-in".into()
            ])),
            "inbound: hk-in, jp-in"
        );
        assert_eq!(
            condition_describe(&RuleCondition::Domain(vec!["a.com".into(), "b.com".into()])),
            "domain: a.com, b.com"
        );
        let ip: IpNet = "192.168.0.0/16".parse().unwrap();
        assert_eq!(
            condition_describe(&RuleCondition::Ip(vec![ip])),
            "ip: 192.168.0.0/16"
        );
        assert_eq!(
            condition_describe(&RuleCondition::And(vec![
                RuleCondition::DomainKeyword(vec!["login".into()]),
                RuleCondition::GeoIp(vec!["CN".into()]),
            ])),
            "and(domain_keyword: login, geoip: CN)"
        );
    }

    #[test]
    fn inbound_condition_matches_route_context() {
        let router = rs(
            vec![Rule {
                condition: RuleCondition::Inbound(vec!["hk-in".to_owned()]),
                action: RouteAction::Route("hk-lb".to_owned()),
            }],
            RouteAction::Direct,
        );
        let address = Address::Domain("example.com".to_owned());

        let matched = router.decide_with_context(RouteContext {
            address: &address,
            sni: None,
            inbound_tag: Some("hk-in"),
        });
        assert_eq!(matched, RouteAction::Route("hk-lb".to_owned()));

        let missing = router.decide_with_context(RouteContext {
            address: &address,
            sni: None,
            inbound_tag: None,
        });
        assert_eq!(missing, RouteAction::Direct);
    }

    #[test]
    fn inbound_condition_composes_with_domain_condition() {
        let router = rs(
            vec![Rule {
                condition: RuleCondition::And(vec![
                    RuleCondition::Inbound(vec!["hk-in".to_owned()]),
                    RuleCondition::Domain(vec!["example.com".to_owned()]),
                ]),
                action: RouteAction::Route("hk-lb".to_owned()),
            }],
            RouteAction::Direct,
        );

        let matched = router.decide_with_context(RouteContext {
            address: &Address::Domain("api.example.com".to_owned()),
            sni: None,
            inbound_tag: Some("hk-in"),
        });
        assert_eq!(matched, RouteAction::Route("hk-lb".to_owned()));

        let wrong_inbound = router.decide_with_context(RouteContext {
            address: &Address::Domain("api.example.com".to_owned()),
            sni: None,
            inbound_tag: Some("jp-in"),
        });
        assert_eq!(wrong_inbound, RouteAction::Direct);
    }
}
