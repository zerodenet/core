use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use zero_core::Address;
use zero_router::{RouteAction, RouteContext, Rule, RuleCondition, RuleSet, RuleSetMatcher};
use zero_rule::{Rule as MatcherRule, RuleSet as MatcherRuleSet, RuleSetCompiler};

#[test]
fn routes_domain_suffix_to_reject() {
    let rules = vec![Rule {
        condition: RuleCondition::Domain(vec!["blocked.example".to_owned()]),
        action: RouteAction::Reject,
    }];
    let ruleset = RuleSet::new(rules, RouteAction::Direct);

    let action = ruleset.decide(&Address::Domain("api.blocked.example".to_owned()), None);

    assert_eq!(action, RouteAction::Reject);
}

#[test]
fn borrowed_decision_reuses_ruleset_action() {
    let rules = vec![Rule {
        condition: RuleCondition::Domain(vec!["blocked.example".to_owned()]),
        action: RouteAction::Reject,
    }];
    let ruleset = RuleSet::new(rules, RouteAction::Direct);

    let action = ruleset.decide_ref(&Address::Domain("api.blocked.example".to_owned()), None);

    assert_eq!(action, &RouteAction::Reject);
}

#[test]
fn route_condition_uses_zero_rule_matcher_and_reports_its_tag() {
    let (compiled, _) = RuleSetCompiler
        .compile(MatcherRuleSet::new(vec![
            MatcherRule::DomainSuffix("example.com".to_owned()),
            MatcherRule::Ipv4Cidr("10.0.0.0/8".parse().unwrap()),
        ]))
        .expect("compile matcher set");
    let ruleset = RuleSet::new(
        vec![Rule {
            condition: RuleCondition::RuleSet(RuleSetMatcher::new("private", Arc::new(compiled))),
            action: RouteAction::Reject,
        }],
        RouteAction::Direct,
    );

    assert_eq!(
        ruleset.decide(&Address::Domain("api.Example.COM".to_owned()), None),
        RouteAction::Reject
    );
    assert_eq!(
        ruleset.decide(&Address::Ipv4([10, 2, 3, 4]), None),
        RouteAction::Reject
    );

    let trace = ruleset.decide_trace(&Address::Domain("api.example.com".to_owned()), None);
    assert_eq!(
        trace.matched_rule.expect("matched rule").condition,
        "rule_set: private"
    );
}

#[test]
fn unresolved_domain_can_be_rechecked_against_resolved_ip_rules() {
    let ruleset = RuleSet::new(
        vec![Rule {
            condition: RuleCondition::Ip(vec!["10.0.0.0/8".parse().unwrap()]),
            action: RouteAction::Direct,
        }],
        RouteAction::Route("proxy".to_owned()),
    );
    let address = Address::Domain("domestic.example".to_owned());
    let context = RouteContext {
        address: &address,
        sni: None,
        inbound_tag: None,
    };

    assert_eq!(
        ruleset.decide_with_context(context),
        RouteAction::Route("proxy".to_owned())
    );
    assert_eq!(
        ruleset.decide_with_context_and_resolved_ips(
            context,
            &[
                IpAddr::V4(Ipv4Addr::new(203, 0, 113, 1)),
                IpAddr::V4(Ipv4Addr::new(10, 2, 3, 4)),
            ],
        ),
        RouteAction::Direct
    );
}

#[test]
fn rule_set_query_keeps_domain_and_resolved_ip_facts_together() {
    let (compiled, _) = RuleSetCompiler
        .compile(MatcherRuleSet::new(vec![
            MatcherRule::DomainSuffix("private.example".to_owned()),
            MatcherRule::Ipv4Cidr("10.0.0.0/8".parse().unwrap()),
        ]))
        .expect("compile matcher set");
    let ruleset = RuleSet::new(
        vec![Rule {
            condition: RuleCondition::RuleSet(RuleSetMatcher::new("private", Arc::new(compiled))),
            action: RouteAction::Direct,
        }],
        RouteAction::Route("proxy".to_owned()),
    );
    let address = Address::Domain("unlisted.example".to_owned());

    let trace = ruleset.decide_trace_with_context_and_resolved_ips(
        RouteContext {
            address: &address,
            sni: None,
            inbound_tag: None,
        },
        &[IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3))],
    );

    assert_eq!(trace.action, RouteAction::Direct);
    assert_eq!(trace.matched_rule.expect("matched IP rule").index, 0);
}

#[test]
fn and_conditions_use_one_resolved_ip_at_a_time() {
    let ruleset = RuleSet::new(
        vec![Rule {
            condition: RuleCondition::And(vec![
                RuleCondition::Ip(vec!["10.0.0.0/8".parse().unwrap()]),
                RuleCondition::Ip(vec!["192.168.0.0/16".parse().unwrap()]),
            ]),
            action: RouteAction::Direct,
        }],
        RouteAction::Route("proxy".to_owned()),
    );
    let address = Address::Domain("multi-address.example".to_owned());

    assert_eq!(
        ruleset.decide_with_context_and_resolved_ips(
            RouteContext {
                address: &address,
                sni: None,
                inbound_tag: None,
            },
            &[
                IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3)),
                IpAddr::V4(Ipv4Addr::new(192, 168, 1, 2)),
            ],
        ),
        RouteAction::Route("proxy".to_owned())
    );
}

#[test]
fn domain_only_rules_do_not_request_dns_backed_route_facts() {
    let (compiled, _) = RuleSetCompiler
        .compile(MatcherRuleSet::new(vec![MatcherRule::DomainSuffix(
            "example.com".to_owned(),
        )]))
        .expect("compile domain matcher set");
    let ruleset = RuleSet::new(
        vec![Rule {
            condition: RuleCondition::RuleSet(RuleSetMatcher::new("domains", Arc::new(compiled))),
            action: RouteAction::Direct,
        }],
        RouteAction::Route("proxy".to_owned()),
    );

    assert!(!ruleset.requires_resolved_ip());
}
