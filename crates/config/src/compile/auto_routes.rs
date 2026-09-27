//! Protocol-owned prefix advertisements compiled into ordinary router rules.
//! Runtime and engine only receive neutral `zero_router::Rule` values.

use std::collections::{HashMap, HashSet};

use ipnet::IpNet;
use zero_router::{RouteAction, Rule, RuleCondition};

use crate::{ConfigError, OutboundProtocolConfig, RuntimeConfig};

pub(super) fn compile(config: &RuntimeConfig) -> Result<Vec<Rule>, ConfigError> {
    let mut selected = HashSet::new();
    let mut owners = HashMap::<IpNet, String>::new();
    let mut advertised = Vec::<(IpNet, String)>::new();

    for tag in &config.route.auto_outbounds {
        if !selected.insert(tag.as_str()) {
            return Err(ConfigError::InvalidRouteAction(format!(
                "route.auto_outbounds contains duplicate tag `{tag}`"
            )));
        }
        let outbound = config
            .outbounds
            .iter()
            .find(|outbound| outbound.tag == *tag)
            .ok_or_else(|| ConfigError::UndefinedRouteTargetTag { tag: tag.clone() })?;
        let OutboundProtocolConfig::Wireguard { peers, .. } = &outbound.protocol else {
            return Err(ConfigError::InvalidRouteAction(format!(
                "outbound `{tag}` does not advertise automatic IP routes"
            )));
        };
        for peer in peers {
            for allowed in &peer.allowed_ips {
                let network = wireguard::validation::parse_network(allowed).map_err(|_| {
                    ConfigError::InvalidRouteAction(format!(
                        "outbound `{tag}` has invalid allowed IP prefix"
                    ))
                })?;
                let prefix =
                    IpNet::new(network.network_address(), network.prefix_len()).map_err(|_| {
                        ConfigError::InvalidRouteAction(format!(
                            "outbound `{tag}` has invalid allowed IP prefix"
                        ))
                    })?;
                if let Some(other) = owners.insert(prefix, tag.clone()) {
                    return Err(ConfigError::InvalidRouteAction(format!(
                        "automatic IP route `{prefix}` is declared by both `{other}` and `{tag}`"
                    )));
                }
                advertised.push((prefix, tag.clone()));
            }
        }
    }

    advertised.sort_by(|left, right| {
        right
            .0
            .prefix_len()
            .cmp(&left.0.prefix_len())
            .then_with(|| left.0.to_string().cmp(&right.0.to_string()))
    });
    Ok(advertised
        .into_iter()
        .map(|(prefix, tag)| Rule {
            condition: RuleCondition::Ip(vec![prefix]),
            action: RouteAction::Route(tag),
            mode: zero_router::RouteMode::Auto,
        })
        .collect())
}
