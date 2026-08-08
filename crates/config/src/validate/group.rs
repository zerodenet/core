use std::collections::{HashMap, HashSet};

use crate::{ConfigError, OutboundGroupConfig, OutboundGroupKind};

impl OutboundGroupConfig {
    pub(crate) fn validate(&self, target_tags: &HashSet<String>) -> Result<(), ConfigError> {
        match &self.group {
            OutboundGroupKind::Selector {
                outbounds,
                default,
                selected,
            } => {
                validate_group_outbounds("selector", outbounds, target_tags)?;

                if let Some(default) = default {
                    validate_selector_choice("default", default, outbounds)?;
                }

                if let Some(selected) = selected {
                    validate_selector_choice("selected", selected, outbounds)?;
                }

                Ok(())
            }
            OutboundGroupKind::Fallback { outbounds } => {
                validate_group_outbounds("fallback", outbounds, target_tags)
            }
            OutboundGroupKind::UrlTest {
                outbounds,
                url,
                interval_seconds,
                tolerance_ms: _,
            } => {
                validate_group_outbounds("url_test", outbounds, target_tags)?;

                if let Some(url) = url {
                    if url.trim().is_empty() {
                        return Err(ConfigError::InvalidOutboundGroup(
                            "`url_test.url` must not be empty when set".to_owned(),
                        ));
                    }

                    if !url.starts_with("http://") {
                        return Err(ConfigError::InvalidOutboundGroup(
                            "`url_test` group currently only supports `http://` probe urls"
                                .to_owned(),
                        ));
                    }
                }

                if *interval_seconds == 0 {
                    return Err(ConfigError::InvalidOutboundGroup(
                        "`url_test` group `interval_seconds` must be greater than 0".to_owned(),
                    ));
                }

                Ok(())
            }
            OutboundGroupKind::Relay { proxies } => {
                if proxies.len() < 2 {
                    return Err(ConfigError::InvalidOutboundGroup(
                        "`relay` group requires at least 2 proxies".to_owned(),
                    ));
                }
                for proxy in proxies {
                    validate_group_member_tag("relay", proxy, target_tags)?;
                }
                Ok(())
            }
            OutboundGroupKind::LoadBalance {
                outbounds,
                default,
                strategy: _,
            } => {
                validate_group_outbounds("load_balance", outbounds, target_tags)?;

                if let Some(default) = default {
                    validate_selector_choice("default", default, outbounds)?;
                }

                Ok(())
            }
        }
    }
}

pub(super) fn validate_group_reference_graph(
    groups: &[OutboundGroupConfig],
) -> Result<(), ConfigError> {
    let group_map = groups
        .iter()
        .map(|group| (group.tag.as_str(), group))
        .collect::<HashMap<_, _>>();
    let mut visited = HashSet::new();
    let mut stack = Vec::new();

    for group in groups {
        validate_group_reference_target(group.tag(), &group_map, &mut visited, &mut stack)?;
    }

    Ok(())
}

fn validate_group_outbounds(
    kind: &'static str,
    outbounds: &[String],
    target_tags: &HashSet<String>,
) -> Result<(), ConfigError> {
    if outbounds.is_empty() {
        return Err(ConfigError::InvalidOutboundGroup(format!(
            "`{kind}` group requires at least one outbound"
        )));
    }

    for outbound in outbounds {
        validate_group_member_tag(kind, outbound, target_tags)?;
    }

    Ok(())
}

fn validate_group_member_tag(
    kind: &'static str,
    tag: &str,
    target_tags: &HashSet<String>,
) -> Result<(), ConfigError> {
    if tag.trim().is_empty() {
        return Err(ConfigError::InvalidOutboundGroup(format!(
            "`{kind}` group does not allow empty target tags"
        )));
    }

    if !target_tags.contains(tag) {
        return Err(ConfigError::InvalidOutboundGroup(format!(
            "`{kind}` group references undefined target `{tag}`"
        )));
    }

    Ok(())
}

fn validate_selector_choice(
    field: &'static str,
    value: &str,
    outbounds: &[String],
) -> Result<(), ConfigError> {
    if value.trim().is_empty() {
        return Err(ConfigError::InvalidOutboundGroup(format!(
            "`selector` group `{field}` must not be empty"
        )));
    }

    if !outbounds.iter().any(|outbound| outbound == value) {
        return Err(ConfigError::InvalidOutboundGroup(format!(
            "`selector` group `{field}` must reference one of its `outbounds`"
        )));
    }

    Ok(())
}

fn validate_group_reference_target<'a>(
    tag: &'a str,
    group_map: &HashMap<&'a str, &'a OutboundGroupConfig>,
    visited: &mut HashSet<&'a str>,
    stack: &mut Vec<&'a str>,
) -> Result<(), ConfigError> {
    if visited.contains(tag) {
        return Ok(());
    }

    if let Some(index) = stack.iter().position(|current| *current == tag) {
        let mut cycle = stack[index..].to_vec();
        cycle.push(tag);
        return Err(ConfigError::InvalidOutboundGroup(format!(
            "group reference cycle detected: {}",
            cycle.join(" -> ")
        )));
    }

    let Some(group) = group_map.get(tag) else {
        return Ok(());
    };

    stack.push(tag);
    for member in group.group.members() {
        if group_map.contains_key(member.as_str()) {
            validate_group_reference_target(member.as_str(), group_map, visited, stack)?;
        }
    }
    stack.pop();
    visited.insert(tag);

    Ok(())
}
