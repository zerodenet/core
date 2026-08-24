use std::collections::HashSet;

use crate::{
    ConfigError, RouteActionConfig, RouteConfig, RouteRuleConfig, RouteRuleSetConfig,
    RuleConditionConfig, RuleSetSourceType,
};

use super::validate_tag;

impl RouteConfig {
    pub(crate) fn validate(
        &self,
        route_target_tags: &HashSet<String>,
        inbound_tags: &HashSet<String>,
        rule_set_tags: &HashSet<String>,
    ) -> Result<(), ConfigError> {
        for rule in &self.rules {
            rule.validate(route_target_tags, inbound_tags, rule_set_tags)?;
        }

        validate_route_action(&self.final_action, route_target_tags)?;

        Ok(())
    }
}

impl RouteRuleSetConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        match self.source_type {
            RuleSetSourceType::File => {
                if self.path.trim().is_empty() {
                    return Err(ConfigError::InvalidRuleSet(
                        "`file` rule set requires a non-empty `path`".to_owned(),
                    ));
                }
            }
            RuleSetSourceType::Url => {
                if self.url.as_ref().is_none_or(|u| u.trim().is_empty()) {
                    return Err(ConfigError::InvalidRuleSet(
                        "`url` rule set requires a non-empty `url`".to_owned(),
                    ));
                }
                if self.path.trim().is_empty() {
                    return Err(ConfigError::InvalidRuleSet(
                        "`url` rule set requires a non-empty `path` for cache".to_owned(),
                    ));
                }
            }
        }

        Ok(())
    }
}

impl RouteRuleConfig {
    pub(crate) fn validate(
        &self,
        route_target_tags: &HashSet<String>,
        inbound_tags: &HashSet<String>,
        rule_set_tags: &HashSet<String>,
    ) -> Result<(), ConfigError> {
        self.condition.validate(inbound_tags, rule_set_tags)?;
        validate_route_action(&self.action, route_target_tags)
    }
}

impl RuleConditionConfig {
    pub(crate) fn validate(
        &self,
        inbound_tags: &HashSet<String>,
        rule_set_tags: &HashSet<String>,
    ) -> Result<(), ConfigError> {
        match self {
            Self::Inbound { values } => {
                validate_values("inbound", values)?;
                for tag in values {
                    if !inbound_tags.contains(tag) {
                        return Err(ConfigError::InvalidRuleCondition(format!(
                            "`inbound` condition references undefined inbound tag `{tag}`"
                        )));
                    }
                }
                Ok(())
            }
            Self::Domain { values } => validate_values("domain", values),
            Self::Ip { values } => {
                if values.is_empty() {
                    return Err(ConfigError::InvalidRuleCondition(
                        "`ip` condition requires at least one CIDR".to_owned(),
                    ));
                }

                Ok(())
            }
            Self::RuleSet { tag } => {
                if tag.trim().is_empty() {
                    return Err(ConfigError::InvalidRuleCondition(
                        "`rule_set` condition requires a non-empty `tag`".to_owned(),
                    ));
                }
                if !rule_set_tags.contains(tag) {
                    return Err(ConfigError::UndefinedRuleSetTag { tag: tag.clone() });
                }
                Ok(())
            }
            Self::DomainKeyword { values } => {
                if values.is_empty() {
                    return Err(ConfigError::InvalidRuleCondition(
                        "`domain_keyword` condition requires at least one value".to_owned(),
                    ));
                }
                Ok(())
            }
            Self::DomainRegex { values } => {
                if values.is_empty() {
                    return Err(ConfigError::InvalidRuleCondition(
                        "`domain_regex` condition requires at least one pattern".to_owned(),
                    ));
                }
                Ok(())
            }
            Self::GeoIp { values } => {
                if values.is_empty() {
                    return Err(ConfigError::InvalidRuleCondition(
                        "`geoip` condition requires at least one country code".to_owned(),
                    ));
                }
                Ok(())
            }
            Self::Sni { values } => {
                if values.is_empty() {
                    return Err(ConfigError::InvalidRuleCondition(
                        "`sni` condition requires at least one domain pattern".to_owned(),
                    ));
                }
                Ok(())
            }
            Self::And { items } => {
                validate_nested_condition("and", items, inbound_tags, rule_set_tags)
            }
            Self::Or { items } => {
                validate_nested_condition("or", items, inbound_tags, rule_set_tags)
            }
        }
    }
}

pub(crate) fn validate_rule_sets(
    rule_sets: &[RouteRuleSetConfig],
) -> Result<HashSet<String>, ConfigError> {
    let mut tags = HashSet::new();
    for rule_set in rule_sets {
        validate_tag("rule set", &rule_set.tag, &mut tags)?;
        rule_set.validate()?;
    }
    Ok(tags)
}

pub(super) fn validate_route_action(
    action: &RouteActionConfig,
    route_target_tags: &HashSet<String>,
) -> Result<(), ConfigError> {
    let Some(outbound) = action.target_ref() else {
        return Ok(());
    };

    if outbound.trim().is_empty() {
        return Err(ConfigError::InvalidRouteAction(
            "`route` action requires a non-empty outbound tag".to_owned(),
        ));
    }

    if !route_target_tags.contains(outbound) {
        return Err(ConfigError::UndefinedRouteTargetTag {
            tag: outbound.to_owned(),
        });
    }

    Ok(())
}

pub(super) fn validate_route_target_tag(
    tag: &str,
    seen: &mut HashSet<String>,
) -> Result<(), ConfigError> {
    if !seen.insert(tag.to_owned()) {
        return Err(ConfigError::DuplicateRouteTargetTag {
            tag: tag.to_owned(),
        });
    }

    Ok(())
}

fn validate_nested_condition(
    kind: &'static str,
    items: &[RuleConditionConfig],
    inbound_tags: &HashSet<String>,
    rule_set_tags: &HashSet<String>,
) -> Result<(), ConfigError> {
    if items.is_empty() {
        return Err(ConfigError::InvalidRuleCondition(format!(
            "`{kind}` condition requires at least one nested condition"
        )));
    }

    for item in items {
        item.validate(inbound_tags, rule_set_tags)?;
    }

    Ok(())
}

fn validate_values(kind: &str, values: &[String]) -> Result<(), ConfigError> {
    if values.is_empty() {
        return Err(ConfigError::InvalidRuleCondition(format!(
            "`{kind}` condition requires at least one value"
        )));
    }

    if values.iter().any(|value| value.trim().is_empty()) {
        return Err(ConfigError::InvalidRuleCondition(format!(
            "`{kind}` condition does not allow empty values"
        )));
    }

    Ok(())
}
