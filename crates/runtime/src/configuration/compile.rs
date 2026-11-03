use std::collections::HashMap;
use std::path::Path;

use zero_router::{RouteAction, Rule, RuleCondition, RuleSet, RuleSetMatcher};

use super::rule_sets::{compile_rule_sets, CompiledRuleSets};
use zero_config::{
    ConfigError, DnsConfig, RouteActionConfig, RouteConfig, RouteRuleConfig, RuleConditionConfig,
    RuntimeConfig,
};

pub trait RuntimeConfigPreparation {
    fn compile_route_bypass(&self) -> Result<RuleSet, ConfigError>;
    fn compile_route(&self) -> Result<RuleSet, ConfigError>;
    fn compile_dns_dispatch(
        &self,
    ) -> Result<Option<zero_router::DomainDispatcher<String>>, ConfigError>;
}
trait RouteCompilation {
    fn compile(
        &self,
        compiled_rule_sets: &CompiledRuleSets,
        geoip: Option<std::sync::Arc<maxminddb::Reader<Vec<u8>>>>,
    ) -> Result<RuleSet, ConfigError>;
}
pub trait DnsConfigPreparation {
    fn compile_dispatch(
        &self,
        rule_sets: &[zero_config::RouteRuleSetConfig],
        base_dir: Option<&Path>,
    ) -> Result<zero_router::DomainDispatcher<String>, ConfigError>;
}
trait DnsCompilation {
    fn compile_dispatch_with(
        &self,
        compiled_rule_sets: &CompiledRuleSets,
    ) -> Result<zero_router::DomainDispatcher<String>, ConfigError>;
}
trait RuleCompilation {
    fn compile(
        &self,
        compiled_rule_sets: &HashMap<String, RuleSetMatcher>,
    ) -> Result<Rule, ConfigError>;
}
trait ConditionCompilation {
    fn compile(
        &self,
        compiled_rule_sets: &HashMap<String, RuleSetMatcher>,
    ) -> Result<RuleCondition, ConfigError>;
}

impl RuntimeConfigPreparation for RuntimeConfig {
    fn compile_route_bypass(&self) -> Result<RuleSet, ConfigError> {
        Ok(super::PreparedRouting::prepare(self)?
            .bypass
            .as_ref()
            .clone())
    }
    fn compile_route(&self) -> Result<RuleSet, ConfigError> {
        Ok(super::PreparedRouting::prepare(self)?
            .router
            .as_ref()
            .clone())
    }
    fn compile_dns_dispatch(
        &self,
    ) -> Result<Option<zero_router::DomainDispatcher<String>>, ConfigError> {
        Ok(super::PreparedRouting::prepare(self)?.dns_dispatch)
    }
}

pub(super) fn prepare(config: &RuntimeConfig) -> Result<super::PreparedRouting, ConfigError> {
    let compiled = compile_rule_sets(&config.route.rule_sets, config.source_dir())?;
    let geoip = load_geoip(config.route.geoip_database.as_deref(), config.source_dir())?;
    let mut router = config.route.compile(&compiled, geoip.clone())?;
    router.rules.extend(
        config
            .automatic_route_prefixes()?
            .into_iter()
            .map(|(prefix, tag)| Rule {
                condition: RuleCondition::Ip(vec![prefix]),
                action: RouteAction::Route(tag),
                mode: zero_router::RouteMode::Auto,
            }),
    );
    let mut bypass = config.route.clone();
    bypass.rules = bypass
        .bypass
        .iter()
        .cloned()
        .map(|condition| RouteRuleConfig {
            condition,
            action: RouteActionConfig::Direct,
            mode: zero_config::RouteModeConfig::Auto,
        })
        .collect();
    bypass.final_action = RouteActionConfig::Reject;
    bypass.final_mode = zero_config::RouteModeConfig::Auto;
    let bypass_geoip = bypass
        .bypass
        .iter()
        .any(bypass_uses_resources)
        .then_some(geoip)
        .flatten();
    Ok(super::PreparedRouting {
        router: std::sync::Arc::new(router),
        bypass: std::sync::Arc::new(bypass.compile(&compiled, bypass_geoip)?),
        dns_dispatch: config
            .runtime
            .dns
            .as_ref()
            .map(|dns| dns.compile_dispatch_with(&compiled))
            .transpose()?,
    })
}

fn load_geoip(
    path: Option<&str>,
    base_dir: Option<&Path>,
) -> Result<Option<std::sync::Arc<maxminddb::Reader<Vec<u8>>>>, ConfigError> {
    path.map(|path| {
        let resolved = resolve_path(path, base_dir);
        let data = std::fs::read(&resolved).map_err(|source| {
            ConfigError::InvalidRuleSet(format!(
                "failed to read geoip database at {}: {source}",
                resolved.display()
            ))
        })?;
        maxminddb::Reader::from_source(data)
            .map(std::sync::Arc::new)
            .map_err(|error| {
                ConfigError::InvalidRuleSet(format!("invalid geoip database: {error}"))
            })
    })
    .transpose()
}

impl RouteCompilation for RouteConfig {
    fn compile(
        &self,
        compiled_rule_sets: &CompiledRuleSets,
        geoip: Option<std::sync::Arc<maxminddb::Reader<Vec<u8>>>>,
    ) -> Result<RuleSet, ConfigError> {
        let mut rules = Vec::with_capacity(self.rules.len());

        for rule in &self.rules {
            rules.push(rule.compile(compiled_rule_sets)?);
        }

        if let Some(reader) = geoip {
            let mut router = RuleSet::with_geoip(rules, self.final_action.compile(), reader);
            router.final_mode = self.final_mode.compile();
            Ok(router)
        } else {
            let mut router = RuleSet::new(rules, self.final_action.compile());
            router.final_mode = self.final_mode.compile();
            Ok(router)
        }
    }
}

impl DnsConfigPreparation for DnsConfig {
    fn compile_dispatch(
        &self,
        rule_sets: &[zero_config::RouteRuleSetConfig],
        base_dir: Option<&Path>,
    ) -> Result<zero_router::DomainDispatcher<String>, ConfigError> {
        let compiled_rule_sets = compile_rule_sets(rule_sets, base_dir)?;
        self.compile_dispatch_with(&compiled_rule_sets)
    }
}

impl DnsCompilation for DnsConfig {
    fn compile_dispatch_with(
        &self,
        compiled_rule_sets: &CompiledRuleSets,
    ) -> Result<zero_router::DomainDispatcher<String>, ConfigError> {
        let rules = self
            .dispatch
            .iter()
            .map(|rule| {
                Ok((
                    rule.condition.compile(compiled_rule_sets)?,
                    rule.server.clone(),
                ))
            })
            .collect::<Result<Vec<_>, ConfigError>>()?;
        Ok(zero_router::DomainDispatcher::new(
            rules,
            self.default_server.clone(),
        ))
    }
}

fn resolve_path(path: &str, base_dir: Option<&Path>) -> std::path::PathBuf {
    let candidate = std::path::Path::new(path);
    if candidate.is_absolute() {
        return candidate.to_path_buf();
    }
    match base_dir {
        Some(base_dir) => base_dir.join(candidate),
        None => candidate.to_path_buf(),
    }
}

impl RuleCompilation for RouteRuleConfig {
    fn compile(
        &self,
        compiled_rule_sets: &HashMap<String, RuleSetMatcher>,
    ) -> Result<Rule, ConfigError> {
        Ok(Rule {
            condition: self.condition.compile(compiled_rule_sets)?,
            action: self.action.compile(),
            mode: self.mode.compile(),
        })
    }
}

impl ConditionCompilation for RuleConditionConfig {
    fn compile(
        &self,
        compiled_rule_sets: &HashMap<String, RuleSetMatcher>,
    ) -> Result<RuleCondition, ConfigError> {
        match self {
            Self::Inbound { values } => {
                validate_non_empty_values("inbound", values)?;
                Ok(RuleCondition::Inbound(values.clone()))
            }
            Self::Domain { values } => {
                validate_domain_values(values)?;
                Ok(RuleCondition::Domain(values.clone()))
            }
            Self::DomainKeyword { values } => {
                validate_domain_values(values)?;
                Ok(RuleCondition::DomainKeyword(values.clone()))
            }
            Self::DomainRegex { values } => {
                let compiled: Result<Vec<_>, _> = values
                    .iter()
                    .map(|p| zero_router::CompiledRegex::new(p.clone()))
                    .collect();
                match compiled {
                    Ok(regexes) => Ok(RuleCondition::DomainRegex(regexes)),
                    Err(e) => Err(ConfigError::InvalidRuleCondition(format!(
                        "invalid regex pattern: {e}"
                    ))),
                }
            }
            Self::Ip { values } => {
                if values.is_empty() {
                    return Err(ConfigError::InvalidRuleCondition(
                        "`ip` condition requires at least one CIDR".to_owned(),
                    ));
                }

                Ok(RuleCondition::Ip(values.clone()))
            }
            Self::RuleSet { tag } => compiled_rule_sets
                .get(tag)
                .cloned()
                .map(RuleCondition::RuleSet)
                .ok_or_else(|| ConfigError::UndefinedRuleSetTag { tag: tag.clone() }),
            Self::GeoIp { values } => {
                if values.is_empty() {
                    return Err(ConfigError::InvalidRuleCondition(
                        "`geoip` condition requires at least one country code".to_owned(),
                    ));
                }
                Ok(RuleCondition::GeoIp(values.clone()))
            }
            Self::Sni { values } => {
                validate_domain_values(values)?;
                Ok(RuleCondition::Sni(values.clone()))
            }
            Self::And { items } => {
                compile_nested_condition("and", items, compiled_rule_sets, RuleCondition::And)
            }
            Self::Or { items } => {
                compile_nested_condition("or", items, compiled_rule_sets, RuleCondition::Or)
            }
        }
    }
}

fn validate_domain_values(values: &[String]) -> Result<(), ConfigError> {
    validate_non_empty_values("domain", values)
}

fn validate_non_empty_values(kind: &str, values: &[String]) -> Result<(), ConfigError> {
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

fn compile_nested_condition<F>(
    kind: &'static str,
    items: &[RuleConditionConfig],
    compiled_rule_sets: &HashMap<String, RuleSetMatcher>,
    wrap: F,
) -> Result<RuleCondition, ConfigError>
where
    F: Fn(Vec<RuleCondition>) -> RuleCondition,
{
    if items.is_empty() {
        return Err(ConfigError::InvalidRuleCondition(format!(
            "`{kind}` condition requires at least one nested condition"
        )));
    }

    let mut compiled = Vec::with_capacity(items.len());
    for item in items {
        compiled.push(item.compile(compiled_rule_sets)?);
    }

    Ok(wrap(compiled))
}

fn bypass_uses_resources(condition: &RuleConditionConfig) -> bool {
    match condition {
        RuleConditionConfig::RuleSet { .. } | RuleConditionConfig::GeoIp { .. } => true,
        RuleConditionConfig::And { items } | RuleConditionConfig::Or { items } => {
            items.iter().any(bypass_uses_resources)
        }
        _ => false,
    }
}
