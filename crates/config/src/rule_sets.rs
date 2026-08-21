use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ipnet::IpNet;
use zero_router::RuleSetMatcher;
use zero_rule::protocol::decode_json;
use zero_rule::zrs::{MappedRuleSet, PrewarmPolicy, VerifyMode};
use zero_rule::{Rule, RuleMatcher, RuleSet, RuleSetCompiler};

use crate::{ConfigError, RuleSetConfig, RuleSetFormatConfig};

pub type CompiledRuleSets = HashMap<String, RuleSetMatcher>;

pub fn compile_rule_sets(
    rule_sets: &BTreeMap<String, RuleSetConfig>,
    base_dir: Option<&Path>,
) -> Result<CompiledRuleSets, ConfigError> {
    let mut compiled = HashMap::with_capacity(rule_sets.len());

    for (tag, rule_set) in rule_sets {
        compiled.insert(tag.clone(), compile_rule_set(tag, rule_set, base_dir)?);
    }

    Ok(compiled)
}

fn compile_rule_set(
    tag: &str,
    rule_set: &RuleSetConfig,
    base_dir: Option<&Path>,
) -> Result<RuleSetMatcher, ConfigError> {
    let path = resolve_rule_set_path(rule_set.source_path(), base_dir);
    let matcher: Arc<dyn RuleMatcher> = match rule_set.format {
        RuleSetFormatConfig::DomainList => {
            let raw = read_text_rule_set(&path)?;
            compile_semantic_rule_set(tag, parse_domain_list(tag, &raw)?)?
        }
        RuleSetFormatConfig::CidrList => {
            let raw = read_text_rule_set(&path)?;
            compile_semantic_rule_set(tag, parse_cidr_list(tag, &raw)?)?
        }
        RuleSetFormatConfig::ZeroRuleIr => {
            let raw = read_rule_set(&path)?;
            let semantic = decode_json(&raw)
                .map_err(|error| invalid_rule_set(tag, format!("invalid Zero Rule IR: {error}")))?;
            compile_semantic_rule_set(tag, semantic)?
        }
        RuleSetFormatConfig::Zrs => {
            let mapped = MappedRuleSet::open(&path, VerifyMode::FullChecksum).map_err(|error| {
                invalid_rule_set(
                    tag,
                    format!("failed to open ZRS `{}`: {error}", path.display()),
                )
            })?;
            mapped.prewarm(PrewarmPolicy::Roots);
            Arc::new(mapped)
        }
    };

    Ok(RuleSetMatcher::new(tag.to_owned(), matcher))
}

fn compile_semantic_rule_set(
    tag: &str,
    semantic: RuleSet,
) -> Result<Arc<dyn RuleMatcher>, ConfigError> {
    let (compiled, _) = RuleSetCompiler.compile(semantic).map_err(|error| {
        invalid_rule_set(tag, format!("failed to compile matcher set: {error}"))
    })?;
    Ok(Arc::new(compiled))
}

fn parse_domain_list(tag: &str, raw: &str) -> Result<RuleSet, ConfigError> {
    let rules = source_lines(raw)
        .map(|value| Rule::DomainSuffix(value.trim_start_matches('.').to_owned()))
        .collect::<Vec<_>>();
    ensure_not_empty(tag, &rules)?;
    Ok(RuleSet::new(rules))
}

fn parse_cidr_list(tag: &str, raw: &str) -> Result<RuleSet, ConfigError> {
    let mut rules = Vec::new();
    for value in source_lines(raw) {
        let network = value.parse::<IpNet>().map_err(|error| {
            invalid_rule_set(tag, format!("contains invalid CIDR `{value}`: {error}"))
        })?;
        rules.push(match network {
            IpNet::V4(network) => Rule::Ipv4Cidr(network),
            IpNet::V6(network) => Rule::Ipv6Cidr(network),
        });
    }
    ensure_not_empty(tag, &rules)?;
    Ok(RuleSet::new(rules))
}

fn source_lines(raw: &str) -> impl Iterator<Item = &str> {
    raw.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#') && !line.starts_with("//"))
}

fn ensure_not_empty(tag: &str, rules: &[Rule]) -> Result<(), ConfigError> {
    if rules.is_empty() {
        return Err(invalid_rule_set(tag, "does not contain any entries"));
    }
    Ok(())
}

fn read_text_rule_set(path: &Path) -> Result<String, ConfigError> {
    let raw = read_rule_set(path)?;
    String::from_utf8(raw).map_err(|error| {
        ConfigError::InvalidRuleSet(format!(
            "rule set at `{}` is not valid UTF-8: {error}",
            path.display()
        ))
    })
}

fn read_rule_set(path: &Path) -> Result<Vec<u8>, ConfigError> {
    fs::read(path).map_err(|source| ConfigError::ReadRuleSet {
        path: path.display().to_string(),
        source,
    })
}

fn invalid_rule_set(tag: &str, detail: impl std::fmt::Display) -> ConfigError {
    ConfigError::InvalidRuleSet(format!("rule set `{tag}` {detail}"))
}

fn resolve_rule_set_path(path: &str, base_dir: Option<&Path>) -> PathBuf {
    let candidate = Path::new(path);
    if candidate.is_absolute() {
        return candidate.to_path_buf();
    }

    match base_dir {
        Some(base_dir) => base_dir.join(candidate),
        None => candidate.to_path_buf(),
    }
}
