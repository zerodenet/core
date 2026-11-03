use super::io::ConfigurationIo;
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ipnet::IpNet;
use sha2::{Digest, Sha256};
use zero_router::RuleSetMatcher;
use zero_rule::protocol::decode_json;
use zero_rule::zrs::{MappedRuleSet, PrewarmPolicy, VerifyMode};
use zero_rule::{Rule, RuleMatcher, RuleSet, RuleSetCompiler};

use zero_config::{ConfigError, RouteRuleSetConfig, RuleSetFormatConfig};

pub type CompiledRuleSets = HashMap<String, RuleSetMatcher>;

pub fn compile_rule_sets(
    rule_sets: &[RouteRuleSetConfig],
    base_dir: Option<&Path>,
) -> Result<CompiledRuleSets, ConfigError> {
    super::with_cold_io(|owner| compile_rule_sets_owned(rule_sets, base_dir, owner))
}
pub(super) fn compile_rule_sets_owned(
    rule_sets: &[RouteRuleSetConfig],
    base_dir: Option<&Path>,
    owner: &ConfigurationIo,
) -> Result<CompiledRuleSets, ConfigError> {
    let mut compiled = HashMap::with_capacity(rule_sets.len());

    for rule_set in rule_sets {
        compiled.insert(
            rule_set.tag.clone(),
            compile_rule_set(&rule_set.tag, rule_set, base_dir, owner)?,
        );
    }

    Ok(compiled)
}

fn compile_rule_set(
    tag: &str,
    rule_set: &RouteRuleSetConfig,
    base_dir: Option<&Path>,
    owner: &ConfigurationIo,
) -> Result<RuleSetMatcher, ConfigError> {
    let path = resolve_rule_set_path(rule_set.source_path(), base_dir);
    let (matcher, digest): (Arc<dyn RuleMatcher>, [u8; 32]) = match rule_set.format {
        RuleSetFormatConfig::DomainList => {
            let raw = read_text_rule_set(&path, owner)?;
            (
                compile_semantic_rule_set(tag, parse_domain_list(tag, &raw)?)?,
                source_digest(0, raw.as_bytes()),
            )
        }
        RuleSetFormatConfig::CidrList => {
            let raw = read_text_rule_set(&path, owner)?;
            (
                compile_semantic_rule_set(tag, parse_cidr_list(tag, &raw)?)?,
                source_digest(1, raw.as_bytes()),
            )
        }
        RuleSetFormatConfig::ZeroRuleIr => {
            let raw = read_rule_set(&path, owner)?;
            let semantic = decode_json(&raw)
                .map_err(|error| invalid_rule_set(tag, format!("invalid Zero Rule IR: {error}")))?;
            (
                compile_semantic_rule_set(tag, semantic)?,
                source_digest(2, &raw),
            )
        }
        RuleSetFormatConfig::Zrs => {
            let mmap = owner
                .with_file(
                    || File::open(&path),
                    |file| {
                        let size = file.metadata()?.len();
                        if size > zero_rule::zrs::MAX_FILE_SIZE {
                            return Err(std::io::Error::other(
                                zero_rule::zrs::ZrsError::ResourceLimit {
                                    resource: "file size",
                                    actual: size,
                                    maximum: zero_rule::zrs::MAX_FILE_SIZE,
                                },
                            ));
                        }
                        // SAFETY: preserve ZRS's existing immutable published-file contract.
                        unsafe { file.map_readonly() }
                    },
                )
                .map_err(|error| {
                    if error
                        .get_ref()
                        .is_some_and(|source| source.is::<zero_rule::zrs::ZrsError>())
                    {
                        *error
                            .into_inner()
                            .unwrap()
                            .downcast::<zero_rule::zrs::ZrsError>()
                            .expect("checked original ZRS error")
                    } else {
                        zero_rule::zrs::ZrsError::Io(error)
                    }
                })
                .map_err(|error| {
                    invalid_rule_set(
                        tag,
                        format!("failed to open ZRS `{}`: {error}", path.display()),
                    )
                })?;
            // Hash the actual retained mapping after its original FD closes;
            // neither a path, timestamp nor a config tag establishes equality.
            let digest = source_digest(3, &mmap);
            // The actual file close has been confirmed before domain handoff.
            let mapped =
                MappedRuleSet::from_mmap(mmap, VerifyMode::FullChecksum).map_err(|error| {
                    invalid_rule_set(
                        tag,
                        format!("failed to open ZRS `{}`: {error}", path.display()),
                    )
                })?;
            mapped.prewarm(PrewarmPolicy::Roots);
            (Arc::new(mapped), digest)
        }
    };

    Ok(RuleSetMatcher::new(tag.to_owned(), matcher).with_source_digest(digest))
}

fn source_digest(format: u8, bytes: &[u8]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"zero.prepared-rule-set.v1");
    digest.update([format]);
    digest.update(bytes);
    digest.finalize().into()
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

fn read_text_rule_set(path: &Path, owner: &ConfigurationIo) -> Result<String, ConfigError> {
    let raw = read_rule_set(path, owner)?;
    String::from_utf8(raw).map_err(|error| {
        ConfigError::InvalidRuleSet(format!(
            "rule set at `{}` is not valid UTF-8: {error}",
            path.display()
        ))
    })
}

fn read_rule_set(path: &Path, owner: &ConfigurationIo) -> Result<Vec<u8>, ConfigError> {
    owner.read(path).map_err(|source| ConfigError::ReadRuleSet {
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
