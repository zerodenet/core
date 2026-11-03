//! Configuration resource preparation belongs to runtime ownership, never validation.
mod compile;
mod rule_sets;
pub use compile::{DnsConfigPreparation, RuntimeConfigPreparation};
use std::sync::Arc;
use zero_config::{ConfigError, RuntimeConfig};
use zero_router::{DomainDispatcher, RuleSet};

/// One actual resource preparation shared by traffic, bypass and DNS plans.
/// Clones retain the same compiled/mapped rule matchers and GeoIP database.
#[derive(Debug, Clone)]
pub struct PreparedRouting {
    pub router: Arc<RuleSet>,
    pub bypass: Arc<RuleSet>,
    pub dns_dispatch: Option<DomainDispatcher<String>>,
}
impl PreparedRouting {
    pub fn prepare(config: &RuntimeConfig) -> Result<Self, ConfigError> {
        config.validate()?;
        compile::prepare(config)
    }
}

#[cfg(test)]
#[path = "../tests/configuration.rs"]
mod tests;
