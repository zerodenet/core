//! Configuration resource preparation belongs to runtime ownership, never validation.
mod compile;
mod rule_sets;
mod source;
pub use compile::{DnsConfigPreparation, RuntimeConfigPreparation};
pub use source::{ConfigurationSource, SourceWriteObservation};
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

/// An inert, once-prepared input. No engine/proxy type crosses the runtime boundary.
#[derive(Debug)]
pub struct PreparedConfiguration {
    pub config: RuntimeConfig,
    pub routing: PreparedRouting,
}
impl PreparedConfiguration {
    pub fn prepare(mut config: RuntimeConfig) -> Result<Self, ConfigError> {
        config.materialize_endpoints()?;
        let routing = PreparedRouting::prepare(&config)?;
        Ok(Self { config, routing })
    }
}
