//! Configuration resource preparation belongs to runtime ownership, never validation.
mod compile;
mod io;
mod notifications;
mod rule_sets;
mod source;
mod startup;
pub use compile::{DnsConfigPreparation, RuntimeConfigPreparation};
pub use notifications::{ConfigurationReconcileChanges, ConfigurationReconcileNotifications};
pub use source::{
    ConfigurationSource, SourceDisposition, SourcePersistError, SourceWriteObservation,
};
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
        let source = Arc::new(ConfigurationSource::default());
        startup::with_source(source.clone(), || Self::prepare_owned(config, &source)).map_err(
            |source| ConfigError::ReadConfig {
                path: "configuration resource preparation".into(),
                source,
            },
        )?
    }
    pub fn prepare_owned(
        config: &RuntimeConfig,
        source: &ConfigurationSource,
    ) -> Result<Self, ConfigError> {
        source
            .io
            .ensure_available()
            .map_err(|source| ConfigError::ReadConfig {
                path: "configuration resource preparation".into(),
                source,
            })?;
        config.validate()?;
        compile::prepare(config, &source.io)
    }
}

#[cfg(test)]
#[path = "../tests/configuration.rs"]
mod tests;

/// An inert, once-prepared input. No engine/proxy type crosses the runtime boundary.
#[derive(Debug)]
pub struct PreparedConfiguration {
    pub config: RuntimeConfig,
    pub source: Arc<ConfigurationSource>,
    pub routing: PreparedRouting,
}
impl PreparedConfiguration {
    pub fn prepare(config: RuntimeConfig) -> Result<Self, ConfigError> {
        Self::prepare_startup(config, Arc::new(ConfigurationSource::default()))
    }
    pub fn prepare_startup(
        config: RuntimeConfig,
        source: Arc<ConfigurationSource>,
    ) -> Result<Self, ConfigError> {
        startup::with_source(source.clone(), || {
            Self::prepare_owned(config, source.clone())
        })
        .map_err(|source| ConfigError::ReadConfig {
            path: "configuration startup".into(),
            source,
        })?
    }
    pub fn prepare_owned(
        mut config: RuntimeConfig,
        source: Arc<ConfigurationSource>,
    ) -> Result<Self, ConfigError> {
        config.materialize_endpoints()?;
        let routing = PreparedRouting::prepare_owned(&config, &source)?;
        Ok(Self {
            config,
            routing,
            source,
        })
    }
}

fn with_cold_io<T>(
    prepare: impl FnOnce(&io::ConfigurationIo) -> Result<T, ConfigError>,
) -> Result<T, ConfigError> {
    let source = Arc::new(ConfigurationSource::default());
    startup::with_source(source.clone(), || prepare(&source.io)).map_err(|source| {
        ConfigError::ReadConfig {
            path: "configuration resource preparation".into(),
            source,
        }
    })?
}
