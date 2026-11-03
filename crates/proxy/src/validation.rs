use zero_config::RuntimeConfig;
use zero_engine::{EngineError, EnginePlan};

use crate::ProtocolInventory;

/// Validate configuration against this build without starting an engine,
/// binding listeners, or opening runtime persistence (including Fake-IP).
/// Relative resources still resolve against the original configuration path.
pub fn validate_config(config: &RuntimeConfig) -> Result<(), EngineError> {
    config.validate()?;
    let routing = zero_runtime::configuration::PreparedRouting::prepare(config)?;
    validate_routing(config, &routing)
}

/// Validate the actual once-prepared initial input without reloading its
/// resources or replacing its source owner. No engine/host is started.
pub fn validate_prepared_configuration(
    prepared: &zero_runtime::configuration::PreparedConfiguration,
) -> Result<(), EngineError> {
    prepared.config.validate()?;
    validate_routing(&prepared.config, &prepared.routing)
}

fn validate_routing(
    config: &RuntimeConfig,
    routing: &zero_runtime::configuration::PreparedRouting,
) -> Result<(), EngineError> {
    EnginePlan::build(config)?;
    ProtocolInventory::default().validate_config(config)?;
    // Materialize DNS backends and address pools to retain construction-time
    // validation, but use the in-memory API even for file-backed configs.
    zero_dns::DnsSystem::build_with_egress_and_dispatch(
        config.runtime.dns.as_ref(),
        routing.dns_dispatch.clone(),
        zero_platform_tokio::EgressInterfaceControl::default(),
    )?;
    Ok(())
}

#[cfg(test)]
#[path = "validation/tests.rs"]
mod tests;
