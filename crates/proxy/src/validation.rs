use zero_config::RuntimeConfig;
use zero_engine::{EngineError, EnginePlan};

use crate::ProtocolInventory;

/// Validate configuration against this build without starting an engine,
/// binding listeners, or opening runtime persistence (including Fake-IP).
/// Relative resources still resolve against the original configuration path.
pub fn validate_config(config: &RuntimeConfig) -> Result<(), EngineError> {
    config.validate()?;
    config.compile_route()?;
    EnginePlan::build(config)?;
    ProtocolInventory::default().validate_config(config)?;
    // Materialize DNS backends and address pools to retain construction-time
    // validation, but use the in-memory API even for file-backed configs.
    zero_dns::DnsSystem::build_with_egress_and_dispatch(
        config.runtime.dns.as_ref(),
        config.compile_dns_dispatch()?,
        zero_platform_tokio::EgressInterfaceControl::default(),
    )?;
    Ok(())
}
