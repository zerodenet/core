//! A retained recipe stores selected operations and weak authority, never an
//! Engine, historical snapshot, complete plan, or resolver. Static dependency
//! identity and the actual protocol-owner admission are separate checks.
use super::{egress_generation, PreparedTcpRelayPrefix};
use crate::{
    inventory::PreparedTcpCandidateExecution, protocol_registry::TcpExecutionServices,
    runtime::current_services::CurrentServiceSource,
};
use std::{
    io,
    sync::{Arc, Weak},
};
use zero_engine::{EndpointAdmission, EngineError, OutboundStaticBinding};

#[derive(Clone)]
pub(super) enum RetainedSource {
    Bound(Recipe),
    // Explicit migration gap: unknown owners retain the existing compatibility
    // path. Unsupported is never converted into a successful owner check.
    Legacy(TcpExecutionServices),
    Rejected(Arc<EngineError>),
}
#[derive(Clone)]
pub(super) struct Recipe {
    source: Weak<CurrentServiceSource>,
    dependencies: Vec<OutboundStaticBinding>,
    network_generation: u64,
    invocation: Option<crate::component::direct_bridge::Invocation>,
    ingress: Option<crate::component::request::IngressIdentity>,
}
impl RetainedSource {
    pub(super) fn capture(
        services: &TcpExecutionServices,
        prefix: &PreparedTcpRelayPrefix,
    ) -> Self {
        let capture = || -> Result<Recipe, EngineError> {
            check_owners(prefix)?;
            if matches!(
                prefix.first.execution,
                PreparedTcpCandidateExecution::Native(_)
            ) && services.invocation.is_none()
            {
                return Err(unsupported(
                    "native retained prefix requires actual request identity",
                ));
            }
            if services.current_source.upgrade().is_none() {
                return Err(unsupported("retained relay has no host source"));
            }
            let tags = std::iter::once(prefix.first.tag.as_deref())
                .chain(prefix.relay_hops.iter().map(|hop| Some(hop.tag.as_str())));
            let dependencies = tags
                .map(|tag| {
                    tag.and_then(|tag| services.snapshot().plan().outbound_static_binding(tag))
                        .ok_or_else(|| {
                            unsupported("retained relay lacks a concrete static dependency")
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Recipe {
                source: services.current_source.clone(),
                dependencies,
                network_generation: egress_generation(services),
                invocation: services.invocation.clone(),
                ingress: services.ingress.clone(),
            })
        };
        match capture() {
            Ok(recipe) => Self::Bound(recipe),
            Err(EngineError::Core(zero_core::Error::Unsupported(_))) => {
                Self::Legacy(services.clone())
            }
            Err(error) => Self::Rejected(Arc::new(error)),
        }
    }
    pub(super) fn services(
        &self,
        prefix: &PreparedTcpRelayPrefix,
    ) -> Result<TcpExecutionServices, EngineError> {
        match self {
            Self::Legacy(services) => Ok(services.clone()),
            Self::Rejected(error) => Err(EngineError::Io(io::Error::other(error.clone()))),
            Self::Bound(recipe) => {
                let source = recipe
                    .source
                    .upgrade()
                    .ok_or_else(|| revoked("relay host is gone"))?;
                let mut services = source.runtime_services()?.execution();
                if egress_generation(&services) != recipe.network_generation {
                    return Err(revoked("relay physical egress generation changed"));
                }
                let admission = EndpointAdmission::from_snapshot(services.snapshot());
                for dependency in &recipe.dependencies {
                    if !dependency.matches(services.snapshot().plan()) {
                        return Err(revoked("relay selected static dependency changed"));
                    }
                    if let Some((reason, endpoint)) = admission.outbound_denial(dependency.tag()) {
                        return Err(EngineError::InvalidPlan {
                            message: format!("{reason}: {endpoint}"),
                        });
                    }
                }
                if let PreparedTcpCandidateExecution::Native(captured) = &prefix.first.execution {
                    let tag = prefix
                        .first
                        .tag
                        .as_deref()
                        .ok_or_else(|| revoked("native prefix lost its tag"))?;
                    let plan = services.components.as_ref().map_err(|error| {
                        EngineError::Io(io::Error::other(crate::component::instance::retained(
                            error.clone(),
                        )))
                    })?;
                    let current = plan
                        .configured(tag)
                        .ok_or_else(|| revoked("native prefix owner is no longer configured"))?;
                    if !Arc::ptr_eq(captured, &current) {
                        return Err(revoked("native prefix actual owner changed"));
                    }
                }
                check_owners(prefix)?;
                services.invocation = recipe.invocation.clone();
                services.ingress = recipe.ingress.clone();
                Ok(services)
            }
        }
    }
}

/// Every operation actually used by this prepared path must have a real owner.
/// No group resolution or load-balance pick occurs while checking this recipe.
pub(in crate::runtime) fn check_owners(prefix: &PreparedTcpRelayPrefix) -> Result<(), EngineError> {
    match &prefix.first.execution {
        PreparedTcpCandidateExecution::Connect(operation) => operation.retained_admission()?,
        PreparedTcpCandidateExecution::Native(slot) => slot
            .service_admission()
            .ok_or_else(|| unsupported("native prefix has no actual host admission"))?
            .check_current()
            .map_err(|source| EngineError::Io(io::Error::other(source)))?,
        _ => return Err(unsupported("retained first-hop owner is not migrated")),
    }
    for hop in &prefix.relay_hops {
        hop.operation.retained_admission()?;
    }
    #[cfg(feature = "udp-runtime")]
    for plan in prefix.datagram_prefixes.iter().flatten() {
        plan.retained_admission()?;
    }
    Ok(())
}
fn unsupported(message: &'static str) -> EngineError {
    zero_core::Error::Unsupported(message).into()
}
fn revoked(message: &'static str) -> EngineError {
    EngineError::Io(io::Error::new(io::ErrorKind::NotConnected, message))
}

#[cfg(test)]
mod tests;
