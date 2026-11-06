//! Remaining root compatibility callers delegate to the one actual Stream role.
use crate::{
    inventory::{PreparedTcpCandidate, PreparedTcpCandidateExecution},
    protocol_registry::TcpExecutionServices,
    runtime::tcp_dispatch::operation::PreparedTcpConnectOperation,
    transport::{EstablishedTcpOutbound, TcpOutboundFailure},
};
use std::{future::Future, pin::Pin};
pub(super) struct Connect {
    pub(super) tag: String,
    pub(super) server: String,
    pub(super) port: u16,
}
impl PreparedTcpConnectOperation for Connect {
    fn execute<'a>(
        &'a self,
        services: TcpExecutionServices,
        session: &'a zero_core::Session,
    ) -> Pin<Box<dyn Future<Output = Result<EstablishedTcpOutbound, TcpOutboundFailure>> + Send + 'a>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            let failure = |reason, message: &'static str| {
                let mut error = zero_platform_tokio::contract::ChannelError::new(
                    zero_platform_tokio::contract::Stage::Prepare,
                    reason,
                    zero_platform_tokio::contract::Impact::Component,
                )
                .with_source(std::io::Error::other(message));
                if let Some(invocation) = &services.invocation {
                    error.session = invocation.identity.session();
                }
                crate::component::stream_bridge::failure(
                    "connect_upstream_shadowsocks",
                    error,
                    &(self.server.clone(), self.port),
                )
            };
            let plan = services.components.as_ref().map_err(|source| {
                crate::component::stream_bridge::failure(
                    "connect_upstream_shadowsocks",
                    crate::component::instance::retained(source.clone()),
                    &(self.server.clone(), self.port),
                )
            })?;
            let slot = plan.configured(&self.tag).ok_or_else(|| {
                failure(
                    zero_platform_tokio::contract::Reason::Unsupported,
                    "same configured SS Stream role is unavailable",
                )
            })?;
            let prepared = PreparedTcpCandidate {
                health_tag: None,
                tag: Some(self.tag.clone()),
                protocol: "shadowsocks".into(),
                endpoint: Some((self.server.clone(), self.port)),
                execution: PreparedTcpCandidateExecution::Native(slot.clone()),
            };
            // Legacy relay's destination DTO is not an admission source. Carry
            // only the actual existing Business ID for control-byte metering;
            // the complete original facts/caller/ticket stay in `services`.
            let mut destination = session.clone();
            if let Some(actual) = services
                .invocation
                .as_ref()
                .and_then(|invocation| invocation.identity.session())
            {
                destination.id = actual.0;
            }
            crate::component::stream_bridge::connect(&services, &destination, &slot, &prepared)
                .await
        })
    }
}
