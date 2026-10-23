use std::future::Future;
use std::pin::Pin;

use zero_core::Session;

use super::contract::PreparedTcpConnectOperation;
use crate::protocol_registry::TcpExecutionServices;
use crate::transport::{EstablishedTcpOutbound, TcpOutboundFailure};

pub(crate) struct DirectTcpConnectOperation {
    pub(crate) tag: String,
    pub(crate) configured_tag: Option<String>,
    pub(crate) dial_policy: zero_traits::DialPolicy,
}

impl PreparedTcpConnectOperation for DirectTcpConnectOperation {
    #[cfg(feature = "raw-ip-runtime")]
    fn controlled_native_coverage(
        &self,
        services: &TcpExecutionServices,
        session: &Session,
    ) -> bool {
        services.components.as_ref().ok()
            .and_then(|plan| plan.direct(self.configured_tag.as_deref()).ok())
            .is_some_and(|slot| slot.recipe.spec().resources.iter().any(|resource| matches!(resource,
                zero_platform_tokio::contract::component::ResourceDeclaration::ControlledDirectStream { .. })))
            && !matches!(session.target, zero_core::Address::Domain(_))
            && !matches!(
                session.effective_direct_target(),
                zero_core::Address::Domain(_)
            )
    }

    fn execute<'a>(
        &'a self,
        services: TcpExecutionServices,
        session: &'a Session,
    ) -> Pin<Box<dyn Future<Output = Result<EstablishedTcpOutbound, TcpOutboundFailure>> + Send + 'a>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            if let Some(invocation) = services.invocation.clone() {
                return crate::component::direct_bridge::connect(
                    &services,
                    session,
                    &self.tag,
                    self.configured_tag.as_deref(),
                    &self.dial_policy,
                    invocation,
                )
                .await;
            }
            execute_direct_tcp_operation(
                services,
                session,
                PreparedTcpOperation::Direct {
                    tag: &self.tag,
                    dial_policy: &self.dial_policy,
                },
            )
            .await
        })
    }
}

enum PreparedTcpOperation<'a> {
    Direct {
        tag: &'a str,
        dial_policy: &'a zero_traits::DialPolicy,
    },
}

async fn execute_direct_tcp_operation(
    services: TcpExecutionServices,
    session: &Session,
    operation: PreparedTcpOperation<'_>,
) -> Result<EstablishedTcpOutbound, TcpOutboundFailure> {
    let PreparedTcpOperation::Direct { tag, dial_policy } = operation;
    match services.connect_direct(session, dial_policy).await {
        Ok(connection) => Ok(EstablishedTcpOutbound::direct(
            tag,
            (connection.remote.ip().to_string(), connection.remote.port()),
            connection.socket.into(),
            connection.network,
        )),
        Err(failure) => Err(TcpOutboundFailure {
            stage: failure.stage,
            error: failure.error.into(),
            upstream_endpoint: None,
            network: Some(failure.network),
        }),
    }
}
