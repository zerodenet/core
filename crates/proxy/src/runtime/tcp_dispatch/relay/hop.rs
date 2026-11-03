use crate::{
    inventory::{PreparedTcpRelayExecution, PreparedTcpRelayHop},
    protocol_registry::TcpExecutionServices,
    runtime::tcp_dispatch::operation::LazyTcpRelayCarrier,
    transport::TcpRelayStream,
};
pub(super) async fn execute(
    services: &TcpExecutionServices,
    hop: &PreparedTcpRelayHop,
    carrier: LazyTcpRelayCarrier<'static>,
    session: &zero_core::Session,
) -> Result<TcpRelayStream, zero_engine::EngineError> {
    match &hop.execution {
        PreparedTcpRelayExecution::Native(selected) => {
            crate::component::stream_bridge::relay::hop::connect(
                services,
                hop,
                selected,
                zero_core::request::Destination::new(session.target.clone(), session.port),
                carrier,
            )
            .await
            .map_err(|source| {
                crate::component::stream_bridge::failure(
                    "relay_last",
                    source,
                    &(hop.server.clone(), hop.port),
                )
                .error
            })
        }
        PreparedTcpRelayExecution::Legacy(operation) => {
            operation
                .execute_lazy(services.upstream_observed(&hop.tag), carrier, session)
                .await
        }
    }
}
