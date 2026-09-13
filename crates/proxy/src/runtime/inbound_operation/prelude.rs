use crate::runtime::route_runtime::InboundRouteRuntime;
use zero_engine::EngineError;
use zero_platform_tokio::TokioSocket;

pub(super) async fn prepare_socket(
    mut socket: TokioSocket,
    runtime: InboundRouteRuntime,
    enabled: bool,
) -> Result<(TokioSocket, InboundRouteRuntime), EngineError> {
    if !enabled {
        return Ok((socket, runtime));
    }
    let addresses = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        zero_transport::proxy_protocol::accept(&mut socket),
    )
    .await
    .map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "PROXY protocol header timeout",
        )
    })??;
    if let Some((source, destination)) = addresses {
        socket.set_effective_addresses(source, destination);
        return Ok((
            socket,
            runtime
                .with_source_addr(Some(source))
                .with_local_addr(Some(destination)),
        ));
    }
    Ok((socket, runtime))
}
