//! Website traffic has no business SessionId and uses the actual declared target owner.
use super::*;
use tokio::io::AsyncWriteExt;
use zero_platform_tokio::contract::inbound::AcceptedFallback;
pub(super) async fn run(
    runtime: InboundRouteRuntime,
    request: Box<dyn AcceptedFallback>,
    cancel: CancellationToken,
) -> Result<(), EngineError> {
    let caller = runtime
        .accepted_tasks()
        .ok_or_else(|| std::io::Error::other("fallback lacks original accepted task"))?
        .caller()
        .map_err(std::io::Error::other)?;
    let resources = runtime
        .resources
        .as_ref()
        .and_then(|resources| resources.upgrade())
        .ok_or_else(|| std::io::Error::other("fallback original resource provider retired"))?;
    let route = request.route().clone();
    let mut upstream = resources
        .open_inbound_stream(caller, route.endpoint)
        .await
        .map_err(std::io::Error::other)?;
    let prefix = zero_transport::proxy_protocol::encode(
        route.proxy_protocol,
        route.source,
        route.destination,
    )?;
    upstream.write_all(&prefix).await?;
    let mut streams = request
        .replay(Box::pin(upstream))
        .await
        .map_err(std::io::Error::other)?;
    let result = tokio::select! {biased; _=cancel.cancelled()=>Ok(()), result=tokio::io::copy_bidirectional(&mut streams.inbound,&mut streams.upstream)=>result.map(|_|())};
    match result {
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotConnected | std::io::ErrorKind::BrokenPipe
            ) =>
        {
            Ok(())
        }
        result => result.map_err(EngineError::from),
    }
}
