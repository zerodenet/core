use crate::runtime::route_runtime::InboundRouteRuntime;
use zero_core::inbound::InboundControlSession;
use zero_engine::EngineError;

pub(crate) async fn run(
    runtime: InboundRouteRuntime,
    control: Box<dyn InboundControlSession>,
    cancel: Option<zero_platform_tokio::contract::CancellationToken>,
) -> Result<(), EngineError> {
    let device = runtime.acquire_principal_device(control.auth())?;
    let (cancel_tx, mut cancel_rx) = tokio::sync::mpsc::unbounded_channel();
    let principal = control
        .auth()
        .and_then(|a| a.principal_key.as_deref())
        .map(|key| {
            runtime.register_principal_cancellation(key, move |reason| {
                let _ = cancel_tx.send(reason);
            })
        });
    let result = tokio::select! {
        _=async {match cancel {Some(cancel)=>cancel.cancelled().await,None=>std::future::pending().await}}=>Ok(()),
        result = control.run() => result.map_err(EngineError::from),
        Some(_) = cancel_rx.recv() => Ok(()),
    };
    drop(principal);
    drop(device);
    result
}

#[cfg(test)]
#[path = "control/tests.rs"]
mod tests;
