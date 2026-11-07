//! Embedded callers have no frontend Work owner. Only this branch installs
//! actual raw/wrapper registrations in the same kernel's finite control Scope.
use super::super::ProxyHandle;
use std::{error::Error, sync::Arc};
use zero_api::{CommandRequest, CommandResponse};
use zero_platform_tokio::contract::{ChannelError, Impact, Reason, Stage};
use zero_runtime::{ApplyObservation, CommandInvocationFault};

pub(super) async fn execute(
    handle: ProxyHandle,
    command: CommandRequest,
    current: Option<zero_runtime::FrontendCommand>,
) -> zero_api::ApiResult<CommandResponse> {
    let proxy = handle.proxy.clone();
    let retained = proxy.clone();
    let complete = move |observation| async move {
        retained
            .protocols
            .drive_component_supervision(settle(observation))
            .await
    };
    let result = match super::diagnostics::asynchronous(handle.clone(), command) {
        Ok(work) => match current {
            Some(current) => current.run_async(work, complete).await,
            None => {
                proxy
                    .host
                    .embedded_commands()
                    .run_async(proxy.host.control_budget(), work, complete)
                    .await
            }
        },
        Err(command) => {
            let work = move || zero_api::CommandService::execute(&handle, command);
            match current {
                Some(current) => current.run_blocking(work, complete).await,
                None => {
                    proxy
                        .host
                        .embedded_commands()
                        .run(proxy.host.control_budget(), work, complete)
                        .await
                }
            }
        }
    };
    result.map_err(|source| {
        zero_api::ApiError::new(
            zero_api::ApiErrorCode::Internal,
            format!("command execution task failed: {source}"),
        )
    })?
}
async fn settle(observation: ApplyObservation) -> Result<(), ChannelError> {
    let joined = observation.wait_joined().await;
    if let Some(faults) = observation.completion_fault(joined.as_ref()) {
        // These sources are retained by this exact bounded row before any
        // corresponding canonical owner accepts the local fault delivery.
        for source in faults.faults() {
            if let CommandInvocationFault::Cleanup(_, source) = source {
                settle_source(source.as_ref()).await;
            }
        }
        return Err(
            ChannelError::new(Stage::Close, Reason::Internal, Impact::Component)
                .with_source(faults),
        );
    }
    Ok(())
}
enum Cleanup {
    Resolution(zero_dns::DnsResolutionCall),
    Probe(zero_runtime::ProbeOwnershipFailure),
}
fn original_cleanup(source: &(dyn Error + 'static)) -> Option<Cleanup> {
    let mut current = source;
    loop {
        if let Some(actual) = current.downcast_ref::<zero_dns::DnsResolutionUnsettled>() {
            return Some(Cleanup::Resolution(actual.call().clone()));
        }
        if let Some(actual) = current.downcast_ref::<zero_runtime::ProbeOwnershipFailure>() {
            return Some(Cleanup::Probe(actual.clone()));
        }
        if let Some(inner) = current
            .downcast_ref::<std::io::Error>()
            .and_then(std::io::Error::get_ref)
        {
            current = inner;
            continue;
        }
        if let Some(inner) = current.downcast_ref::<Arc<std::io::Error>>() {
            current = inner.as_ref();
            continue;
        }
        current = current.source()?;
    }
}
async fn settle_source(source: &(dyn Error + Send + Sync + 'static)) {
    let original = original_cleanup(source);
    match original {
        Some(Cleanup::Resolution(call)) => loop {
            let closed = call
                .wait_closed(tokio::time::Instant::now() + std::time::Duration::from_secs(5))
                .await;
            if closed.is_ok_and(|closed| closed.matches(&call)) {
                break;
            }
            // Unknown keeps this same call and this wrapper's original credit.
        },
        Some(Cleanup::Probe(call)) => {
            if call.wait_settled().await.is_err() {
                // A lost canonical owner without its positive original token
                // cannot authorize refund or a replacement.
                std::future::pending::<()>().await;
            }
        }
        None => {}
    }
}

#[cfg(all(test, feature = "dns"))]
mod tests;
