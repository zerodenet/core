//! The component executor owns decoy work on the original authenticated carrier.
//! It never dials another connection and cannot turn a decoy into proxy admission.
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use zero_platform_tokio::contract::{
    admission::SessionAdmission,
    scope::{TaskClass, TaskRegistrar},
    ChannelError, Impact, Reason, Stage,
};

pub(crate) async fn start<S>(
    stream: S,
    host: String,
    plan: zero_transport::http_navigation::Plan,
    tasks: TaskRegistrar,
    admission: Arc<SessionAdmission>,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let Some(prepared) = zero_transport::http_navigation::prepare(stream, host, plan) else {
        return;
    };
    let (delay, job) = prepared.into_parts();
    let cancel = tasks.cancellation();
    let registered = tasks.spawn(TaskClass::Auxiliary, async move {
        tokio::select! { biased;
            _ = cancel.cancelled() => Ok(()),
            _ = admission.cancelled() => Ok(()),
            result = job => result.map_err(|source| {
                ChannelError::new(Stage::Handshake, Reason::Io, Impact::Component).with_source(source)
            }),
        }
    });
    if let Err(error) = registered {
        // Registration failure drops the original carrier and its permit. The
        // caller still returns its original REALITY authentication rejection.
        tracing::debug!(%error, "REALITY navigation registration rejected");
        return;
    }
    // This camouflage delay is independent of the registered job's lifetime.
    tokio::time::sleep(delay).await;
}
