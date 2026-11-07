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

/// The ordinary wrapper's actual original owns this handoff. Explicit fixed
/// library factories keep their original Scope executor through start().
pub(crate) async fn start_owned<S>(
    stream: S,
    host: String,
    plan: zero_transport::http_navigation::Plan,
    tasks: TaskRegistrar,
    admission: Arc<SessionAdmission>,
    upstream: zero_platform_tokio::contract::network::UpstreamTransport,
    call: zero_platform_tokio::contract::network::CarrierContext,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    if !upstream.has_preparation_job_owner() {
        start(stream, host, plan, tasks, admission).await;
        return;
    }
    let grant = match upstream.reserve_preparation_job(&call) {
        Ok(grant) => grant,
        Err(source) => {
            tracing::debug!(%source,"original navigation admission rejected");
            return;
        }
    };
    stage(stream, host, plan, grant);
}
/// Same IO, actual original grant and unchanged60s/process safety bounds.
/// No task or native grant is created by this protocol-owned preparation.
pub(crate) fn stage<S>(
    stream: S,
    host: String,
    plan: zero_transport::http_navigation::Plan,
    grant: zero_platform_tokio::controlled_carrier::CarrierPreparationJob,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let Some(prepared) =
        zero_transport::http_navigation::prepare_owned(stream, host, plan, grant.clone())
    else {
        return;
    };
    let (delay, job) = prepared.into_parts();
    let work = Box::pin(async move {
        job.await.map_err(|source| {
            ChannelError::new(Stage::Handshake, Reason::Io, Impact::Component).with_source(source)
        })
    });
    if let Err(source) = grant.stage(delay, work) {
        tracing::debug!(%source,"same original navigation staging rejected");
    }
}
