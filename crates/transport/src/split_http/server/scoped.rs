//! HTTP/1 and H2 use the original accepted caller for every connection/body task.
use super::super::body::Caller;
use super::*;
use std::{future::Future, sync::Mutex};
use zero_platform_tokio::{
    contract::{scope::TaskClass, CancellationToken, ChannelError},
    AcceptedCaller,
};

pub(in crate::split_http) struct Stop(pub CancellationToken);
impl Drop for Stop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
#[derive(Clone)]
struct Executor {
    caller: AcceptedCaller,
    stopped: CancellationToken,
    failure: Arc<Mutex<Option<ChannelError>>>,
}
impl<F> hyper::rt::Executor<F> for Executor
where
    F: Future + Send + 'static,
    F::Output: Send,
{
    fn execute(&self, work: F) {
        let stopped = self.stopped.clone();
        if let Err(error) = self
            .caller
            .spawn(TaskClass::Auxiliary, move |_| async move {
                tokio::select! { biased; _=stopped.cancelled()=>{}, _=work=>{} }
                Ok(())
            })
        {
            let mut failure = self.failure.lock().unwrap_or_else(|e| e.into_inner());
            if failure.is_none() {
                *failure = Some(error);
            }
            self.stopped.cancel();
        }
    }
}

pub fn accept<S, P>(
    stream: S,
    config: &P,
    registry: &SplitHttpRegistry,
    caller: &AcceptedCaller,
) -> Result<XhttpIncoming, crate::RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    P: SplitHttpTransportProfile + ?Sized,
{
    let profile = Profile::new(config);
    let registry = registry.clone();
    let (sender, receiver) = mpsc::channel(64);
    let cancel = CancellationToken::new();
    let stopped = cancel.clone();
    let executor = Executor {
        caller: caller.clone(),
        stopped: cancel.clone(),
        failure: Arc::new(Mutex::new(None)),
    };
    caller
        .spawn(TaskClass::Auxiliary, move |_| async move {
            drive(stream, profile, registry, sender, executor, stopped).await
        })
        .map_err(|error| crate::RuntimeError::Io(io::Error::other(error)))?;
    Ok(XhttpIncoming {
        receiver: tokio::sync::Mutex::new(receiver),
        task: None,
        scoped: Some(Arc::new(Stop(cancel))),
    })
}
async fn drive<S>(
    stream: S,
    profile: Profile,
    registry: SplitHttpRegistry,
    sender: mpsc::Sender<XhttpStream>,
    executor: Executor,
    cancel: CancellationToken,
) -> Result<(), ChannelError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let max_header_bytes = profile.options.server_max_header_bytes;
    let policy = http1::ReplyPolicy::default();
    let stream = http1::UploadDrain::new(stream, policy.clone());
    let caller = Caller(executor.caller.clone());
    let service =
        hyper::service::service_fn(move |mut request: http::Request<hyper::body::Incoming>| {
            policy.set_packet(false);
            request.extensions_mut().insert(policy.clone());
            request.extensions_mut().insert(caller.clone());
            let profile = profile.clone();
            let sessions = registry.sessions.clone();
            let sender = sender.clone();
            let permit = registry.requests.clone().try_acquire_owned();
            async move {
                let response = match permit {
                    Ok(permit) => exchange::handle(request, profile, sessions, sender, permit)
                        .await
                        .unwrap_or_else(|error| {
                            tracing::debug!(%error,"XHTTP request rejected");
                            status(400)
                        }),
                    Err(_) => status(503),
                };
                Ok::<_, io::Error>(response)
            }
        });
    let mut builder = hyper_util::server::conn::auto::Builder::new(executor.clone());
    builder
        .http1()
        .half_close(true)
        .max_buf_size((max_header_bytes as usize).max(8192))
        .timer(TokioTimer::new())
        .header_read_timeout(Duration::from_secs(30));
    builder
        .http2()
        .max_concurrent_streams(64)
        .max_header_list_size(max_header_bytes)
        .timer(TokioTimer::new());
    tokio::select! { biased;
        _=cancel.cancelled()=>{},
        result=builder.serve_connection(TokioIo::new(stream),service)=> {
            if let Err(error)=result { tracing::debug!(?error,"scoped XHTTP connection ended"); }
        }
    }
    // Stopping a source requests cancellation only; actual tasks and native
    // originals stay in the host's existing accepted ledger until joined/closed.
    if let Some(error) = executor
        .failure
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .take()
    {
        return Err(error);
    }
    Ok(())
}
