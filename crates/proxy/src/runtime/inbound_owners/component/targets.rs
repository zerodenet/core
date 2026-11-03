//! Native service originals use the same accepted graph and original host Scope.
mod native;
use super::*;
use crate::protocol_registry::UpstreamConnectServices;
use tokio::sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore};
use zero_platform_tokio::contract::lifecycle::ChannelControl;
use zero_platform_tokio::contract::lifecycle::TaskClass;
use zero_platform_tokio::{AcceptedCaller, ControlledTcpControl, TcpRelayStream};
use zero_traits::FallbackEndpoint;
struct State {
    originals: Vec<(ControlledTcpControl, OwnedSemaphorePermit)>,
    receipts: CloseReport,
    pending: std::collections::BTreeMap<
        usize,
        Vec<zero_platform_tokio::contract::lifecycle::Unfinished>,
    >,
    driver: Option<zero_platform_tokio::contract::scope::TaskJoinWitness>,
    view_issued: bool,
    fatal: Option<Arc<ChannelError>>,
}
#[derive(Clone)]
pub(super) struct Targets {
    endpoints: Arc<[FallbackEndpoint]>,
    permits: Arc<Semaphore>,
    upstream: UpstreamConnectServices,
    scope: zero_platform_tokio::contract::scope::TaskScopeIdentity,
    cancel: CancellationToken,
}
impl Targets {
    pub(super) fn new(
        declaration: &ResourceDeclaration,
        upstream: UpstreamConnectServices,
        scope: zero_platform_tokio::contract::scope::TaskScopeIdentity,
        cancel: CancellationToken,
    ) -> Result<Self, EngineError> {
        let ResourceDeclaration::InboundTargets {
            endpoints,
            max_sockets,
        } = declaration
        else {
            return Err(std::io::Error::other(denied()).into());
        };
        Ok(Self {
            endpoints: endpoints.clone().into(),
            permits: Arc::new(Semaphore::new(max_sockets.get())),
            upstream,
            scope,
            cancel,
        })
    }
    pub(super) async fn open(
        &self,
        caller: AcceptedCaller,
        endpoint: FallbackEndpoint,
    ) -> Result<TcpRelayStream, ChannelError> {
        caller.check_open()?;
        if !self.scope.same_scope(&caller.scope_identity()) || !self.endpoints.contains(&endpoint) {
            return Err(denied());
        }
        let state = Arc::new(AsyncMutex::new(State {
            originals: Vec::new(),
            receipts: CloseReport::default(),
            pending: Default::default(),
            driver: None,
            view_issued: false,
            fatal: None,
        }));
        let stop = self.cancel.child_token();
        let returned = CancellationToken::new();
        let owner = Owner {
            state: state.clone(),
            stop: stop.clone(),
            returned: returned.clone(),
        };
        let original = Arc::new(caller.retain_logical_resource(erase_lifecycle(owner))?);
        let retained = original.clone();
        let grant = self.clone();
        let (ready, received) = tokio::sync::oneshot::channel();
        let stopping = stop.clone();
        let _cancel_unsubmitted = stop.clone().drop_guard();
        caller.spawn(TaskClass::Critical, move |tasks| {
            let binding = retained.bind_driver(&tasks);
            async move {
                binding?;
                state.lock().await.driver = tasks.caller()?.task_join_witness();
                let opening = grant.connect(endpoint, state.clone(), stopping.clone());
                let result = tokio::select! { biased; _=stopping.cancelled()=>Err(std::io::Error::new(std::io::ErrorKind::Interrupted,"target preparation stopped")), result=opening=>result };
                match result {
                    Ok(stream) => {
                        state.lock().await.view_issued=true;
                        let io = TcpRelayStream::new(Returned { stream, returned: returned.clone() });
                        if ready.send(Ok(io)).is_ok() {
                            // Stop wakes the controlled I/O; it cannot substitute
                            // for the unique view's actual return or this join.
                            returned.cancelled().await;
                        }
                    }
                    Err(error) => { let _=ready.send(Err(error)); }
                }
                Ok(())
            }
        })?;
        let result = received.await.map_err(|source| {
            ChannelError::new(Stage::Dial, Reason::Io, Impact::Session).with_source(source)
        })?;
        match result {
            Ok(stream) => {
                _cancel_unsubmitted.disarm();
                Ok(stream)
            }
            Err(source) => {
                // Complete failed originals before returning/retrying, using
                // the actual driver join and original native close provider.
                let closed = original
                    .close(Instant::now() + std::time::Duration::from_secs(5))
                    .await?;
                if !closed {
                    return Err(ChannelError::new(
                        Stage::Close,
                        Reason::Timeout,
                        Impact::Component,
                    ));
                }
                Err(ChannelError::new(Stage::Dial, Reason::Io, Impact::Session).with_source(source))
            }
        }
    }
    async fn connect(
        &self,
        endpoint: FallbackEndpoint,
        state: Arc<AsyncMutex<State>>,
        stop: CancellationToken,
    ) -> std::io::Result<TcpRelayStream> {
        let opener = native::Opener {
            state,
            stop,
            permits: self.permits.clone(),
            observer: self.upstream.observer(),
        };
        let result = match endpoint {
            FallbackEndpoint::Tcp { server, port } => self
                .upstream
                .connect_inbound_target_using(&server, port, &opener)
                .await
                .map(TcpRelayStream::new)
                .map_err(std::io::Error::other),
            FallbackEndpoint::Unix { path } => opener.unix(&path).await,
        };
        let mut state = opener.state.lock().await;
        for index in 0..state.originals.len() {
            if state.originals[index].0.is_cancelled() {
                let report = state.originals[index]
                    .0
                    .wait_closed(Instant::now() + std::time::Duration::from_secs(5))
                    .await
                    .map_err(std::io::Error::other)?;
                let complete = report.is_complete() && report.failed_tasks.is_empty();
                state.collect(index, report);
                if !complete {
                    return Err(std::io::Error::other(
                        "losing original target remains unconfirmed",
                    ));
                }
            }
        }
        result
    }
}
struct Owner {
    state: Arc<AsyncMutex<State>>,
    stop: CancellationToken,
    returned: CancellationToken,
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, _: Instant) -> Result<(), BuildError> {
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        self.stop.cancel();
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, BuildError> {
        self.stop.cancel();
        let mut state = tokio::time::timeout_at(deadline, self.state.lock())
            .await
            .map_err(|source| {
                ChannelError::new(Stage::Close, Reason::Timeout, Impact::Component)
                    .with_source(source)
            })?;
        if state
            .driver
            .as_ref()
            .is_some_and(|driver| !driver.is_joined())
        {
            return Ok(CloseReport {
                unfinished: vec![
                    zero_platform_tokio::contract::lifecycle::Unfinished::Resource(
                        "original target driver join remains pending".into(),
                    ),
                ],
                ..Default::default()
            });
        }
        if state.view_issued && !self.returned.is_cancelled() {
            return Ok(CloseReport {
                unfinished: vec![
                    zero_platform_tokio::contract::lifecycle::Unfinished::Resource(
                        "original target view has not returned".into(),
                    ),
                ],
                ..Default::default()
            });
        }
        for (control, _) in &mut state.originals {
            control.cancel(CloseReason::Shutdown);
        }
        for index in 0..state.originals.len() {
            let report = state.originals[index].0.wait_closed(deadline).await?;
            state.collect(index, report);
        }
        if let Some(error) = &state.fatal {
            return Err(crate::component::instance::retained(error.clone()));
        }
        if state.pending.is_empty() && state.receipts.failed_tasks.is_empty() {
            state.originals.clear();
        }
        let mut report = std::mem::take(&mut state.receipts);
        report.unfinished = state.pending.values().flatten().cloned().collect();
        Ok(report)
    }
}
struct Returned {
    stream: TcpRelayStream,
    returned: CancellationToken,
}
impl Drop for Returned {
    fn drop(&mut self) {
        self.returned.cancel();
    }
}
impl tokio::io::AsyncRead for Returned {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_read(cx, buffer)
    }
}
impl tokio::io::AsyncWrite for Returned {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.stream).poll_write(cx, bytes)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}
impl zero_platform_tokio::ClientStream for Returned {
    fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.stream.local_addr()
    }
    fn peer_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.stream.peer_addr()
    }
}

impl State {
    fn collect(&mut self, index: usize, mut report: CloseReport) {
        let pending = std::mem::take(&mut report.unfinished);
        if pending.is_empty() {
            self.pending.remove(&index);
        } else {
            self.pending.insert(index, pending);
        }
        self.receipts.append(report);
    }
}

impl zero_traits::AsyncSocket for Returned {
    type Error = std::io::Error;
    async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        tokio::io::AsyncReadExt::read(self, buf).await
    }
    async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        tokio::io::AsyncWriteExt::write_all(self, buf).await
    }
    async fn shutdown(&mut self) -> std::io::Result<()> {
        tokio::io::AsyncWriteExt::shutdown(self).await
    }
}
