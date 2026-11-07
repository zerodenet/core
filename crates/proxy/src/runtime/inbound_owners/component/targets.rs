//! Same accepted graph and actor, with persistent concrete Host native custody.
mod native;
use super::*;
use crate::protocol_registry::UpstreamConnectServices;
use zero_platform_tokio::contract::scope::TaskRegistrar;
use zero_platform_tokio::{
    AcceptedCaller, InboundTargetNativeOwner, InboundTargetSocketPool, TcpRelayStream,
};
use zero_traits::FallbackEndpoint;
#[derive(Clone)]
pub(super) struct Targets {
    pool: InboundTargetSocketPool,
    upstream: UpstreamConnectServices,
}
impl Targets {
    pub(super) fn new(
        declaration: &ResourceDeclaration,
        upstream: UpstreamConnectServices,
        instance: zero_core::component::ComponentInstanceId,
        tasks: &TaskRegistrar,
        root: &Arc<zero_platform_tokio::controlled_carrier::CarrierOriginalTotals>,
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
            pool: InboundTargetSocketPool::new(
                instance,
                tasks.retirement_observation(),
                root,
                *max_sockets,
                endpoints.clone().into(),
                cancel,
            ),
            upstream,
        })
    }
    pub(super) async fn open(
        &self,
        caller: AcceptedCaller,
        endpoint: FallbackEndpoint,
    ) -> Result<TcpRelayStream, ChannelError> {
        caller.check_open()?;
        let native = self
            .pool
            .reserve_graph(&caller.scope_identity(), &endpoint)?;
        let stop = native.cancellation();
        let _cancel_unsubmitted = stop.clone().drop_guard();
        let (admit, admitted) = tokio::sync::oneshot::channel::<()>();
        let (ready, received) = tokio::sync::oneshot::channel();
        let actual_tasks = Arc::new(std::sync::OnceLock::new());
        let installed = actual_tasks.clone();
        let grant = self.clone();
        let original = native.clone();
        let stopping = stop.clone();
        // This exact same single Critical actor is registered before IO. Its
        // gate prevents even native preparation until same-row custody is set.
        let registration=native.register_actor(&caller,move |tasks|{
            assert!(installed.set(tasks).is_ok());
            async move{
                if admitted.await.is_err(){return Ok(());}
                let result=tokio::select!{biased;_=stopping.cancelled()=>Err(std::io::Error::new(std::io::ErrorKind::Interrupted,"target preparation stopped")),result=grant.connect(endpoint,original.clone())=>result};
                match result{
                    Ok(stream)=>match original.wrap_stream(stream){
                        Ok(stream)=>{if ready.send(Ok(stream)).is_ok(){original.wait_view_returned().await;}},
                        Err(source)=>{let _=ready.send(Err(std::io::Error::other(source)));},
                    },
                    Err(source)=>{let _=ready.send(Err(source));},
                }
                Ok(())
            }
        });
        let _actual = registration?;
        let tasks = actual_tasks.get().ok_or_else(denied)?;
        let original = caller.retain_logical_resource_with_driver(
            erase_lifecycle(Owner {
                native: native.clone(),
            }),
            tasks,
        )?;
        // Everything above is synchronous. The already installed owner, exact
        // witness and original retained row precede this actor's first IO.
        admit.send(()).map_err(|_| denied())?;
        match received.await.map_err(|source| {
            ChannelError::new(Stage::Dial, Reason::Io, Impact::Session).with_source(source)
        })? {
            Ok(stream) => {
                _cancel_unsubmitted.disarm();
                Ok(stream)
            }
            Err(source) => {
                let work = Arc::new(source);
                match original
                    .close(Instant::now() + std::time::Duration::from_secs(5))
                    .await
                {
                    Ok(true) => Err(ChannelError::new(Stage::Dial, Reason::Io, Impact::Session)
                        .with_source(WorkError(work))),
                    Ok(false) => Err(cleanup_error(
                        work,
                        ChannelError::new(Stage::Close, Reason::Timeout, Impact::Component),
                    )),
                    Err(cleanup) => Err(cleanup_error(work, cleanup)),
                }
            }
        }
    }
    async fn connect(
        &self,
        endpoint: FallbackEndpoint,
        native: Arc<InboundTargetNativeOwner>,
    ) -> std::io::Result<TcpRelayStream> {
        let opener = native::Opener {
            native: native.clone(),
            observer: self.upstream.observer(),
        };
        let result = match endpoint {
            FallbackEndpoint::Tcp { server, port } => self
                .upstream
                .connect_inbound_target_using(&server, port, &opener)
                .await
                .map(TcpRelayStream::from_client)
                .map_err(std::io::Error::other),
            FallbackEndpoint::Unix { path } => opener.unix(&path).await,
        };
        let cleanup = native
            .close_cancelled_candidates(Instant::now() + std::time::Duration::from_secs(5))
            .await;
        match cleanup {
            Ok(()) => result,
            Err(cleanup) => match result {
                Ok(stream) => {
                    drop(stream);
                    Err(std::io::Error::other(cleanup))
                }
                Err(work) => Err(std::io::Error::other(cleanup_error(
                    Arc::new(work),
                    cleanup,
                ))),
            },
        }
    }
}
struct Owner {
    native: Arc<InboundTargetNativeOwner>,
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, _: Instant) -> Result<(), BuildError> {
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        self.native.cancellation().cancel();
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, BuildError> {
        self.native.close(deadline).await
    }
}

#[derive(Debug)]
struct WorkError(Arc<std::io::Error>);
impl std::fmt::Display for WorkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.0.as_ref(), f)
    }
}
impl std::error::Error for WorkError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
#[derive(Debug)]
struct WorkCleanup {
    work: Arc<std::io::Error>,
    cleanup: Arc<ChannelError>,
}
impl std::fmt::Display for WorkCleanup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "original target operation: {}; cleanup: {}",
            self.work, self.cleanup
        )
    }
}
impl std::error::Error for WorkCleanup {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.cleanup.as_ref())
    }
}
fn cleanup_error(work: Arc<std::io::Error>, cleanup: ChannelError) -> ChannelError {
    let cleanup = Arc::new(cleanup);
    ChannelError::retained(cleanup.clone()).with_source(WorkCleanup { work, cleanup })
}
