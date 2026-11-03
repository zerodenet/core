//! The original native control is installed before the first connect await.
use super::*;
use crate::transport::{CandidateOpenFailure, CandidateOpened, CandidateOpener, DirectCandidate};
use zero_platform_tokio::{
    ControlledTcpStream, EgressInterfaceControl, EgressSelection, PendingTcpConnect,
};
pub(super) struct Opener {
    pub state: Arc<AsyncMutex<State>>,
    pub stop: CancellationToken,
    pub permits: Arc<Semaphore>,
    pub observer: Option<Arc<dyn zero_traits::IoObserver>>,
}
impl CandidateOpener for Opener {
    type Socket = ControlledTcpStream;
    fn open<'a>(
        &'a self,
        candidate: DirectCandidate,
        _egress: &'a EgressInterfaceControl,
        selection: &'a EgressSelection,
        policy: &'a zero_traits::DialPolicy,
    ) -> BoxFuture<'a, Result<CandidateOpened<Self::Socket>, CandidateOpenFailure>> {
        Box::pin(async move {
            let remote = candidate.peer();
            let failed = |source: std::io::Error| CandidateOpenFailure {
                operation: "connect_socket",
                interface_bound: false,
                local: None,
                os_error: source.raw_os_error(),
                source,
            };
            if candidate.generation().0 != _egress.generation() {
                return Err(failed(std::io::Error::other(
                    "target network generation changed",
                )));
            }
            let permit = self
                .permits
                .clone()
                .try_acquire_owned()
                .map_err(|source| failed(std::io::Error::other(source)))?;
            let mut state = self.state.lock().await;
            if self.stop.is_cancelled() {
                return Err(failed(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "target owner stopped",
                )));
            }
            state
                .originals
                .try_reserve(1)
                .map_err(|source| failed(std::io::Error::other(source)))?;
            let (mut pending, control) = PendingTcpConnect::prepare(
                remote,
                policy,
                selection,
                self.stop.clone(),
                self.observer.clone(),
            )
            .map_err(|source| {
                let stage = source.stage();
                let source = Arc::new(source);
                if !matches!(stage, "validate_dial_policy" | "create_socket") {
                    state.fatal = Some(Arc::new(
                        ChannelError::new(Stage::Prepare, Reason::Io, Impact::Component)
                            .with_source(source.clone()),
                    ));
                }
                failed(std::io::Error::other(source))
            })?;
            let index = state.originals.len();
            state.originals.push((control, permit));
            drop(state);
            let io = match pending
                .connect(Instant::now() + std::time::Duration::from_secs(10))
                .await
            {
                Ok(io) => io,
                Err(source) => {
                    let mut state = self.state.lock().await;
                    let report = state.originals[index]
                        .0
                        .wait_closed(Instant::now() + std::time::Duration::from_secs(5))
                        .await
                        .map_err(|error| {
                            self.stop.cancel();
                            failed(std::io::Error::other(error))
                        })?;
                    let complete = report.is_complete() && report.failed_tasks.is_empty();
                    state.collect(index, report);
                    if !complete {
                        self.stop.cancel();
                        return Err(failed(std::io::Error::other(
                            "original failed target remains unconfirmed",
                        )));
                    }
                    return Err(failed(std::io::Error::other(source)));
                }
            };
            if candidate.generation().0 != _egress.generation() {
                drop(io);
                return Err(failed(std::io::Error::other(
                    "target network generation changed",
                )));
            }
            let local = io.local_addr().map_err(failed)?;
            Ok(CandidateOpened { io, local, remote })
        })
    }
}
impl Opener {
    pub(super) async fn unix(&self, path: &str) -> std::io::Result<TcpRelayStream> {
        #[cfg(unix)]
        {
            let permit = self
                .permits
                .clone()
                .try_acquire_owned()
                .map_err(std::io::Error::other)?;
            let mut state = self.state.lock().await;
            if self.stop.is_cancelled() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "target owner stopped",
                ));
            }
            state
                .originals
                .try_reserve(1)
                .map_err(std::io::Error::other)?;
            let (mut pending, control) =
                zero_platform_tokio::PendingUnixConnect::prepare(path, self.stop.clone())?;
            state.originals.push((control, permit));
            drop(state);
            pending
                .connect(Instant::now() + std::time::Duration::from_secs(10))
                .await
                .map(TcpRelayStream::new)
        }
        #[cfg(not(unix))]
        {
            let _ = path;
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Unix targets require Unix",
            ))
        }
    }
}
