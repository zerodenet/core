//! The original native control is installed before the first connect await.
use super::*;
use crate::transport::{CandidateOpenFailure, CandidateOpened, CandidateOpener, DirectCandidate};
use zero_platform_tokio::{ControlledTcpStream, EgressInterfaceControl, EgressSelection};
pub(super) struct Opener {
    pub native: Arc<InboundTargetNativeOwner>,
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
            let (mut pending, index) = self
                .native
                .prepare_tcp(remote, policy, selection, self.observer.clone())
                .await
                .map_err(|source| failed(std::io::Error::other(source)))?;
            let io = match pending
                .connect(Instant::now() + std::time::Duration::from_secs(10))
                .await
            {
                Ok(io) => io,
                Err(source) => {
                    let cleanup = self
                        .native
                        .close_candidate(index, Instant::now() + std::time::Duration::from_secs(5))
                        .await;
                    let cleanup = match cleanup {
                        Ok(true) => None,
                        Ok(false) => Some(ChannelError::new(
                            Stage::Close,
                            Reason::Timeout,
                            Impact::Component,
                        )),
                        Err(cleanup) => Some(cleanup),
                    };
                    if let Some(cleanup) = cleanup {
                        return Err(failed(std::io::Error::other(ConnectCleanupFailure {
                            connect: source,
                            cleanup: Arc::new(cleanup),
                        })));
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
            let (mut pending, _index) = self.native.prepare_unix(path).await?;
            pending
                .connect(Instant::now() + std::time::Duration::from_secs(10))
                .await
                .map(TcpRelayStream::from_client)
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

#[derive(Debug)]
struct ConnectCleanupFailure {
    connect: Arc<zero_platform_tokio::TcpConnectError>,
    cleanup: Arc<ChannelError>,
}
impl std::fmt::Display for ConnectCleanupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "original target connection failed: {}; cleanup: {}",
            self.connect, self.cleanup
        )
    }
}
impl std::error::Error for ConnectCleanupFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.cleanup.as_ref())
    }
}
