//! Explicit same-original platform providers; no raw File or socket fallback.
use super::*;
use zero_platform_tokio::contract::{
    lifecycle::ChannelControl, scope::TaskRegistrar, CancellationToken,
};
#[cfg(windows)]
use zero_platform_tokio::{ControlledPipe, ControlledPipeOwner};
#[cfg(unix)]
use zero_platform_tokio::{ControlledTcpControl, ControlledTcpStream, PendingUnixConnect};
#[cfg(unix)]
pub(super) type Stream = ControlledTcpStream;
#[cfg(windows)]
pub(super) type Stream = ControlledPipe;
pub(super) enum Native {
    Cold,
    /// A synchronous creation that did not return is not KnownNoNative.
    Creating,
    #[cfg(unix)]
    Unix(ControlledTcpControl),
    #[cfg(windows)]
    Pipe(ControlledPipeOwner),
}
pub(super) enum Opening {
    #[cfg(unix)]
    Unix(PendingUnixConnect),
    #[cfg(windows)]
    Pipe(ControlledPipeOwner),
}
impl Native {
    pub(super) fn prepare(
        &mut self,
        path: &str,
        tasks: TaskRegistrar,
        stop: CancellationToken,
    ) -> io::Result<Opening> {
        if !matches!(self, Self::Cold) {
            return Err(io::Error::other("client original already installed"));
        }
        *self = Self::Creating;
        #[cfg(unix)]
        {
            let _ = tasks;
            let result = PendingUnixConnect::prepare(path, stop);
            match result {
                Ok((pending, control)) => {
                    *self = Self::Unix(control);
                    Ok(Opening::Unix(pending))
                }
                Err(error) => {
                    *self = Self::Cold;
                    Err(error)
                }
            }
        }
        #[cfg(windows)]
        {
            let _ = stop;
            let original = ControlledPipeOwner::new_client(path.into(), tasks);
            *self = Self::Pipe(original.clone());
            Ok(Opening::Pipe(original))
        }
    }
    pub(super) async fn close(
        &mut self,
        end: tokio::time::Instant,
        errors: &owner::Faults,
    ) -> io::Result<bool> {
        match self {
            Self::Cold => Ok(true),
            Self::Creating => Ok(false),
            #[cfg(unix)]
            Self::Unix(control) => {
                let report = match control.wait_closed(end).await {
                    Ok(report) => report,
                    Err(error) => {
                        // The real receiver keeps the exact source before accepting
                        // its receipt; this never claims native close.
                        let receipt = error.cleanup_failure_receipt().cloned();
                        let stored = errors.native(error);
                        if let Some(receipt) = receipt {
                            assert!(receipt.acknowledge_retained(&stored));
                        }
                        return Ok(false);
                    }
                };
                Ok(report.is_complete())
            }
            #[cfg(windows)]
            Self::Pipe(control) => {
                let closed = control.close(end).await?;
                Ok(control.matches_closed(&closed))
            }
        }
    }
}
impl Opening {
    pub(super) async fn connect(self) -> io::Result<Stream> {
        match self {
            #[cfg(unix)]
            Self::Unix(mut pending) => pending.connect_original().await,
            #[cfg(windows)]
            Self::Pipe(original) => original.open().await,
        }
    }
}
