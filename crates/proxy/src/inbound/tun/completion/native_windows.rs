//! Windows joins channel tasks, returns their unique view, then joins the
//! original native reader and consumes the same Session/Adapter receipts.
use super::*;
impl TunCompletion {
    pub(in crate::inbound::tun) async fn wait_native(&self, deadline: Instant) -> io::Result<()> {
        if self.main.lock().await.task.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "TUN main original task has not joined",
            ));
        }
        let channels = self.native.lock().unwrap().clone();
        let original = self.acquisition.lock().unwrap().clone();
        timeout_at(deadline, async {
            let io = match channels {
                Some(channels) => channels.return_io_view().await,
                None => Ok(()),
            };
            let native = match original {
                Some(original) => original
                    .close()
                    .await
                    .map(|_| ())
                    .map_err(|source| io::Error::other(NativeSource(source))),
                None => Ok(()),
            };
            join_results(io, native)
        })
        .await
        .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?
    }
    pub(super) async fn windows_resources_complete(&self) -> bool {
        let channels = self.native.lock().unwrap().clone();
        if let Some(channels) = channels {
            if !channels.io_complete().await {
                return false;
            }
        }
        self.acquisition
            .lock()
            .unwrap()
            .as_ref()
            .is_none_or(|original| {
                matches!(
                    original.outcome(),
                    zero_tun::WintunAcquisitionOutcome::KnownNoNative
                        | zero_tun::WintunAcquisitionOutcome::Closed
                )
            })
    }
}
#[derive(Debug)]
struct NativeSource(Arc<io::Error>);
impl std::fmt::Display for NativeSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for NativeSource {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
