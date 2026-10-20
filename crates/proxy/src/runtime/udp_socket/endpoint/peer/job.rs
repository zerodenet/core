//! One actual peer task structurally drives both consumer and original I/O.
use super::*;
use zero_engine::EngineError;
#[derive(Clone)]
pub(crate) struct DirectPeerDriverSlot {
    sender: Arc<Mutex<Option<oneshot::Sender<DirectPeerDriver>>>>,
}
pub(crate) struct DirectPeerDriverJob(oneshot::Receiver<DirectPeerDriver>);
impl DirectPeerDriverSlot {
    pub(crate) fn channel() -> (Self, DirectPeerDriverJob) {
        let (sender, receiver) = oneshot::channel();
        (
            Self {
                sender: Arc::new(Mutex::new(Some(sender))),
            },
            DirectPeerDriverJob(receiver),
        )
    }
    /// A second job is refused. The caller has already installed its persistent
    /// resource owner and may not fall back to an ungranted legacy socket.
    pub(crate) fn submit(&self, driver: DirectPeerDriver) -> Result<(), ChannelError> {
        let sender = self
            .sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or_else(|| {
                error(
                    Stage::Prepare,
                    Reason::Capacity,
                    DeliveryState::NotSubmitted,
                )
            })?;
        sender
            .send(driver)
            .map_err(|_driver| error(Stage::Prepare, Reason::Revoked, DeliveryState::NotSubmitted))
    }
}
fn ordinary(result: Result<(), EngineError>) {
    if let Err(error) = result {
        tracing::debug!(%error, "UDP peer association ended");
    }
}
fn critical(source: ChannelError) -> EngineError {
    legacy(source)
}
pub(crate) async fn run_peer_with_driver<F>(
    consumer: F,
    mut job: DirectPeerDriverJob,
) -> Result<(), EngineError>
where
    F: Future<Output = Result<(), EngineError>> + Send,
{
    tokio::pin!(consumer);
    let mut driver = tokio::select! {
        result = &mut consumer => { ordinary(result); return Ok(()); },
        received = &mut job.0 => match received {
            Ok(driver) => driver,
            Err(_) => { ordinary(consumer.await); return Ok(()); },
        },
    };
    tokio::select! {
        result = &mut consumer => {
            ordinary(result);
            driver.stop.cancel();
            driver.await.map_err(critical)
        },
        result = &mut driver => {
            result.map_err(critical)?;
            ordinary(consumer.await);
            Ok(())
        },
    }
}
