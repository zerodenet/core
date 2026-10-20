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
    job: DirectPeerDriverJob,
) -> Result<(), EngineError>
where
    F: Future<Output = Result<(), EngineError>> + Send,
{
    let (consumer, driver) = run_pair(consumer, job).await;
    ordinary(consumer);
    driver.map_err(critical)
}
/// Raw-IP's original association result also drives its existing backoff and
/// retained-source ledger. Do not turn that actual failure into success.
pub(crate) async fn run_peer_preserving<F>(
    consumer: F,
    job: DirectPeerDriverJob,
) -> Result<(), EngineError>
where
    F: Future<Output = Result<(), EngineError>> + Send,
{
    match run_pair(consumer, job).await {
        (Ok(()), driver) => driver.map_err(critical),
        (Err(consumer), Ok(())) => Err(consumer),
        (Err(consumer), Err(driver)) => {
            Err(std::io::Error::other(PeerFailures { consumer, driver }).into())
        }
    }
}
#[derive(Debug)]
struct PeerFailures {
    consumer: EngineError,
    driver: ChannelError,
}
impl std::fmt::Display for PeerFailures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "UDP peer consumer failed: {}; original driver failed: {}",
            self.consumer, self.driver
        )
    }
}
impl std::error::Error for PeerFailures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.driver)
    }
}
async fn run_pair<F>(
    consumer: F,
    mut job: DirectPeerDriverJob,
) -> (Result<(), EngineError>, Result<(), ChannelError>)
where
    F: Future<Output = Result<(), EngineError>> + Send,
{
    tokio::pin!(consumer);
    let mut driver = tokio::select! {
        result = &mut consumer => return (result, Ok(())),
        received = &mut job.0 => match received {
            Ok(driver) => driver,
            Err(_) => return (consumer.await, Ok(())),
        },
    };
    tokio::select! {
        result = &mut consumer => {
            driver.stop.cancel();
            (result, driver.await)
        },
        result = &mut driver => {
            // The same task finishes both original futures. No child driver
            // escapes when either result first becomes ready.
            driver.stop.cancel();
            (consumer.await, result)
        },
    }
}
