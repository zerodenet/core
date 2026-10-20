//! Bounded peer tasks: reported results close inputs and join actual handles.
use super::*;
use tokio::task::{Id, JoinError};
const MAX_PEERS: usize = 4096;
#[derive(Debug)]
pub(super) struct PeerFailures {
    socket: Option<std::io::Error>,
    tasks: Vec<JoinError>,
}
impl std::fmt::Display for PeerFailures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} UDP peer task(s) failed", self.tasks.len())?;
        if let Some(error) = &self.socket {
            write!(f, "; UDP listener failed: {error}")?;
        }
        Ok(())
    }
}
impl std::error::Error for PeerFailures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.tasks
            .first()
            .map(|error| error as _)
            .or_else(|| self.socket.as_ref().map(|error| error as _))
    }
}
fn observe(
    result: Result<(Id, Result<(), EngineError>), JoinError>,
    failures: &mut Vec<JoinError>,
) {
    match result {
        Ok((_, Ok(()))) => {}
        Ok((_, Err(error))) => tracing::debug!(%error, "UDP peer association ended"),
        Err(error) => failures.push(error),
    }
}
pub(super) async fn run<F, Fut>(
    socket: Arc<UdpSocket>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    host: crate::runtime::inbound_owners::InboundTaskAccess,
    make: F,
) -> Result<(), EngineError>
where
    F: Fn(SocketAddr, mpsc::Receiver<Vec<u8>>) -> Fut + Send,
    Fut: Future<Output = Result<(), EngineError>> + Send + 'static,
{
    let mut peers: HashMap<SocketAddr, mpsc::Sender<Vec<u8>>> = HashMap::new();
    let mut tasks = JoinSet::new();
    let mut failures = Vec::new();
    let mut socket_error = None;
    let mut buffer = vec![0; 65535];
    loop {
        tokio::select! { biased;
            _ = shutdown.wait_for(|stop| *stop) => break,
            Some(result) = tasks.join_next_with_id() => {
                observe(result, &mut failures);
                if !failures.is_empty() { break; }
                peers.retain(|_, sender| !sender.is_closed());
            }
            recv = socket.recv_from(&mut buffer) => {
                let (size, peer) = match recv { Ok(received) => received, Err(error) => { socket_error = Some(error); break; } };
                peers.retain(|_, sender| !sender.is_closed());
                if !peers.contains_key(&peer) {
                    if peers.len() >= MAX_PEERS || tasks.len() >= MAX_PEERS { continue; }
                    let (sender, packets) = mpsc::channel(32);
                    tasks.spawn(make(peer, packets));
                    peers.insert(peer, sender);
                }
                let _ = peers[&peer].try_send(buffer[..size].to_vec());
            }
        }
    }
    host.request_stop();
    // EOF gives each actual association its normal cleanup path. This owner is
    // retained by the persistent outer Scope while any peer is still pending.
    peers.clear();
    while let Some(result) = tasks.join_next_with_id().await {
        observe(result, &mut failures);
    }
    if !failures.is_empty() || socket_error.is_some() {
        return Err(std::io::Error::other(PeerFailures {
            socket: socket_error,
            tasks: failures,
        })
        .into());
    }
    Ok(())
}
