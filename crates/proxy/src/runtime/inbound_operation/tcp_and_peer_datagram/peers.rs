//! Bounded peer tasks: reported results close inputs and join actual handles.
use super::*;
use crate::runtime::inbound_owners::{InboundPeerTasks, MAX_DATAGRAM_PEERS};
pub(super) async fn run<F, Fut>(
    socket: Arc<UdpSocket>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    host: crate::runtime::inbound_owners::InboundTaskAccess,
    tasks: Arc<InboundPeerTasks>,
    make: F,
) -> Result<(), EngineError>
where
    F: Fn(SocketAddr, mpsc::Receiver<Vec<u8>>) -> Fut + Send,
    Fut: Future<Output = Result<(), EngineError>> + Send + 'static,
{
    let mut peers: HashMap<SocketAddr, mpsc::Sender<Vec<u8>>> = HashMap::new();
    let mut socket_error = None;
    let mut buffer = vec![0; 65535];
    loop {
        tokio::select! { biased;
            _ = shutdown.wait_for(|stop| *stop) => break,
            _ = tasks.changed() => {
                peers.retain(|_, sender| !sender.is_closed());
                if let Err(error) = tasks.retire_joined(tokio::time::Instant::now() + std::time::Duration::from_secs(5)).await {
                    socket_error = Some(std::io::Error::other(error));
                    break;
                }
            },
            recv = socket.recv_from(&mut buffer) => {
                let (size, peer) = match recv { Ok(received) => received, Err(error) => { socket_error = Some(error); break; } };
                peers.retain(|_, sender| !sender.is_closed());
                if !peers.contains_key(&peer) {
                    if peers.len() >= MAX_DATAGRAM_PEERS { continue; }
                    let (sender, packets) = mpsc::channel(32);
                    match tasks.spawn(&host, || make(peer, packets)) {
                        Ok(true) => { peers.insert(peer, sender); }
                        Ok(false) => continue,
                        Err(_) if host.cancellation().is_cancelled() => break,
                        Err(error) => { socket_error = Some(std::io::Error::other(error)); break; }
                    }
                }
                let _ = peers[&peer].try_send(buffer[..size].to_vec());
            }
        }
    }
    host.request_stop();
    // EOF gives each actual association its normal cleanup path. This owner is
    // retained by the persistent outer Scope while any peer is still pending.
    peers.clear();
    let joined = tasks.wait_closed().await;
    match (socket_error, joined) {
        (None, result) => result,
        (Some(error), Ok(())) => Err(error.into()),
        (Some(socket), Err(tasks)) => {
            Err(std::io::Error::other(PeerFailures { socket, tasks }).into())
        }
    }
}
#[derive(Debug)]
struct PeerFailures {
    socket: std::io::Error,
    tasks: EngineError,
}
impl std::fmt::Display for PeerFailures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "UDP listener failed: {}; peer task failed: {}",
            self.socket, self.tasks
        )
    }
}
impl std::error::Error for PeerFailures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.tasks)
    }
}
