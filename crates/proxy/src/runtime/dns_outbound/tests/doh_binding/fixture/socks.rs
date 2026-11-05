use super::*;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    task::JoinSet,
};
pub struct SocksPeer {
    pub endpoint: std::net::SocketAddr,
    pub counts: Arc<Counts>,
    pub task: Task,
    pub greeting: Arc<Notify>,
    pub reject_greeting: Arc<Notify>,
}
impl SocksPeer {
    pub async fn start(target: std::net::SocketAddr) -> Self {
        Self::with_paused_greeting(target, false).await
    }
    pub async fn with_paused_greeting(target: std::net::SocketAddr, paused: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        let counts = Arc::new(Counts::default());
        let events = counts.clone();
        let greeting = Arc::new(Notify::new());
        let observed = greeting.clone();
        let reject_greeting = Arc::new(Notify::new());
        let rejected = reject_greeting.clone();
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut peers = JoinSet::new();
            loop {
                tokio::select! {biased;
                    _=&mut stopped=>break,
                    accepted=listener.accept()=>{
                        let (io,_) = accepted?;
                        events.accepted.fetch_add(1,Ordering::AcqRel);
                        let events = events.clone();
                        let observed = observed.clone();
                        let rejected = rejected.clone();
                        peers.spawn(async move {let result=relay(io,target,paused,observed,rejected).await;events.record_close();terminal(result)});
                    },
                    joined=peers.join_next(),if !peers.is_empty()=>{joined.unwrap().map_err(io::Error::other)??;},
                }
            }
            drop(listener);
            while let Some(joined) = peers.join_next().await {
                joined.map_err(io::Error::other)??;
            }
            Ok(())
        });
        Self {
            endpoint,
            counts,
            task: Task { stop, task },
            greeting,
            reject_greeting,
        }
    }
}
async fn relay(
    mut client: TcpStream,
    target: std::net::SocketAddr,
    paused: bool,
    observed: Arc<Notify>,
    rejected: Arc<Notify>,
) -> io::Result<()> {
    let mut greeting = [0; 3];
    client.read_exact(&mut greeting).await?;
    assert_eq!(greeting, [5, 1, 0]);
    observed.notify_one();
    if paused {
        let mut bytes = [0];
        tokio::select! {biased;
            result = client.read_exact(&mut bytes) => {
                result?;
                return Err(io::Error::new(io::ErrorKind::InvalidData, "unexpected payload before SOCKS greeting reply"));
            }
            _ = rejected.notified() => {
                client.write_all(&[5, 0xff]).await?;
                return Ok(());
            }
        }
    }
    client.write_all(&[5, 0]).await?;
    let mut header = [0; 4];
    client.read_exact(&mut header).await?;
    assert_eq!(header, [5, 1, 0, 1]);
    let mut ip = [0; 4];
    client.read_exact(&mut ip).await?;
    assert_eq!(
        std::net::IpAddr::V4(std::net::Ipv4Addr::from(ip)),
        target.ip()
    );
    assert_eq!(client.read_u16().await?, target.port());
    let mut peer = TcpStream::connect(target).await?;
    client.write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 0]).await?;
    tokio::io::copy_bidirectional(&mut client, &mut peer).await?;
    Ok(())
}
