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
}
impl SocksPeer {
    pub async fn start(target: std::net::SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        let counts = Arc::new(Counts::default());
        let events = counts.clone();
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
                        peers.spawn(async move {let result=relay(io,target).await;events.record_close();terminal(result)});
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
        }
    }
}
async fn relay(mut client: TcpStream, target: std::net::SocketAddr) -> io::Result<()> {
    let mut greeting = [0; 3];
    client.read_exact(&mut greeting).await?;
    assert_eq!(greeting, [5, 1, 0]);
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
