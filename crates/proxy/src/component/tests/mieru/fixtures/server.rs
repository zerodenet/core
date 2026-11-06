use super::*;
use tokio::net::{TcpListener, UdpSocket};
use zero_core::InboundRouteMultiplexer;
mod session;
pub(crate) struct Server {
    pub address: SocketAddr,
    pub accepts: Arc<AtomicUsize>,
    stop: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}
fn profile() -> mieru::inbound::MieruInboundProfile {
    mieru::inbound::MieruInboundProfile::from_config(vec![("u".into(), "p".into())])
}
impl Server {
    pub async fn new(forward: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let stop = CancellationToken::new();
        let cancel = stop.clone();
        let accepts = Arc::new(AtomicUsize::new(0));
        let count = accepts.clone();
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let (socket, _) = tokio::select! {biased;_=cancel.cancelled()=>break,result=listener.accept()=>result.unwrap()};
                count.fetch_add(1, Ordering::SeqCst);
                let cancel = cancel.clone();
                connections.spawn(async move {
                    let connection = profile()
                        .accept_multiplexer(zero_platform_tokio::TcpRelayStream::new(socket))
                        .await
                        .unwrap();
                    serve(connection, cancel, forward).await;
                });
            }
            while let Some(result) = connections.join_next().await {
                result.unwrap();
            }
        });
        Self {
            address,
            accepts,
            stop,
            task,
        }
    }
    pub async fn packet(forward: bool) -> Self {
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let address = socket.local_addr().unwrap();
        let stop = CancellationToken::new();
        let cancel = stop.clone();
        let accepts = Arc::new(AtomicUsize::new(0));
        let count = accepts.clone();
        let task = tokio::spawn(async move {
            let mut peers =
                std::collections::HashMap::<SocketAddr, tokio::sync::mpsc::Sender<Vec<u8>>>::new();
            let mut connections = tokio::task::JoinSet::new();
            let mut bytes = vec![0; 65536];
            loop {
                let (size, peer) = tokio::select! {biased;_=cancel.cancelled()=>break,result=socket.recv_from(&mut bytes)=>result.unwrap()};
                if peers.get(&peer).is_some_and(|sender| sender.is_closed()) {
                    peers.remove(&peer);
                }
                let sender=peers.entry(peer).or_insert_with(|| {
                    let (sender,receiver)=tokio::sync::mpsc::channel(64);let socket=socket.clone();let cancel=cancel.clone();count.fetch_add(1,Ordering::SeqCst);
                    connections.spawn(async move {
                        let profile=profile();
                        let connection=tokio::select! {biased;_=cancel.cancelled()=>return,result=profile.accept_packet_peer(socket,peer,receiver)=>result.unwrap()};
                        eprintln!("Mieru packet fixture authenticated actual UDP peer {peer}");
                        serve(connection,cancel,forward).await;
                    });sender
                });
                let _ = sender.send(bytes[..size].to_vec()).await;
            }
            while let Some(result) = connections.join_next().await {
                result.unwrap();
            }
        });
        Self {
            address,
            accepts,
            stop,
            task,
        }
    }
    pub async fn close(self) {
        self.stop.cancel();
        self.task.await.unwrap();
    }
}
async fn serve(
    connection: mieru::inbound::multiplex::MieruInboundMultiplexer,
    cancel: CancellationToken,
    forward: bool,
) {
    let mut streams = tokio::task::JoinSet::new();
    loop {
        let stream = tokio::select! {biased;_=cancel.cancelled()=>break,result=connection.accept_next()=>match result {
            Ok(Some(stream))=>stream,
            Ok(None)=>break,
            Err(source)=>{eprintln!("Mieru fixture actual logical receive ended: {source}");break;}
        }};
        if forward {
            eprintln!(
                "Mieru fixture received actual logical stream; reading CONNECT and writing its ACK"
            );
        }
        let route = connection.accept_route(stream).await.unwrap();
        if forward {
            eprintln!("Mieru fixture actual CONNECT parsed and ACK flushed");
        }
        let cancel = cancel.clone();
        streams.spawn(async move {
            tokio::select! {biased;_=cancel.cancelled()=>{},_=session::run(route,forward)=>{}}
        });
    }
    connection.close("fixture stopped");
    while let Some(result) = streams.join_next().await {
        result.unwrap();
    }
}
