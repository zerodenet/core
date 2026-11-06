//! Consume the real first CONNECT bytes but withhold its protocol ACK.
use super::*;
use zero_core::InboundRouteMultiplexer;
pub(super) struct PacketPeer {
    pub(super) address: std::net::SocketAddr,
    pub(super) accepts: Arc<std::sync::atomic::AtomicUsize>,
    pub(super) waiting: tokio::sync::oneshot::Receiver<()>,
    stop: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}
impl PacketPeer {
    pub(super) async fn new() -> Self {
        let socket = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let address = socket.local_addr().unwrap();
        let stop = CancellationToken::new();
        let stopped = stop.clone();
        let (waiting_tx, waiting) = tokio::sync::oneshot::channel();
        let accepts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = accepts.clone();
        let task = tokio::spawn(async move {
            let mut bytes = vec![0; 65536];
            let (size, peer) = socket.recv_from(&mut bytes).await.unwrap();
            let (packets, receive) = tokio::sync::mpsc::channel(64);
            packets.send(bytes[..size].to_vec()).await.unwrap();
            let shared = socket.clone();
            let stopping = stopped.clone();
            let serve = async move {
                let profile = mieru::inbound::MieruInboundProfile::from_config(vec![(
                    "u".into(),
                    "p".into(),
                )]);
                let connection = tokio::select! {biased;_=stopping.cancelled()=>return,connection=profile.accept_packet_peer(shared,peer,receive)=>connection.unwrap()};
                count.fetch_add(1, Ordering::SeqCst);
                let mut first = connection.accept_next().await.unwrap().unwrap();
                let mut connect = [0; 10];
                first.read_exact(&mut connect).await.unwrap();
                assert_eq!(&connect[..4], &[5, 1, 0, 1]);
                waiting_tx.send(()).unwrap();
                let mut jobs = tokio::task::JoinSet::new();
                loop {
                    let next = tokio::select! {biased;_=stopping.cancelled()=>break,next=connection.accept_next()=>next};
                    let Some(next) = next.unwrap() else {
                        break;
                    };
                    let route = connection.accept_route(next).await.unwrap();
                    let mieru::inbound::MieruInboundAcceptedSession::Tcp {
                        session,
                        mut stream,
                    } = route
                    else {
                        panic!("actual DNS Stream over the UDP underlay")
                    };
                    let zero_core::Address::Ipv4(ip) = session.target else {
                        panic!("actual literal DNS fixture destination")
                    };
                    let stopping = stopping.clone();
                    jobs.spawn(async move {
                        let mut peer=tokio::net::TcpStream::connect((std::net::Ipv4Addr::from(ip),session.port)).await.unwrap();
                        tokio::select! {biased;_=stopping.cancelled()=>{},_=tokio::io::copy_bidirectional(&mut stream,&mut peer)=>{}}
                    });
                }
                drop(first);
                connection.close("fixture stopped");
                while let Some(joined) = jobs.join_next().await {
                    joined.unwrap();
                }
            };
            tokio::pin!(serve);
            loop {
                tokio::select! {biased;
                    _=stopped.cancelled()=>break,
                    _=&mut serve=>return,
                    received=socket.recv_from(&mut bytes)=>{
                        let (size,source)=received.unwrap();assert_eq!(source,peer,"same actual SOCKS UDP original is reused");
                        packets.send(bytes[..size].to_vec()).await.unwrap();
                    }
                }
            }
            serve.await;
        });
        Self {
            address,
            accepts,
            waiting,
            stop,
            task,
        }
    }
    pub(super) async fn close(self) {
        self.stop.cancel();
        tokio::time::timeout_at(deadline(), self.task)
            .await
            .unwrap()
            .unwrap();
    }
}
