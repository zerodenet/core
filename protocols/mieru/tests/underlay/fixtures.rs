use mieru::inbound::{multiplex::MieruInboundMultiplexer, MieruInboundProfile};
use std::{collections::HashMap, sync::Arc};
use tokio::{net::UdpSocket, sync::mpsc};
use zero_core::InboundRouteMultiplexer;
pub fn profile() -> MieruInboundProfile {
    MieruInboundProfile::from_config(vec![("u".into(), "p".into()), ("v".into(), "q".into())])
}
pub async fn echo(connection: MieruInboundMultiplexer) {
    let mut tasks = tokio::task::JoinSet::new();
    while let Ok(Some(mut stream)) = connection.accept_next().await {
        tasks.spawn(async move {
            let (mut read, mut write) = tokio::io::split(&mut stream);
            let _ = tokio::io::copy(&mut read, &mut write).await;
        });
    }
    tasks.shutdown().await;
}
pub async fn udp_server() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let addr = socket.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut peers = HashMap::<_, mpsc::Sender<Vec<u8>>>::new();
        let mut tasks = tokio::task::JoinSet::new();
        let mut buffer = [0; 1501];
        loop {
            tokio::select! {
                received=socket.recv_from(&mut buffer)=>{
                    let(n,peer)=received.unwrap();peers.retain(|_,tx|!tx.is_closed());
                    if let std::collections::hash_map::Entry::Vacant(entry) = peers.entry(peer) {
                        let(tx,packets)=mpsc::channel(64);entry.insert(tx);
                        let socket=socket.clone();tasks.spawn(async move {if let Ok(connection)=profile().accept_packet_peer(socket,peer,packets).await {echo(connection).await;}});
                    }
                    peers[&peer].send(buffer[..n].to_vec()).await.unwrap();
                }
                _=tasks.join_next(),if !tasks.is_empty()=>{}
            }
        }
    });
    (addr, task)
}
