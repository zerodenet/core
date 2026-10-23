use crate::{protocol_registry::TcpRuntimeServices, Proxy};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinSet,
};
use zero_core::{Address, Network, ProtocolType, Session};

pub(super) async fn setup(count: usize) -> (Proxy, Session, TcpRuntimeServices, Vec<TcpListener>) {
    let mut listeners = Vec::new();
    let mut outbounds = Vec::new();
    let mut tags = Vec::new();
    for index in 0..count {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let tag = format!("node-{index}");
        outbounds.push(serde_json::json!({"tag":tag,"protocol":{"type":"socks5","server":"127.0.0.1","port":port}}));
        tags.push(tag);
        listeners.push(listener);
    }
    let config = serde_json::json!({"outbounds":outbounds,"outbound_groups":[{"tag":"relay","type":"relay","proxies":tags}],"route":{"final":{"type":"route","outbound":"relay"}}});
    let proxy =
        Proxy::new(zero_config::RuntimeConfig::parse(&config.to_string()).unwrap()).unwrap();
    let config = proxy.engine().runtime_snapshot().config().clone();
    proxy
        .protocols
        .start_components(
            config.clone(),
            tokio::time::Instant::now() + std::time::Duration::from_secs(3),
        )
        .await
        .unwrap();
    proxy
        .protocols
        .commit_components(&config, || Ok(()))
        .unwrap();
    let mut session = Session::new(
        0,
        Address::Ipv4([203, 0, 113, 9]),
        443,
        Network::Tcp,
        ProtocolType::new("test"),
    );
    proxy
        .engine()
        .prepare_session(&mut session, "incoming")
        .unwrap();
    assert_ne!(session.id, 0);
    let services = proxy
        .tcp_runtime_services()
        .for_inbound("incoming".into())
        .with_business_identity(&session)
        .unwrap();
    (proxy, session, services, listeners)
}

pub(super) async fn greeting(io: &mut TcpStream) {
    let mut greeting = [0; 3];
    io.read_exact(&mut greeting).await.unwrap();
    assert_eq!(greeting, [5, 1, 0]);
}
pub(super) async fn request(io: &mut TcpStream) -> (Address, u16) {
    greeting(io).await;
    io.write_all(&[5, 0]).await.unwrap();
    let mut header = [0; 4];
    io.read_exact(&mut header).await.unwrap();
    assert_eq!(&header[..3], &[5, 1, 0]);
    let address = match header[3] {
        1 => {
            let mut ip = [0; 4];
            io.read_exact(&mut ip).await.unwrap();
            Address::Ipv4(ip)
        }
        3 => {
            let size = io.read_u8().await.unwrap();
            let mut host = vec![0; size as usize];
            io.read_exact(&mut host).await.unwrap();
            Address::Domain(String::from_utf8(host).unwrap())
        }
        other => panic!("unexpected target address type {other}"),
    };
    let port = io.read_u16().await.unwrap();
    (address, port)
}
pub(super) async fn accept(io: &mut TcpStream) {
    io.write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 0])
        .await
        .unwrap();
}
pub(super) fn servers(listeners: Vec<TcpListener>) -> JoinSet<()> {
    let ports: Vec<_> = listeners
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect();
    let mut jobs = JoinSet::new();
    for (index, listener) in listeners.into_iter().enumerate() {
        let next = ports.get(index + 1).copied();
        jobs.spawn(async move {
            let (mut io, _) = listener.accept().await.unwrap();
            let destination = request(&mut io).await;
            if let Some(port) = next {
                // Preserve the configured node-name representation of the
                // existing relay, even when that name is an IP literal.
                assert_eq!(destination, (Address::Domain("127.0.0.1".into()), port));
                let mut carrier = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
                accept(&mut io).await;
                tokio::io::copy_bidirectional(&mut io, &mut carrier)
                    .await
                    .unwrap();
            } else {
                assert_eq!(destination, (Address::Ipv4([203, 0, 113, 9]), 443));
                accept(&mut io).await;
                io.write_all(b"pre").await.unwrap();
                let mut payload = Vec::new();
                io.read_to_end(&mut payload).await.unwrap();
                assert_eq!(payload, b"request");
                io.write_all(b"tail").await.unwrap();
                io.shutdown().await.unwrap();
            }
        });
    }
    jobs
}
