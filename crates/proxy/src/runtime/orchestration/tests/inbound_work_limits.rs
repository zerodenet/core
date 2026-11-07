//! Every original accepted/peer producer and real task pressure continuity.
use crate::{Proxy, RunningProxy};
use std::{io, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};
use zero_config::RuntimeConfig;
use zero_platform_tokio::contract::component::ResourceDeclaration as R;
async fn owner(proxy: &Proxy, tag: &str) -> Arc<crate::runtime::inbound_owners::InboundOwner> {
    timeout(Duration::from_secs(5), async {
        loop {
            if let Some(owner) = proxy.inbound_owners.owner(tag) {
                owner.wait_ready().await.unwrap();
                return owner;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}
fn closed(owner: &crate::runtime::inbound_owners::InboundOwner) {
    let actual = owner
        .terminal_for_test()
        .expect("actual original Source native/users/Scope receipt");
    assert_eq!(actual.identity(), owner.identity());
    assert!(actual.joined_tasks() > 0 && actual.closed_io_receipts() > 0);
    assert!(
        actual.failure().is_none()
            && actual.task_failures().is_empty()
            && actual.native_bound_failures().is_empty()
    );
}
#[tokio::test]
async fn all_actual_inbound_accepted_and_peer_budgets_are_cold_frozen_and_preserve_omission() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let files = tempfile::tempdir().unwrap();
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert = files.path().join("cert.pem");
    let key = files.path().join("key.pem");
    std::fs::write(&cert, certificate.cert.pem()).unwrap();
    std::fs::write(&key, certificate.signing_key.serialize_pem()).unwrap();
    let id = "11111111-2222-3333-4444-555555555555";
    for protocol in [
        serde_json::json!({"type":"http"}),
        serde_json::json!({"type":"socks5"}),
        serde_json::json!({"type":"mixed"}),
        serde_json::json!({"type":"shadowsocks","cipher":"aes-128-gcm","password":"actual"}),
        serde_json::json!({"type":"mieru","users":[{"username":"u","password":"p"}]}),
        serde_json::json!({"type":"vless","users":[{"id":id}]}),
        serde_json::json!({"type":"vmess","users":[{"id":id}]}),
        serde_json::json!({"type":"trojan","password":"actual","tls":{"cert_path":cert,"key_path":key}}),
        serde_json::json!({"type":"hysteria2","password":"actual","cert_path":cert,"key_path":key}),
    ] {
        let config=RuntimeConfig::parse(&serde_json::json!({"runtime":{"inbound_max_tasks_per_carrier":600,"inbound_max_resources_per_carrier":300,"inbound_max_in_flight_requests":400},"inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":protocol}],"route":{"final":{"type":"direct"}}}).to_string()).unwrap();
        let proxy = Proxy::new(config.clone()).unwrap();
        let prepared = proxy
            .protocols
            .prepare_inbound_bind_in_config(config.inbounds[0].clone(), &config)
            .unwrap_or_else(|source| panic!("actual cold {} Source: {source}", protocol["type"]))
            .unwrap();
        let mut covered = 0;
        for declaration in prepared.resources() {
            let (tasks, resources) = match declaration {
                R::TcpListener {
                    accepted: Some(limits),
                    ..
                } => (
                    limits.max_tasks_per_socket.get(),
                    limits.max_resources_per_socket,
                ),
                R::DatagramListener {
                    accepted: Some(limits),
                    ..
                } => (
                    limits.max_tasks_per_peer.get(),
                    limits.max_resources_per_peer,
                ),
                R::DatagramAssociations {
                    max_tasks_per_association,
                    max_resources_per_association,
                    ..
                } => (
                    max_tasks_per_association.get(),
                    *max_resources_per_association,
                ),
                _ => continue,
            };
            assert_eq!(
                (tasks, resources),
                (600, 300),
                "actual cold producer {}",
                protocol["type"]
            );
            covered += 1;
        }
        assert!(covered > 0);
        assert_eq!(
            proxy
                .protocols
                .runtime_host()
                .original_budgets()
                .carrier
                .snapshot()
                .charged,
            0,
            "limits do not preallocate originals"
        );
    }
}
async fn socks(address: std::net::SocketAddr, port: u16) -> io::Result<TcpStream> {
    let mut io = TcpStream::connect(address).await?;
    io.write_all(&[5, 1, 0]).await?;
    let mut auth = [0; 2];
    io.read_exact(&mut auth).await?;
    assert_eq!(auth, [5, 0]);
    let mut request = vec![5, 1, 0, 1, 127, 0, 0, 1];
    request.extend_from_slice(&port.to_be_bytes());
    io.write_all(&request).await?;
    let mut reply = [0; 10];
    io.read_exact(&mut reply).await?;
    if reply[1] != 0 {
        return Err(io::Error::new(
            io::ErrorKind::ConnectionRefused,
            "actual new logical request was refused",
        ));
    }
    Ok(io)
}
async fn live(front: &mut TcpStream, back: &mut TcpStream) {
    front.write_all(b"still live").await.unwrap();
    let mut bytes = [0; 10];
    back.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"still live");
    back.write_all(&bytes).await.unwrap();
    front.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"still live");
}
#[tokio::test]
async fn actual_mieru_carrier_task_pressure_refuses_only_new_request_and_retains_live_wire() {
    timeout(Duration::from_secs(25),async {
        let config=RuntimeConfig::parse(&serde_json::json!({"runtime":{"inbound_max_tasks_per_carrier":3,"inbound_max_resources_per_carrier":4,"inbound_max_in_flight_requests":4},"inbounds":[{"tag":"source","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"mieru","users":[{"username":"u","password":"p"}]}}],"route":{"final":{"type":"direct"}}}).to_string()).unwrap();
        let remote=Proxy::new(config).unwrap();let remote_running:RunningProxy=remote.spawn();let remote_owner=owner(&remote,"source").await;let endpoint=*remote_owner.original_listener_address.get().unwrap();
        let config=RuntimeConfig::parse(&serde_json::json!({"runtime":{"mieru_max_carriers":1},"inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],"outbounds":[{"tag":"pool","protocol":{"type":"mieru","server":"127.0.0.1","port":endpoint.port(),"username":"u","password":"p"}}],"route":{"final":{"type":"route","outbound":"pool"}}}).to_string()).unwrap();
        let client=Proxy::new(config).unwrap();let running=client.spawn();let client_owner=owner(&client,"entry").await;let address=*client_owner.original_listener_address.get().unwrap();
        let echo=TcpListener::bind("127.0.0.1:0").await.unwrap();let port=echo.local_addr().unwrap().port();
        let (first,back)=tokio::join!(socks(address,port),echo.accept());let mut first=first.unwrap();let mut back=back.unwrap().0;live(&mut first,&mut back).await;
        // The SOCKS local Ready response does not prove the remote Mieru
        // Source admitted a logical request. Drive actual payload and observe
        // its failed delivery on the same N1 physical carrier instead.
        let denied=timeout(Duration::from_secs(3),async {
            let mut next=socks(address,port).await?;
            next.write_all(b"must not allocate").await?;
            let mut byte=[0];next.read_exact(&mut byte).await.map(|_|())
        }).await.unwrap();
        assert!(denied.is_err(),"the real source task book cannot allocate a second logical request");
        let mut unallocated=Box::pin(echo.accept());
        std::future::poll_fn(|cx|{assert!(std::future::Future::poll(unallocated.as_mut(),cx).is_pending(),"no second original destination socket was created");std::task::Poll::Ready(())}).await;
        drop(unallocated);
        assert_eq!(remote.engine().active_sessions().len(),1);live(&mut first,&mut back).await;
        drop(first);drop(back);running.shutdown().await.unwrap();closed(&client_owner);
        remote_running.shutdown().await.unwrap();closed(&remote_owner);
        assert_eq!(remote.protocols.runtime_host().original_budgets().carrier.snapshot().charged,0);
    }).await.unwrap();
}
