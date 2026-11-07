//! The whole ordinary Source cap set, not scalar counters or a new executor.
use super::inbound_work_limits::{closed, owner, socks};
use crate::{Proxy, ProxyHandle};
use std::{
    future::{poll_fn, Future},
    task::Poll,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};
use zero_config::RuntimeConfig;
use zero_platform_tokio::contract::component::ResourceDeclaration as R;
#[tokio::test]
async fn every_source_socket_peer_queue_and_service_limit_is_in_the_actual_cold_declaration() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let files = tempfile::tempdir().unwrap();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let certificate = files.path().join("cert.pem");
    let key = files.path().join("key.pem");
    std::fs::write(&certificate, cert.cert.pem()).unwrap();
    std::fs::write(&key, cert.signing_key.serialize_pem()).unwrap();
    let id = "11111111-2222-3333-4444-555555555555";
    let mut seen = [false; 9];
    for protocol in [
        serde_json::json!({"type":"http"}),
        serde_json::json!({"type":"socks5"}),
        serde_json::json!({"type":"mixed"}),
        serde_json::json!({"type":"shadowsocks","cipher":"aes-128-gcm","password":"actual"}),
        serde_json::json!({"type":"shadowsocks","cipher":"aes-128-gcm","password":"actual","plugin":{"command":"example-plugin","mode":"tcp_and_udp"}}),
        serde_json::json!({"type":"mieru","users":[{"username":"u","password":"p"}]}),
        serde_json::json!({"type":"mieru","users":[{"username":"u","password":"p"}],"transport":"udp"}),
        serde_json::json!({"type":"vless","users":[{"id":id}],"fallback":{"server":"127.0.0.1","port":9}}),
        serde_json::json!({"type":"vless","users":[{"id":id}],"mkcp":{}}),
        serde_json::json!({"type":"vmess","users":[{"id":id}]}),
        serde_json::json!({"type":"trojan","password":"actual","tls":{"cert_path":certificate,"key_path":key}}),
        serde_json::json!({"type":"hysteria2","password":"actual","cert_path":certificate,"key_path":key,"masquerade":{"type":"file","dir":files.path()}}),
    ] {
        let config=RuntimeConfig::parse(&serde_json::json!({"runtime":{"inbound_max_accepted_sockets":2,"inbound_max_datagram_associations":3,"inbound_max_datagram_peers":4,"inbound_max_packets_per_peer":5,"inbound_max_pending_sends":6,"inbound_max_bind_attempts":7,"inbound_max_target_sockets":8,"inbound_max_file_sessions":9,"inbound_max_handoff_probe_attempts":10},"inbounds":[{"tag":"source","listen":{"address":"127.0.0.1","port":0},"protocol":protocol}],"route":{"final":{"type":"direct"}}}).to_string()).unwrap();
        let proxy = Proxy::new(config.clone()).unwrap();
        let actual = proxy
            .protocols
            .prepare_inbound_bind_in_config(config.inbounds[0].clone(), &config)
            .unwrap()
            .unwrap();
        for resource in actual.resources() {
            match resource {
                R::TcpListener {
                    accepted: Some(limits),
                    max_bind_attempts,
                    ..
                } => {
                    assert_eq!((limits.max_sockets.get(), max_bind_attempts.get()), (2, 7));
                    seen[0] = true;
                    seen[5] = true;
                }
                R::DatagramListener {
                    accepted: Some(limits),
                    max_pending_sends,
                    max_bind_attempts,
                    ..
                } => {
                    assert_eq!(
                        (
                            limits.max_peers.get(),
                            limits.packets_per_peer.get(),
                            max_pending_sends.get(),
                            max_bind_attempts.get()
                        ),
                        (4, 5, 6, 7)
                    );
                    seen[2] = true;
                    seen[3] = true;
                    seen[4] = true;
                    seen[5] = true;
                }
                R::DatagramAssociations {
                    max_associations, ..
                } => {
                    assert_eq!(max_associations.get(), 3);
                    seen[1] = true;
                }
                R::AcceptedDatagram {
                    max_sockets,
                    max_pending_sends,
                } => {
                    assert_eq!(
                        max_sockets.get(),
                        1,
                        "one actual physical socket per SOCKS association remains structural"
                    );
                    assert_eq!(max_pending_sends.get(), 6);
                    seen[4] = true;
                }
                R::InboundTargets { max_sockets, .. } => {
                    assert_eq!(max_sockets.get(), 8);
                    seen[6] = true;
                }
                R::FileTree { max_sessions, .. } => {
                    assert_eq!(max_sessions.get(), 9);
                    seen[7] = true;
                }
                R::ListenerHandoff {
                    max_probe_attempts, ..
                } => {
                    assert_eq!(max_probe_attempts.get(), 10);
                    seen[8] = true;
                }
                R::MetadataDatagram {
                    max_bind_attempts, ..
                } => assert_eq!(
                    max_bind_attempts.get(),
                    1,
                    "single literal main bind retains its actual original structure"
                ),
                _ => {}
            }
        }
        assert_eq!(
            proxy
                .protocols
                .runtime_host()
                .original_budgets()
                .carrier
                .snapshot()
                .charged,
            0,
            "cold limits do not preallocate originals"
        );
    }
    assert_eq!(seen, [true; 9]);
}
async fn wire(front: &mut TcpStream, back: &mut TcpStream) {
    front.write_all(b"old").await.unwrap();
    let mut bytes = [0; 3];
    back.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"old");
    back.write_all(&bytes).await.unwrap();
    front.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"old");
}
#[tokio::test]
async fn accepted_cap_wait_and_cancel_preserve_old_wire_and_actual_stop_old_replaces_source() {
    timeout(Duration::from_secs(25),async {
        let mut config=RuntimeConfig::parse(&serde_json::json!({"runtime":{"inbound_max_accepted_sockets":1},"inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],"route":{"final":{"type":"direct"}}}).to_string()).unwrap();
        let proxy=Proxy::new(config.clone()).unwrap();let running=proxy.spawn();let original=owner(&proxy,"entry").await;let address=*original.original_listener_address.get().unwrap();let echo=TcpListener::bind("127.0.0.1:0").await.unwrap();let (first,back)=tokio::join!(socks(address,echo.local_addr().unwrap().port()),echo.accept());let mut first=first.unwrap();let mut back=back.unwrap().0;wire(&mut first,&mut back).await;
        let mut next=TcpStream::connect(address).await.unwrap();next.write_all(&[5,1,0]).await.unwrap();let mut greeting=[0;2];
        {let mut waiting=Box::pin(next.read_exact(&mut greeting));poll_fn(|cx|{assert!(waiting.as_mut().poll(cx).is_pending());Poll::Ready(())}).await;wire(&mut first,&mut back).await;drop(waiting);}
        assert!(original.terminal_for_test().is_none());
        config.runtime.inbound_max_accepted_sockets=Some(2);
        let handle=ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()),proxy.clone());
        assert!(handle.apply_runtime_config_and_wait(config.clone(),Duration::from_secs(5)).await.is_err(),"active original users cannot be evicted to install a changed cap");
        assert_eq!(owner(&proxy,"entry").await.identity(),original.identity());wire(&mut first,&mut back).await;
        drop(first);drop(back);
        timeout(Duration::from_secs(5),next.read_exact(&mut greeting)).await.unwrap().unwrap();assert_eq!(greeting,[5,0],"cancelled wait did not consume the pending peer or stop the old Source");drop(next);
        timeout(Duration::from_secs(5),async{while !original.acquisition_idle_for_test(){tokio::task::yield_now().await;}}).await.unwrap();
        handle.apply_runtime_config_and_wait(config,Duration::from_secs(5)).await.unwrap();closed(&original);let fresh=owner(&proxy,"entry").await;assert_ne!(fresh.identity(),original.identity());
        let mut peer=TcpStream::connect(*fresh.original_listener_address.get().unwrap()).await.unwrap();peer.write_all(&[5,1,0]).await.unwrap();peer.read_exact(&mut greeting).await.unwrap();assert_eq!(greeting,[5,0]);drop(peer);running.shutdown().await.unwrap();closed(&fresh);assert_eq!(proxy.protocols.runtime_host().original_budgets().carrier.snapshot().charged,0);
    }).await.unwrap();
}
