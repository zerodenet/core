//! Existing admitted Source and outbound users must survive a budget rejection.
use super::*;

async fn socks(endpoint: std::net::SocketAddr) -> TcpStream {
    let mut io = TcpStream::connect(endpoint).await.unwrap();
    io.write_all(&[5, 1, 0]).await.unwrap();
    let mut auth = [0; 2];
    io.read_exact(&mut auth).await.unwrap();
    assert_eq!(auth, [5, 0]);
    io.write_all(&[5, 1, 0, 1, 203, 0, 113, 7, 1, 187])
        .await
        .unwrap();
    let mut reply = [0; 10];
    io.read_exact(&mut reply).await.unwrap();
    assert_eq!(&reply[..4], &[5, 0, 0, 1]);
    io
}

#[tokio::test]
async fn source_and_outbound_budget_change_rejects_before_revoking_any_admitted_original_user() {
    tokio::time::timeout(Duration::from_secs(25), async {
        for with_source in [false, true] {
            let source_reservation = crate::socket_fixture::tcp_udp();
            let source_endpoint = source_reservation.0.local_addr().unwrap();
            let entry_reservation = crate::socket_fixture::tcp_udp();
            let entry_endpoint = entry_reservation.0.local_addr().unwrap();
            let remote = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let remote_endpoint = remote.local_addr().unwrap();
            let mut inbounds = vec![serde_json::json!({"tag":"entry", "listen":{"address":"127.0.0.1","port":entry_endpoint.port()},"protocol":{"type":"socks5"}})];
            if with_source { inbounds.push(serde_json::json!({"tag":"salt-source", "listen":{"address":"127.0.0.1","port":source_endpoint.port()},
                "protocol":{"type":"shadowsocks","cipher":METHOD,"password":PASSWORD}})); }
            let initial_total = if with_source {2} else {1};
            let config = zero_config::RuntimeConfig::parse(&serde_json::json!({
                "runtime":{"ss_tcp_replay_max_salts":initial_total}, "inbounds":inbounds,
                "outbounds":[{"tag":"ss-out", "protocol":{"type":"shadowsocks", "server":"127.0.0.1", "port":remote_endpoint.port(), "cipher":METHOD,"password":PASSWORD}}],
                "route":{"rules":[{"condition":{"type":"inbound","values":["entry"]},"action":{"type":"route","outbound":"ss-out"}}],"final":{"type":"direct"}}
            }).to_string()).unwrap();
            let proxy = crate::Proxy::new(config).unwrap();
            let registry = proxy.system_tun_source_registry();
            drop(source_reservation); drop(entry_reservation);
            let running = proxy.spawn();
            let entry = loop { if let Some(owner) = registry.owner("entry") { break owner; } tokio::task::yield_now().await; };
            entry.wait_ready().await.unwrap();
            let local = if with_source {
                let owner = loop { if let Some(owner) = registry.owner("salt-source") { break owner; } tokio::task::yield_now().await; };
                owner.wait_ready().await.unwrap(); Some(owner)
            } else {None};
            let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut incoming = if with_source {
                let mut client = client(source_endpoint,target.local_addr().unwrap()).await;
                let (mut direct, _) = target.accept().await.unwrap();
                let mut bytes = [0;4]; direct.read_exact(&mut bytes).await.unwrap(); assert_eq!(&bytes,b"salt");
                direct.write_all(b"seen").await.unwrap(); client.read_exact(&mut bytes).await.unwrap(); assert_eq!(&bytes,b"seen");
                Some((client,direct))
            } else {None};
            let acceptor = ::shadowsocks::ShadowsocksInboundTcpAcceptor::new(
                ::shadowsocks::ShadowsocksInboundProfile::from_config(METHOD,PASSWORD).unwrap());
            let (front, server) = tokio::join!(async {
                let mut front = socks(entry_endpoint).await;
                front.write_all(b"sent").await.unwrap(); front
            }, async {
                let (io,_) = remote.accept().await.unwrap();
                let (destination,_,mut server) = acceptor.accept_stream_facts(zero_platform_tokio::TokioSocket::new(io)).await.unwrap();
                assert_eq!(destination.address,zero_core::Address::Ipv4([203,0,113,7]));
                assert_eq!(destination.port,443);
                let mut bytes = [0;4]; server.read_exact(&mut bytes).await.unwrap(); assert_eq!(&bytes,b"sent");
                server.write_all(b"back").await.unwrap(); server.flush().await.unwrap(); server
            });
            let (mut front, mut server) = (front, server);
            let mut bytes = [0;4]; front.read_exact(&mut bytes).await.unwrap(); assert_eq!(&bytes,b"back");
            let handle = crate::ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()),proxy.clone());
            let mut candidate = (*proxy.engine().config()).clone();
            candidate.runtime.ss_tcp_replay_max_salts = Some(initial_total+1);
            assert!(handle.apply_runtime_config_and_wait(candidate,Duration::from_secs(5)).await.is_err(),
                "growing budget must not revoke already admitted Source/outbound users to obtain StopOld");
            assert!(Arc::ptr_eq(&entry,&registry.owner("entry").unwrap()));
            if let Some(owner) = &local { assert!(Arc::ptr_eq(owner,&registry.owner("salt-source").unwrap())); }
            front.write_all(b"live").await.unwrap(); server.read_exact(&mut bytes).await.unwrap(); assert_eq!(&bytes,b"live");
            server.write_all(b"same").await.unwrap(); front.read_exact(&mut bytes).await.unwrap(); assert_eq!(&bytes,b"same");
            if let Some((client,direct)) = &mut incoming {
                client.write_all(b"keep").await.unwrap(); direct.read_exact(&mut bytes).await.unwrap(); assert_eq!(&bytes,b"keep");
                direct.write_all(b"same").await.unwrap(); client.read_exact(&mut bytes).await.unwrap(); assert_eq!(&bytes,b"same");
            }
            assert_eq!(proxy.engine().config().runtime.effective_ss_tcp_replay_max_salts(),initial_total);
            drop(incoming); drop(front); drop(server);
            loop {
                if entry.accepted_join_for_test().is_some() && local.as_ref().is_none_or(|owner| owner.accepted_join_for_test().is_some()) {break;}
                tokio::task::yield_now().await;
            }
            running.shutdown().await.unwrap();
            assert!(entry.terminal_for_test().unwrap().closed_io_receipts() >= 3);
            if let Some(owner) = local { assert!(owner.terminal_for_test().unwrap().closed_io_receipts() >= 4); }
        }
    }).await.unwrap();
}
