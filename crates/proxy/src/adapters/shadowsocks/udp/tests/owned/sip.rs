//! Demand quota is refused before poisoning a cold node or unrelated live IO.
use crate::{Proxy, ProxyHandle};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};

async fn connect(endpoint: std::net::SocketAddr) -> std::io::Result<TcpStream> {
    let mut io = TcpStream::connect(endpoint).await?;
    io.write_all(&[5, 1, 0]).await?;
    let mut auth = [0; 2];
    io.read_exact(&mut auth).await?;
    assert_eq!(auth, [5, 0]);
    io.write_all(&[5, 1, 0, 1, 203, 0, 113, 7, 1, 187]).await?;
    let mut reply = [0; 10];
    io.read_exact(&mut reply).await?;
    if reply[1] != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "actual SOCKS request refused",
        ));
    }
    assert_eq!(&reply[..4], &[5, 0, 0, 1]);
    Ok(io)
}
#[tokio::test]
async fn actual_sip_process_quota_keeps_other_business_alive_and_cold_node_retries_after_matching_retirement(
) {
    timeout(Duration::from_secs(30),async {
        let directory=tempfile::tempdir().unwrap();
        let fixture=std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../protocols/shadowsocks/tests/support/sip003.py");
        let remote=TcpListener::bind("127.0.0.1:0").await.unwrap();let remote_port=remote.local_addr().unwrap().port();
        let config=zero_config::RuntimeConfig::parse(&serde_json::json!({
            "runtime":{"process_max_jobs":1,"process_max_jobs_per_instance":1},
            "inbounds":[{"tag":"entry-a","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}},{"tag":"entry-b","listen":{"address":"127.0.0.2","port":0},"protocol":{"type":"socks5"}}],
            "outbounds":[{"tag":"a","protocol":{"type":"shadowsocks","server":"127.0.0.1","port":remote_port,"cipher":"aes-128-gcm","password":"process-wire","plugin":{"command":"python3","args":[fixture,"client","tcp_only",directory.path().join("a")],"options":"test=options;escaped=value","mode":"tcp_only"}}},{"tag":"b","protocol":{"type":"shadowsocks","server":"127.0.0.1","port":remote_port,"cipher":"aes-128-gcm","password":"process-wire","plugin":{"command":"python3","args":[fixture,"client","tcp_only",directory.path().join("b")],"options":"test=options;escaped=value","mode":"tcp_only"}}}],
            "route":{"rules":[{"condition":{"type":"inbound","values":["entry-a"]},"action":{"type":"route","outbound":"a"}},{"condition":{"type":"inbound","values":["entry-b"]},"action":{"type":"route","outbound":"b"}}],"final":{"type":"direct"}}
        }).to_string()).unwrap();
        let proxy=Proxy::new(config).unwrap();let registry=proxy.system_tun_source_registry();let running=proxy.spawn();
        let mut endpoints=Vec::new();let mut owners=Vec::new();
        for tag in ["entry-a","entry-b"] {
            let owner=loop {if let Some(owner)=registry.owner(tag) {break owner;}tokio::task::yield_now().await;};
            owner.wait_ready().await.unwrap();endpoints.push(*owner.original_listener_address.get().unwrap());owners.push(owner);
        }
        let ready=ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()),proxy.clone());
        ready.apply_runtime_config_and_wait((*proxy.engine().config()).clone(),Duration::from_secs(5)).await.unwrap();
        let roots=proxy.protocols.runtime_host().original_budgets();assert_eq!(roots.process.snapshot().charged_jobs,0);
        assert!(!directory.path().join("a").exists()&&!directory.path().join("b").exists(),"cold Ready cannot eagerly start unused SIP children");
        let plan=proxy.tcp_runtime_services().components.clone().unwrap();let cold_b=plan.configured("b").unwrap().instance_id();
        let (accepted_tx,mut accepted_rx)=tokio::sync::mpsc::channel(4);
        let peer=tokio::spawn(async move {
            let acceptor=::shadowsocks::ShadowsocksInboundTcpAcceptor::new(::shadowsocks::ShadowsocksInboundProfile::from_config("aes-128-gcm","process-wire").unwrap());
            loop {
                let (io,_)=remote.accept().await.unwrap();
                if let Ok((target,_,io))=acceptor.accept_stream_facts(zero_platform_tokio::TokioSocket::new(io)).await {
                    assert_eq!(target.address,zero_core::Address::Ipv4([203,0,113,7]));assert_eq!(target.port,443);
                    if accepted_tx.send(io).await.is_err() {break;}
                }
            }
        });
        let (front,server)=tokio::join!(async {let mut io=connect(endpoints[0]).await.unwrap();io.write_all(b"sent").await.unwrap();io},async {accepted_rx.recv().await.unwrap()});
        let (mut front,mut server)=(front,server);let mut bytes=[0;4];server.read_exact(&mut bytes).await.unwrap();assert_eq!(&bytes,b"sent");
        server.write_all(b"back").await.unwrap();server.flush().await.unwrap();front.read_exact(&mut bytes).await.unwrap();assert_eq!(&bytes,b"back");
        assert_eq!(roots.process.snapshot().charged_jobs,1);assert!(directory.path().join("a").exists());
        for _ in 0..2 {
            assert!(timeout(Duration::from_secs(4),connect(endpoints[1])).await.unwrap().is_err(),"quota rejects this original caller before cold initializer creation");
            assert_eq!(roots.process.snapshot().charged_jobs,1);assert!(!directory.path().join("b").exists());
            assert!(owners.iter().all(|owner|owner.is_running()&&owner.terminal_for_test().is_none()));
            front.write_all(b"live").await.unwrap();server.read_exact(&mut bytes).await.unwrap();assert_eq!(&bytes,b"live");
            server.write_all(b"same").await.unwrap();server.flush().await.unwrap();front.read_exact(&mut bytes).await.unwrap();assert_eq!(&bytes,b"same");
        }
        drop(front);drop(server);
        assert_eq!(roots.process.snapshot().charged_jobs,1,"closing a TCP user does not claim the cached child tree closed");
        let mut candidate=(*proxy.engine().config()).clone();candidate.outbounds.retain(|node|node.tag!="a");candidate.route.rules.remove(0);
        let handle=ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()),proxy.clone());
        handle.apply_runtime_config_and_wait(candidate,Duration::from_secs(5)).await.unwrap();
        assert_eq!(roots.process.snapshot().charged_jobs,0,"matching old tree, endpoint and actual actor ACK precede quota reuse");
        assert_eq!(proxy.tcp_runtime_services().components.as_ref().unwrap().configured("b").unwrap().instance_id(),cold_b,"budget refusal never poisoned or swapped the cold original slot");
        let (front,server)=tokio::join!(async {let mut io=connect(endpoints[1]).await.unwrap();io.write_all(b"next").await.unwrap();io},async {accepted_rx.recv().await.unwrap()});
        let (mut front,mut server)=(front,server);server.read_exact(&mut bytes).await.unwrap();assert_eq!(&bytes,b"next");server.write_all(b"done").await.unwrap();server.flush().await.unwrap();front.read_exact(&mut bytes).await.unwrap();assert_eq!(&bytes,b"done");
        assert_eq!(roots.process.snapshot().charged_jobs,1);assert!(directory.path().join("b").exists());
        drop(front);drop(server);running.shutdown().await.unwrap();assert_eq!(roots.process.snapshot().charged_jobs,0);
        peer.abort();assert!(peer.await.unwrap_err().is_cancelled());
    }).await.unwrap();
}
