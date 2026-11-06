//! Warm physical leases belong to actual cached receivers; Business stays Busy.
use super::*;
use std::sync::Arc;
use tokio::net::TcpStream;
async fn address(proxy: &Proxy) -> std::net::SocketAddr {
    let owner = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(owner) = proxy.system_tun_source_registry().owner("entry") {
                break owner;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    owner.wait_ready().await.unwrap();
    *owner.original_listener_address.get().unwrap()
}
async fn business(proxy: &Proxy) -> TcpStream {
    let mut io = TcpStream::connect(address(proxy).await).await.unwrap();
    io.write_all(&[5, 1, 0]).await.unwrap();
    let mut auth = [0; 2];
    io.read_exact(&mut auth).await.unwrap();
    assert_eq!(auth, [5, 0]);
    io.write_all(&[5, 1, 0, 1, 203, 0, 113, 1, 1, 187])
        .await
        .unwrap();
    let mut ready = [0; 10];
    io.read_exact(&mut ready).await.unwrap();
    assert_eq!(ready[1], 0);
    io
}
async fn echo(io: &mut TcpStream, bytes: &[u8]) {
    io.write_all(bytes).await.unwrap();
    let mut response = vec![0; bytes.len()];
    io.read_exact(&mut response).await.unwrap();
    assert_eq!(response, bytes);
}
#[tokio::test]
async fn warm_same_pool_runtime_leases_stop_actual_receivers_but_live_business_keeps_original_wire()
{
    tokio::time::timeout(Duration::from_secs(30), async {
        let a = Server::new(true).await; let b = Server::new(true).await; let c = Server::new(false).await;
        let mut config = config([a.address,b.address,c.address]);
        let ingress = RuntimeConfig::parse(r#"{"inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],"route":{"final":{"type":"direct"}}}"#).unwrap();
        config.inbounds = ingress.inbounds;
        config.route.final_action = zero_config::RouteActionConfig::Route { outbound: "chain".into() };
        config.runtime.carrier_max_total_originals = Some(32);
        let proxy = Proxy::new(config.clone()).unwrap(); let running = proxy.spawn();
        let handle = ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()),proxy.clone());
        handle.apply_runtime_config_and_wait(config.clone(),Duration::from_secs(5)).await.unwrap();
        let mut held = loan(&proxy).await;
        held.io.write_all(b"warm").await.unwrap(); let mut warm=[0;4]; held.io.read_exact(&mut warm).await.unwrap(); assert_eq!(&warm,b"warm");
        let mut originals=Vec::new(); let mut current=Some(held.owner.original().clone());
        while let Some(original)=current { current=original.parent().cloned(); originals.push(original); }
        assert_eq!(originals.len(),3);
        drop(held.io); assert!(held.owner.close(deadline()).await.unwrap().is_complete());
        for original in &originals { assert!(original.closed().is_none()); assert!(!original.driver().unwrap().is_joined()); }
        // An actual accepted Business also leases the same roots. It has no
        // carrier-receiver token, regardless of the already warm physical pool.
        let mut io=business(&proxy).await; echo(&mut io,b"before-busy").await;
        let services=proxy.tcp_runtime_services(); let plan=services.components.as_ref().unwrap().clone();
        let old=plan.configured("c").unwrap();
        let mut next=config.clone(); next.runtime.carrier_max_total_originals=Some(16);
        assert!(handle.apply_runtime_config_and_wait(next.clone(),Duration::from_secs(5)).await.is_err());
        assert!(Arc::ptr_eq(&old,&proxy.tcp_runtime_services().components.as_ref().unwrap().configured("c").unwrap()));
        echo(&mut io,b"old-wire-still-live").await;
        for original in &originals { assert!(original.closed().is_none()); assert!(!original.driver().unwrap().is_joined()); }
        drop(io);
        tokio::time::timeout(Duration::from_secs(3),async {
            while !proxy.engine().active_sessions().is_empty() { tokio::task::yield_now().await; }
        }).await.unwrap();
        handle.apply_runtime_config_and_wait(next,Duration::from_secs(5)).await.unwrap();
        for original in &originals {
            let driver=original.driver().unwrap(); let joined=driver.try_joined().unwrap();
            assert!(driver.matches_joined(&joined) && joined.registration_released());
            let closed=original.closed().expect("same original closed before replacement"); assert!(closed.matches(original));
            let owner=original.owner(); assert!(owner.closed().unwrap().matches(owner));
            assert!(owner.users_closed().is_some());
        }
        let mut fresh=loan(&proxy).await;
        assert!(!fresh.owner.original().same_original(&originals[0]));
        for server in [&a,&b,&c] { assert_eq!(server.accepts.load(Ordering::SeqCst),2); }
        fresh.io.write_all(b"after").await.unwrap(); let mut after=[0;5]; fresh.io.read_exact(&mut after).await.unwrap(); assert_eq!(&after,b"after");
        drop(fresh.io); assert!(fresh.owner.close(deadline()).await.unwrap().is_complete());
        running.shutdown().await.unwrap(); a.close().await; b.close().await; c.close().await;
    }).await.unwrap();
}
