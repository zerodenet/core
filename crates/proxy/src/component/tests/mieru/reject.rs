use super::*;
use tokio::net::TcpListener;
use zero_platform_tokio::contract::Reason;

#[tokio::test]
async fn actual_proxy_rejects_unqualified_domain_root_before_opening_a_mieru_carrier() {
    reject(false).await;
}
#[cfg(feature = "vless")]
#[tokio::test]
async fn actual_proxy_rejects_unmigrated_cross_owner_role_before_dialing_the_real_donor_listener() {
    reject(true).await;
}
async fn reject(cross_owner: bool) {
    tokio::time::timeout(Duration::from_secs(12),async {
        let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();let address=listener.local_addr().unwrap();
        let mut config=config([address;3]);
        if cross_owner {
            let donor=zero_config::RuntimeConfig::parse(&format!(r#"{{"outbounds":[{{"tag":"a","protocol":{{"type":"vless","server":"127.0.0.1","port":{},"id":"00000000-0000-0000-0000-000000000001"}}}}],"route":{{"final":{{"type":"direct"}}}}}}"#,address.port())).unwrap();
            config.outbounds[0]=donor.outbounds[0].clone();
        } else if let zero_config::OutboundProtocolConfig::Mieru {server,..}=&mut config.outbounds[0].protocol {
            *server="unowned.node.test".into();
        }
        let proxy=Proxy::from_engine_with_background(zero_engine::Engine::new(config.clone()).unwrap(),true).unwrap();
        let running=proxy.spawn();let handle=ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()),proxy.clone());
        handle.apply_runtime_config_and_wait(config,Duration::from_secs(5)).await.unwrap();
        std::future::poll_fn(|cx| {assert!(listener.poll_accept(cx).is_pending());std::task::Poll::Ready(())}).await;
        let error=match try_service(&proxy).await {Ok(_)=>panic!("unqualified native ownership was admitted"),Err(error)=>error};
        assert_eq!(error.reason,Reason::Unsupported,"{error}");
        std::future::poll_fn(|cx| {assert!(listener.poll_accept(cx).is_pending(),"no real donor socket may have been opened");std::task::Poll::Ready(())}).await;
        running.shutdown().await.unwrap();
    }).await.unwrap();
}
