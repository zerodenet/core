//! Unselected migrated graphs preserve the actual standard Mieru owner.
use super::*;
use tokio::net::TcpStream;

fn with_alias(mut config: zero_config::RuntimeConfig) -> zero_config::RuntimeConfig {
    config.outbounds.push(serde_json::from_value(serde_json::json!({"tag":"grpc-grammar","protocol":{
        "type":"vmess","server":"127.0.0.1","port":1,"id":"11111111-2222-3333-4444-555555555555","grpc":{"service_names":["unused/Tun"]}
    }})).unwrap());
    config.outbound_groups.push(
        serde_json::from_value(
            serde_json::json!({"tag":"unused-relay","type":"relay","proxies":["a","grpc-grammar"]}),
        )
        .unwrap(),
    );
    config
}
async fn echo(io: &mut TcpStream) {
    io.write_all(b"live").await.unwrap();
    io.flush().await.unwrap();
    let mut data = [0; 4];
    io.read_exact(&mut data).await.unwrap();
    assert_eq!(&data, b"live");
}
async fn reload_alias(remove: bool) {
    tokio::time::timeout(Duration::from_secs(20), async {
        let a = Server::new(false).await;
        let b = Server::new(false).await;
        let c = Server::new(false).await;
        let mut native = config([a.address, b.address, c.address]);
        native.inbounds.push(
            serde_json::from_value(serde_json::json!({
                "tag":"migration","listen":{"address":"127.0.0.1","port":0},
                "protocol":{"type":"direct","target":"203.0.113.1","port":443}
            }))
            .unwrap(),
        );
        native.route.final_action = zero_config::RouteActionConfig::Route {
            outbound: "a".into(),
        };
        let (initial, next) = if remove {
            (with_alias(native.clone()), native)
        } else {
            (native.clone(), with_alias(native))
        };
        let proxy = Proxy::new(initial.clone()).unwrap();
        let running = proxy.spawn();
        let handle = ProxyHandle::new(
            zero_engine::EngineHandle::new(proxy.engine().clone()),
            proxy.clone(),
        );
        handle
            .apply_runtime_config_and_wait(initial, Duration::from_secs(5))
            .await
            .unwrap();
        let source = crate::runtime::test_listener::original_owner(&proxy, "migration");
        source.wait_ready().await.unwrap();
        let address = *source.original_listener_address.get().unwrap();
        let mut old = TcpStream::connect(address).await.unwrap();
        echo(&mut old).await;
        let plan = proxy.tcp_runtime_services().components.clone().unwrap();
        let slot = plan.configured("a").expect("actual standard Mieru role");
        let registration = plan
            .original_carrier_owner(slot.dependency_instances()[0], 0)
            .unwrap();
        let observed = registration.observe().unwrap();
        assert_eq!(observed.retained(), 1);
        let native = observed.slots()[0].original().clone();
        let driver = observed.slots()[0].driver().unwrap().1.clone();
        let revision = proxy.config_revision();
        handle
            .apply_runtime_config_and_wait(next.clone(), Duration::from_secs(5))
            .await
            .unwrap();
        assert!(proxy.config_revision() > revision);
        assert_eq!(proxy.engine().runtime_snapshot().config().as_ref(), &next);
        let next_plan = proxy.tcp_runtime_services().components.clone().unwrap();
        assert_eq!(
            slot.instance_id(),
            next_plan.configured("a").unwrap().instance_id()
        );
        let same = next_plan
            .original_carrier_owner(slot.dependency_instances()[0], 0)
            .unwrap();
        let current = same.observe().unwrap();
        assert_eq!(current.retained(), 1);
        assert!(current.slots()[0].original().same_original(&native));
        assert!(current.slots()[0]
            .driver()
            .unwrap()
            .1
            .same_registration(&driver));
        assert!(native.closed().is_none() && !driver.is_joined());
        assert_eq!(next_plan.configured("grpc-grammar").is_some(), !remove);
        echo(&mut old).await;
        let mut fresh = TcpStream::connect(address).await.unwrap();
        echo(&mut fresh).await;
        assert_eq!(
            a.accepts.load(Ordering::SeqCst),
            1,
            "both actual Source sessions use the same original pool"
        );
        assert_eq!(
            b.accepts.load(Ordering::SeqCst) + c.accepts.load(Ordering::SeqCst),
            0
        );
        old.shutdown().await.unwrap();
        fresh.shutdown().await.unwrap();
        drop(old);
        drop(fresh);
        running.shutdown().await.unwrap();
        let joined = driver
            .try_joined()
            .expect("same original Mieru driver joined");
        assert!(driver.matches_joined(&joined) && joined.registration_released());
        let closed = native
            .closed()
            .expect("same original native provider closed");
        assert!(native.matches_closed(&closed) && closed.had_native_acquisition());
        assert!(registration.closed().unwrap().matches(&registration));
        assert!(registration.users_closed().unwrap().matches(&registration));
        let terminal = source
            .terminal_for_test()
            .expect("actual Direct Source retired");
        assert!(terminal.closed_io_receipts() > 0 && terminal.joined_tasks() > 0);
        assert!(terminal.failure().is_none());
        a.close().await;
        b.close().await;
        c.close().await;
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn ordinary_mieru_removes_migrated_grpc_alias_without_replacing_live_standard_owner() {
    reload_alias(true).await;
}
#[tokio::test]
async fn ordinary_mieru_adds_migrated_grpc_alias_without_replacing_live_standard_owner() {
    reload_alias(false).await;
}
