//! Changed virtual client graphs keep the same actual listener pipe and wire.
use super::*;
use std::sync::Arc;
fn linked(peer: SocketAddr, socks: u16) -> RuntimeConfig {
    RuntimeConfig::parse(&serde_json::json!({
        "inbounds":[
            {"tag":"socks","listen":{"address":"127.0.0.1","port":socks},"protocol":{"type":"socks5"}},
            {"tag":"wg-source","listen":{"address":"127.0.0.1","port":0},"protocol":{
                "type":"wireguard","private_key":STANDARD.encode([121;32]),"addresses":["10.0.0.1/32"],
                "peers":[{"public_key":peer::public(122),"allowed_ips":["0.0.0.0/0"]}]}}
        ],"outbounds":[{"tag":"wg","protocol":{
            "type":"wireguard","private_key":STANDARD.encode([121;32]),"addresses":["10.0.0.1/32"],"inbound_tag":"wg-source",
            "peers":[{"public_key":peer::public(122),"endpoint":peer.to_string(),"allowed_ips":["0.0.0.0/0"]}]
        }}],"route":{"final":{"type":"route","outbound":"wg"}}
    }).to_string()).unwrap()
}
async fn echo(port: u16) -> TcpStream {
    let mut stream = tokio::time::timeout_at(end(), connect(port, false))
        .await
        .unwrap();
    stream.write_all(b"same-wire-after-rebuild").await.unwrap();
    let mut bytes = [0; 23];
    tokio::time::timeout_at(end(), stream.read_exact(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&bytes, b"same-wire-after-rebuild");
    stream
}
#[tokio::test]
async fn config_and_network_rebuild_real_stacks_on_same_live_source_pipe_after_original_joins() {
    let (endpoint, peer_task) = peer::echo_peer().await;
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let socks = port.local_addr().unwrap().port();
    drop(port);
    let config = linked(endpoint, socks);
    let proxy = Proxy::new(config.clone()).unwrap();
    let handle = ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    );
    let running = proxy.spawn();
    handle
        .apply_runtime_config_and_wait(config.clone(), Duration::from_secs(5))
        .await
        .unwrap();
    let source = proxy.inbound_owners.owner("wg-source").unwrap();
    source.wait_ready().await.unwrap();
    let first = proxy
        .protocols
        .test_packet_source_origins(proxy.engine().config().as_ref())
        .remove(0);
    let mut client = echo(socks).await;
    let mut previous = proxy
        .protocols
        .test_outbound_devices(proxy.engine().config().as_ref());
    assert_eq!(previous.len(), 1);
    for network in [false, true] {
        let joins = previous
            .iter()
            .flat_map(|device| device.task.test_original_witnesses())
            .collect::<Vec<_>>();
        assert!(joins.len() >= 3);
        assert!(joins.iter().all(|join| !join.is_joined()));
        if network {
            proxy.egress_interface_control().invalidate_network();
        } else {
            let mut changed = config.clone();
            changed
                .outbounds
                .iter_mut()
                .find(|outbound| outbound.tag == "wg")
                .unwrap()
                .udp
                .enabled = false;
            if let Err(error) = handle
                .apply_runtime_config_and_wait(changed, Duration::from_secs(5))
                .await
            {
                let actual_source = source.wait_ready().await;
                panic!(
                    "same actual graph replacement failed: {error:?}; original Source readiness: {actual_source:#?}; original worker fault: {:?}; matching joined receipts: {:?}",
                    previous[0].task.first_scoped_failure(),
                    joins.iter().map(|witness| witness.try_joined()).collect::<Vec<_>>(),
                );
            }
        }
        tokio::time::timeout_at(end(), async {
            loop {
                let current = proxy
                    .protocols
                    .test_outbound_devices(proxy.engine().config().as_ref());
                if current.len() == 1
                    && current[0].is_usable()
                    && current[0].incarnation != previous[0].incarnation
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("fresh actual graph after confirmed old users/worker retirement");
        assert!(joins.iter().all(|join| join.is_joined()));
        assert!(previous.iter().all(|device| device.is_closed()));
        assert!(Arc::ptr_eq(
            &source,
            &proxy.inbound_owners.owner("wg-source").unwrap()
        ));
        let mut next = echo(socks).await;
        let current = proxy
            .protocols
            .test_packet_source_origins(proxy.engine().config().as_ref())
            .remove(0);
        assert!(current.same_original(&first));
        assert!(current
            .driver()
            .unwrap()
            .same_registration(first.driver().unwrap()));
        assert!(current.closed().is_none());
        assert!(!current.driver().unwrap().is_joined());
        let mut stopped = [0];
        let result = tokio::time::timeout_at(end(), client.read(&mut stopped))
            .await
            .unwrap();
        assert!(matches!(result, Ok(0)) || result.is_err());
        next.write_all(b"still-live").await.unwrap();
        let mut bytes = [0; 10];
        tokio::time::timeout_at(end(), next.read_exact(&mut bytes))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&bytes, b"still-live");
        client = next;
        previous = proxy
            .protocols
            .test_outbound_devices(proxy.engine().config().as_ref());
    }
    tokio::time::timeout_at(end(), running.shutdown())
        .await
        .unwrap()
        .unwrap();
    let closed = first.closed().unwrap();
    assert!(closed.matches(&first));
    assert!(closed.had_native_acquisition());
    assert!(first.driver().unwrap().is_joined());
    peer_task.abort();
    assert!(peer_task.await.is_err_and(|error| error.is_cancelled()));
}
