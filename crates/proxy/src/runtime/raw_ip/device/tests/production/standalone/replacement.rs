//! Old unique view, real wire join/ACK and native close precede new root IO.
use super::*;
use std::sync::Arc;

async fn echo(proxy: &Proxy) -> TcpStream {
    let mut client = tokio::time::timeout_at(end(), connect(socks_port(proxy), false))
        .await
        .unwrap();
    client.write_all(b"original-bound").await.unwrap();
    let mut echoed = [0; 14];
    tokio::time::timeout_at(end(), client.read_exact(&mut echoed))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&echoed, b"original-bound");
    client
}
#[tokio::test]
async fn ordinary_standalone_wireguard_rebuilds_after_actual_users_wire_ack_and_native_close() {
    let (endpoint, peer_task) = peer::echo_peer().await;
    let config = configuration(endpoint, false);
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
    let socks = proxy.inbound_owners.owner("socks").unwrap();
    let mut client = echo(&proxy).await;
    let mut previous = proxy
        .protocols
        .test_outbound_devices(proxy.engine().config().as_ref());
    let mut original = proxy
        .protocols
        .test_packet_source_origins(proxy.engine().config().as_ref())
        .remove(0);
    for network in [false, true] {
        let joins = previous[0].task.test_original_witnesses();
        assert!(joins.len() >= 3);
        let old_wire = original.driver().unwrap();
        assert!(!old_wire.is_joined());
        assert!(original.closed().is_none());
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
            handle
                .apply_runtime_config_and_wait(changed, Duration::from_secs(5))
                .await
                .unwrap();
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
        .expect("fresh actual standalone graph after prior original retirement");
        assert!(previous[0].is_closed());
        assert!(joins.iter().all(|join| join.is_joined()));
        assert!(old_wire.try_joined().unwrap().registration_released());
        let closed = original
            .closed()
            .expect("same old native Bound original is closed before new IO");
        assert!(closed.matches(&original));
        assert!(closed.had_native_acquisition());
        assert!(Arc::ptr_eq(
            &socks,
            &proxy.inbound_owners.owner("socks").unwrap()
        ));
        let current = proxy
            .protocols
            .test_packet_source_origins(proxy.engine().config().as_ref())
            .remove(0);
        assert!(!current.same_original(&original));
        assert!(!current.driver().unwrap().same_registration(&old_wire));
        assert!(current.closed().is_none());
        let mut stopped = [0];
        let result = tokio::time::timeout_at(end(), client.read(&mut stopped))
            .await
            .unwrap();
        assert!(matches!(result, Ok(0)) || result.is_err());
        client = echo(&proxy).await;
        original = current;
        previous = proxy
            .protocols
            .test_outbound_devices(proxy.engine().config().as_ref());
    }
    tokio::time::timeout_at(end(), running.shutdown())
        .await
        .unwrap()
        .unwrap();
    let closed = original.closed().unwrap();
    assert!(closed.matches(&original));
    assert!(closed.had_native_acquisition());
    assert!(original
        .driver()
        .unwrap()
        .try_joined()
        .unwrap()
        .registration_released());
    drop(client);
    peer_task.abort();
    assert!(peer_task.await.is_err_and(|error| error.is_cancelled()));
}
