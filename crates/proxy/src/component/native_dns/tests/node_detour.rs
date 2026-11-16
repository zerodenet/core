//! Node detours consume real standard roles, not an anonymous resolver socket.
use crate::Proxy;
use std::{
    future::{poll_fn, Future},
    task::Poll,
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_platform_tokio::contract::{
    dns::{DnsQueryContext, DnsQueryRole, DnsRequest},
    CancellationToken,
};
use zero_traits::IpAddress;
fn end() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(5)
}
#[tokio::test]
async fn configured_direct_node_udp_retains_originals_and_joins_on_response_and_cancel() {
    tokio::time::timeout(Duration::from_secs(25), async {
        for cancelled in [false, true] {
            run(false, cancelled).await;
        }
    })
    .await
    .unwrap();
}
#[cfg(feature = "socks5")]
#[tokio::test]
async fn node_tcp_detour_borrows_actual_socks_original_and_cannot_dial_dns_directly() {
    tokio::time::timeout(Duration::from_secs(25), async {
        for cancelled in [false, true] {
            run(true, cancelled).await;
        }
    })
    .await
    .unwrap();
}
async fn run(socks: bool, cancelled: bool) {
    let udp = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let destination = if socks { "203.0.113.53" } else { "127.0.0.1" };
    let port = if socks {
        53
    } else {
        udp.local_addr().unwrap().port()
    };
    let outbound = if socks {
        serde_json::json!({"tag":"donor","protocol":{"type":"socks5","server":"127.0.0.1","port":tcp.local_addr().unwrap().port()}})
    } else {
        serde_json::json!({"tag":"donor","protocol":{"type":"direct"},"dial":{"source_ip":"127.0.0.1"}})
    };
    let config = zero_config::RuntimeConfig::parse(&serde_json::json!({
        "runtime":{"native_dns_max_originals":4,"dns":{"servers":{
            "node":{"type":if socks {"tcp"} else {"udp"},"host":destination,"port":port,"detour":"donor"},
            "unused":{"type":"system"}},"default_server":"unused","policy":{"node_server":"node"}}},
        "outbounds":[outbound],"route":{"final":{"type":"direct"}}
    }).to_string()).unwrap();
    assert!(crate::adapters::node_dns::validate(&config).is_ok());
    let proxy = Proxy::new(config.clone()).unwrap();
    let running = proxy.spawn();
    let handle = crate::ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    );
    handle
        .apply_runtime_config_and_wait(config, Duration::from_secs(5))
        .await
        .unwrap();
    let node = proxy.resolver.native_node_service();
    let cancel = CancellationToken::new();
    let mut request = DnsRequest::new("actual.node.detour.test", DnsQueryRole::Node);
    request.family = zero_traits::AddressFamily::OnlyIpv4;
    let mut work = Box::pin(node.resolve(
        request,
        DnsQueryContext {
            deadline: Some(end()),
            cancel: cancel.clone(),
            expected_generation: Some(node.generation()),
        },
    ));
    let (bytes, response) = tokio::sync::oneshot::channel();
    let (answer, delivered) = tokio::sync::oneshot::channel::<Option<Vec<u8>>>();
    let peer = tokio::spawn(async move {
        if socks {
            let (mut stream, _) = tcp.accept().await.unwrap();
            let mut greeting = [0; 2];
            stream.read_exact(&mut greeting).await.unwrap();
            assert_eq!(greeting[0], 5);
            let mut methods = vec![0; greeting[1] as usize];
            stream.read_exact(&mut methods).await.unwrap();
            assert!(methods.contains(&0));
            stream.write_all(&[5, 0]).await.unwrap();
            let mut connect = [0; 10];
            stream.read_exact(&mut connect).await.unwrap();
            assert_eq!(connect, [5, 1, 0, 1, 203, 0, 113, 53, 0, 53]);
            stream
                .write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 0])
                .await
                .unwrap();
            let size = stream.read_u16().await.unwrap() as usize;
            let mut query = vec![0; size];
            stream.read_exact(&mut query).await.unwrap();
            bytes.send(query).unwrap();
            if let Some(response) = delivered.await.unwrap() {
                stream.write_u16(response.len() as u16).await.unwrap();
                stream.write_all(&response).await.unwrap();
            }
            let mut tail = [0; 1];
            assert_eq!(stream.read(&mut tail).await.unwrap(), 0);
        } else {
            let mut query = vec![0; 512];
            let (size, source) = udp.recv_from(&mut query).await.unwrap();
            query.truncate(size);
            bytes.send(query).unwrap();
            if let Some(response) = delivered.await.unwrap() {
                udp.send_to(&response, source).await.unwrap();
            }
        }
    });
    let query = tokio::select! {
        query=response=>query.unwrap(),
        result=&mut work=>panic!("Node detour returned before original wire I/O: {result:?}"),
    };
    assert_eq!(
        zero_dns::udp::parse_dns_question(&query).unwrap().domain,
        "actual.node.detour.test"
    );
    poll_fn(|cx| {
        assert!(work.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let actual = loop {
        if let Some(actual) = proxy
            .resolver
            .native_query_observations()
            .into_iter()
            .find(|entry| entry.io_pending && entry.original.is_some())
        {
            break actual;
        }
        tokio::task::yield_now().await;
    };
    let child = actual.original.as_ref().unwrap();
    let native = child.native_origin().clone();
    assert!(!actual.task.is_joined());
    assert!(child.closed().is_none());
    assert!(native.closed().is_none());
    let plan = proxy.tcp_runtime_services().components.clone().unwrap();
    let donor = plan.configured("donor").unwrap();
    assert_eq!(native.instance(), donor.instance_id());
    if cancelled {
        cancel.cancel();
        assert!(work.await.is_err());
        answer.send(None).unwrap();
    } else {
        answer
            .send(Some(zero_dns::udp::build_dns_response(
                &query,
                &[IpAddress::V4([203, 0, 113, 9])],
            )))
            .unwrap();
        assert_eq!(
            work.await.unwrap().addresses,
            vec![IpAddress::V4([203, 0, 113, 9])]
        );
    }
    let joined = tokio::time::timeout_at(end(), actual.task.wait_registration_released())
        .await
        .unwrap();
    assert!(actual.task.matches_joined(&joined) && joined.registration_released());
    joined.result().unwrap();
    assert!(child.closed().unwrap().matches(child));
    assert!(
        proxy.engine().active_sessions().is_empty()
            && proxy.engine().completed_sessions().is_empty()
    );
    running.shutdown().await.unwrap();
    peer.await.unwrap();
    let closed = native.closed().expect("original native controller receipt");
    assert!(closed.matches(&native) && closed.had_native_acquisition());
    assert!(native.owner().closed().unwrap().matches(native.owner()));
    assert!(native
        .owner()
        .users_closed()
        .unwrap()
        .matches(native.owner()));
    assert!(!actual.task.scope_identity().resource_open());
    assert_eq!(
        proxy
            .protocols
            .runtime_host()
            .original_budgets()
            .carrier
            .snapshot()
            .charged,
        0
    );
}

#[cfg(feature = "socks5")]
#[tokio::test]
async fn actual_node_udp_domain_bnd_cycle_closes_its_original_control_without_recursive_dial() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = zero_config::RuntimeConfig::parse(&serde_json::json!({
            "runtime":{"dns":{"servers":{"node":{"type":"udp","host":"203.0.113.53","port":53,"detour":"donor"}},"default_server":"node","policy":{"address_family":"ipv4_only"}}},
            "outbounds":[{"tag":"donor","protocol":{"type":"socks5","server":"127.0.0.1","port":listener.local_addr().unwrap().port()}}],
            "route":{"final":{"type":"direct"}}
        }).to_string()).unwrap();
        let proxy = Proxy::new(config.clone()).unwrap();
        let running = proxy.spawn();
        let handle = crate::ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()),proxy.clone());
        handle.apply_runtime_config_and_wait(config,Duration::from_secs(5)).await.unwrap();
        let node = proxy.resolver.native_node_service();
        let mut request = DnsRequest::new("recursive.node.detour.test",DnsQueryRole::Node);
        request.family=zero_traits::AddressFamily::OnlyIpv4;
        let mut work = Box::pin(node.resolve(request,DnsQueryContext{deadline:Some(end()),cancel:CancellationToken::new(),expected_generation:Some(node.generation())}));
        let accept = async {
            let (tcp,_) = listener.accept().await.unwrap();
            let mut control=zero_platform_tokio::TokioSocket::new(tcp);
            assert!(matches!(socks5::Socks5Inbound.accept_command(&mut control).await.unwrap(),socks5::Socks5Request::UdpAssociate(_)));
            control
        };
        let mut control=tokio::select! {
            control=accept=>control,
            result=&mut work=>panic!("Node cycle must retain its real original control: {result:?}"),
        };
        let owner=proxy.tcp_runtime_services().components.as_ref().unwrap().configured("donor").unwrap().original_carrier_owner(0).unwrap();
        let actual=owner.observe().unwrap();
        assert_eq!(actual.slots().len(),1,"the real TCP control is installed before BND can require Node resolution");
        let original=actual.slots()[0].original().clone();
        assert!(actual.slots()[0].native().is_some() && original.closed().is_none());
        poll_fn(|cx| {assert!(work.as_mut().poll(cx).is_pending());Poll::Ready(())}).await;
        socks5::Socks5Inbound.send_success_response_with_bound(&mut control,&zero_core::Address::Domain("recursive.bound.test".into()),53).await.unwrap();
        let error=tokio::select! {
            result=&mut work=>result.unwrap_err(),
            repeated=listener.accept()=>panic!("Node bootstrap recursed into another physical control: {repeated:?}"),
        };
        let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
        let mut recursive = false;
        while let Some(actual) = source {
            recursive |= actual.to_string().contains("recursive");
            source = actual.source();
        }
        assert!(recursive,"original bootstrap failure survives Query cleanup: {error:?}");
        running.shutdown().await.unwrap();
        let closed=original.closed().expect("same original TCP native close receipt");
        assert!(original.matches_closed(&closed) && closed.had_native_acquisition());
        assert!(owner.closed().unwrap().matches(&owner));
        assert!(owner.users_closed().unwrap().matches(&owner));
        assert_eq!(proxy.protocols.runtime_host().original_budgets().carrier.snapshot().charged,0);
    }).await.unwrap();
}
