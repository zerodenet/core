//! One DNS H2 original borrowing upper MUX and its actual lower cached gRPC pool.
use super::*;
use zero_platform_tokio::controlled_carrier::CarrierOriginOwner;
const ID: &str = "11111111-2222-3333-4444-555555555555";
#[derive(Debug)]
struct NoCompatibility;
impl zero_dns::DnsOutboundConnector for NoCompatibility {
    fn connect(&self, _: String, _: std::net::SocketAddr) -> zero_dns::DnsOutboundConnectFuture {
        Box::pin(async { panic!("ordinary gRPC MUX DNS cannot use the compatibility connector") })
    }
}
fn live_layers(child: &CarrierOrigin) -> (CarrierOrigin, CarrierOrigin) {
    let upper = child.parent().expect("actual upper MUX logical donor");
    let lower = upper.parent().expect("same actual cached gRPC donor");
    assert!(child.native_origin().same_original(lower));
    assert!(lower.parent().is_none());
    assert!(!upper.owner().same_owner(lower.owner()));
    let upper_driver = upper.driver().unwrap();
    let lower_driver = lower.driver().unwrap();
    assert!(!upper_driver.is_joined() && !lower_driver.is_joined());
    assert!(!upper_driver
        .scope_identity()
        .same_scope(&lower_driver.scope_identity()));
    for (origin, native) in [(upper, false), (lower, true)] {
        assert!(origin.closed().is_none());
        let CarrierOriginOwner::Carrier(owner) = origin.owner() else {
            panic!("actual typed Carrier provider, not an observer-only substitute");
        };
        assert_eq!(owner.limit().get(), 1);
        let observation = owner.observe().unwrap();
        assert_eq!(observation.retained(), 1);
        let slot = &observation.slots()[0];
        assert!(slot.acquisition_unknown().is_none());
        assert!(slot
            .driver()
            .unwrap()
            .1
            .same_registration(origin.driver().unwrap()));
        let preparation = slot.auxiliary().expect("real owning preparation Aux");
        assert!(preparation
            .scope_identity()
            .same_scope(&owner.bound().unwrap().scope_identity()));
        let joined = preparation.try_joined().unwrap();
        assert!(preparation.matches_joined(&joined) && joined.registration_released());
        joined.result().unwrap();
        if native {
            assert!(slot.native().is_some_and(|native| !native.is_closed()));
        } else {
            assert!(
                slot.native().is_none(),
                "upper MUX keeps the original lower loan"
            );
            assert!(slot.borrowed_origin().unwrap().same_original(lower));
        }
    }
    (upper.clone(), lower.clone())
}
async fn leases(proxy: &Proxy, count: usize) {
    tokio::time::timeout_at(deadline(), async {
        loop {
            let pool = zero_dns::test_support::doh_pool_witness(&proxy.resolver, "query").unwrap();
            assert_eq!(pool.registered_driver_tasks, 1);
            if pool.active_leases == count {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
fn retired(origin: &CarrierOrigin, native: bool) {
    let closed = origin.closed().unwrap();
    assert!(closed.matches(origin));
    assert_eq!(closed.had_native_acquisition(), native);
    assert!(origin.owner().closed().unwrap().matches(origin.owner()));
    assert!(origin
        .owner()
        .users_closed()
        .unwrap()
        .matches(origin.owner()));
    let driver = origin.driver().unwrap();
    let joined = driver.try_joined().unwrap();
    assert!(driver.matches_joined(&joined) && joined.registration_released());
    joined.result().unwrap();
}
#[tokio::test]
async fn ordinary_cached_grpc_mux_doh_cancellation_keeps_both_actual_drivers_until_host_retirement()
{
    tokio::time::timeout(Duration::from_secs(25), async {
        for multi in [false, true] {
            let kind = if multi { "grpc-multi" } else { "grpc-single" };
            let remote = Remote::new(kind).await;
            let mut tls = crate::dns_fixture::TlsDns::new(true).await;
            let mut backend = tls.server(true);
            let zero_config::DnsServerConfig::Doh { detour, .. } = &mut backend else { unreachable!() };
            *detour = Some("node".into());
            let config = zero_config::RuntimeConfig::parse(&serde_json::json!({
                "runtime":{"native_mux_max_carriers":1,"grpc_pool_max_carriers":1,"dns":{"servers":{"node":{"type":"system"},"query":backend},"default_server":"query","policy":{"node_server":"node","address_family":"ipv4_only"}}},
                "outbounds":[{"tag":"node","protocol":{"type":"vless","server":"127.0.0.1","port":remote.address.port(),"id":ID,"mux_concurrency":8,"xudp_concurrency":8,"grpc":{"service_names":["actual.dns/Tun"],"multi_mode":multi},"tls":{"server_name":"localhost","ca_cert_path":remote.ca,"client_fingerprint":null}}}],
                "route":{"final":{"type":"direct"}}
            }).to_string()).unwrap();
            let proxy = Proxy::new(config.clone()).unwrap();
            proxy.resolver.set_outbound_connector(Arc::new(NoCompatibility));
            let running = proxy.spawn();
            let handle = crate::ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()), proxy.clone());
            handle.apply_runtime_config_and_wait(config, Duration::from_secs(5)).await.unwrap();
            zero_dns::test_support::install_doh_ca(&proxy.resolver, "query", tls.certificate.clone()).unwrap();
            let mut first = query(&proxy, "first.cached.grpc.mux.actual.test");
            let (_, first_answer) = tokio::select! {
                request=tokio::time::timeout_at(deadline(),tls.requests.recv())=>request.unwrap().unwrap(),
                result=&mut first=>panic!("actual first DoH wire did not arrive: {result:?}"),
            };
            let original = pending(&proxy).await;
            let child = original.original.as_ref().unwrap();
            let (upper, lower) = live_layers(child);
            for donor in [&upper, &lower] {
                assert!(!original.task.scope_identity().same_scope(&donor.driver().unwrap().scope_identity()));
            }
            let mut second = query(&proxy, "second.cached.grpc.mux.actual.test");
            let (bytes, answer) = tokio::select! {
                request=tokio::time::timeout_at(deadline(),tls.requests.recv())=>request.unwrap().unwrap(),
                result=&mut second=>panic!("concurrent DoH query failed before its real HTTP stream: {result:?}"),
            };
            leases(&proxy, 2).await;
            assert!(!first.is_finished() && !second.is_finished());
            first.abort(); assert!(first.await.unwrap_err().is_cancelled()); drop(first_answer);
            leases(&proxy, 1).await;
            let (same_upper, same_lower) = live_layers(child);
            assert!(same_upper.same_original(&upper) && same_lower.same_original(&lower));
            answer.send(zero_dns::udp::build_dns_response(&bytes, &[IpAddress::V4([127,0,0,1])])).unwrap();
            assert_eq!(second.await.unwrap().unwrap(), vec![IpAddress::V4([127,0,0,1])]);
            leases(&proxy, 0).await;
            let mut reused = query(&proxy, "reuse.cached.grpc.mux.actual.test");
            let (bytes, answer) = tokio::select! {
                request=tokio::time::timeout_at(deadline(),tls.requests.recv())=>request.unwrap().unwrap(),
                result=&mut reused=>panic!("same actual pool did not serve reuse: {result:?}"),
            };
            let current = pending(&proxy).await;
            assert!(current.original.as_ref().unwrap().same_original(child));
            assert!(current.task.same_registration(&original.task));
            let (same_upper, same_lower) = live_layers(child);
            assert!(same_upper.same_original(&upper) && same_lower.same_original(&lower));
            assert_eq!(tls.connections.load(std::sync::atomic::Ordering::SeqCst), 1);
            answer.send(zero_dns::udp::build_dns_response(&bytes, &[IpAddress::V4([127,0,0,1])])).unwrap();
            reused.await.unwrap().unwrap();
            assert!(proxy.engine().active_sessions().is_empty() && proxy.engine().completed_sessions().is_empty());
            running.shutdown().await.unwrap();
            let joined = original.task.try_joined().unwrap();
            assert!(original.task.matches_joined(&joined) && joined.registration_released());
            joined.result().unwrap();
            returned(child); retired(&upper, false); retired(&lower, true);
            assert_eq!(proxy.protocols.runtime_host().original_budgets().carrier.snapshot().charged, 0);
            remote.close().await; tls.close().await;
        }
    }).await.unwrap();
}
