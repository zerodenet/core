//! Same existing physical MUX pool and actual TLS material, no second DNS pool.
use super::*;
use zero_platform_tokio::controlled_carrier::CarrierOriginOwner;
#[derive(Debug)]
struct NoCompatibility;
impl zero_dns::DnsOutboundConnector for NoCompatibility {
    fn connect(&self, _: String, _: std::net::SocketAddr) -> zero_dns::DnsOutboundConnectFuture {
        Box::pin(async {
            panic!("ordinary material MUX DNS cannot use the compatibility connector")
        })
    }
}
fn physical(child: &CarrierOrigin) -> CarrierOrigin {
    let donor = child.parent().expect("actual MUX logical donor");
    assert!(
        donor.parent().is_none(),
        "these transports are inline in the original MUX driver"
    );
    assert!(child.native_origin().same_original(donor));
    assert!(donor.closed().is_none());
    let driver = donor.driver().expect("actual shared MUX physical driver");
    assert!(!driver.is_joined());
    let CarrierOriginOwner::Carrier(owner) = donor.owner() else {
        panic!("actual same typed MUX provider");
    };
    assert_eq!(owner.limit().get(), 1);
    assert!(driver
        .scope_identity()
        .same_scope(&owner.bound().unwrap().scope_identity()));
    let observation = owner.observe().unwrap();
    assert_eq!(observation.retained(), 1);
    let slot = &observation.slots()[0];
    assert!(slot.acquisition_unknown().is_none());
    assert!(slot.native().is_some_and(|native| !native.is_closed()));
    assert!(slot.borrowed_origin().is_none());
    assert!(slot.driver().unwrap().1.same_registration(driver));
    let setup = slot.auxiliary().expect("real owning preparation");
    assert!(setup
        .scope_identity()
        .same_scope(&owner.bound().unwrap().scope_identity()));
    let joined = setup.try_joined().unwrap();
    assert!(setup.matches_joined(&joined) && joined.registration_released());
    joined.result().unwrap();
    donor.clone()
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
fn retired(donor: &CarrierOrigin) {
    let closed = donor.closed().unwrap();
    assert!(closed.matches(donor) && closed.had_native_acquisition());
    assert!(donor.owner().closed().unwrap().matches(donor.owner()));
    assert!(donor.owner().users_closed().unwrap().matches(donor.owner()));
    let driver = donor.driver().unwrap();
    let joined = driver.try_joined().unwrap();
    assert!(driver.matches_joined(&joined) && joined.registration_released());
    joined.result().unwrap();
}
#[tokio::test]
async fn ordinary_full_material_mux_doh_cancellation_preserves_the_same_physical_and_query_owners()
{
    tokio::time::timeout(Duration::from_secs(25), async {
        for (flavor, kind) in [
            ("vless", "raw"), ("vless", "ws"), ("vless", "h2"), ("vless", "http_upgrade"),
            ("vmess", "raw"), ("vmess", "ws"), ("vmess", "grpc-single"), ("vmess", "grpc-multi"),
            ("trojan", "raw"), ("trojan", "ws"), ("trojan", "grpc-single"), ("trojan", "grpc-multi"),
        ] {
            let remote = Remote::for_protocol(flavor, kind).await;
            let mut tls = crate::dns_fixture::TlsDns::new(true).await;
            let mut backend = tls.server(true);
            let zero_config::DnsServerConfig::Doh { detour, .. } = &mut backend else { unreachable!() };
            *detour = Some("node".into());
            let mut protocol = if flavor == "trojan" {
                serde_json::json!({"type":flavor,"server":"127.0.0.1","port":remote.address.port(),"password":"actual-dns-secret","sni":"localhost","insecure":true,"mux_concurrency":8})
            } else {
                serde_json::json!({"type":flavor,"server":"127.0.0.1","port":remote.address.port(),"id":"11111111-2222-3333-4444-555555555555","mux_concurrency":8,"tls":{"server_name":"localhost","ca_cert_path":remote.ca,"client_fingerprint":null}})
            };
            if flavor == "vless" { protocol["xudp_concurrency"] = serde_json::json!(8); }
            match kind {
                "raw" => {},
                "grpc-single" | "grpc-multi" => protocol["grpc"] = serde_json::json!({"service_names":["actual.dns/Tun"],"multi_mode":kind=="grpc-multi"}),
                kind => protocol[kind] = serde_json::json!({"host":"front.actual.test","path":"/actual-dns"}),
            }
            let config = zero_config::RuntimeConfig::parse(&serde_json::json!({
                "runtime":{"native_mux_max_carriers":1,"dns":{"servers":{"node":{"type":"system"},"query":backend},"default_server":"query","policy":{"node_server":"node","address_family":"ipv4_only"}}},
                "outbounds":[{"tag":"node","protocol":protocol}],"route":{"final":{"type":"direct"}}
            }).to_string()).unwrap();
            let proxy = Proxy::new(config.clone()).unwrap();
            proxy.resolver.set_outbound_connector(Arc::new(NoCompatibility));
            let running = proxy.spawn();
            let handle = crate::ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()), proxy.clone());
            handle.apply_runtime_config_and_wait(config, Duration::from_secs(5)).await.unwrap();
            zero_dns::test_support::install_doh_ca(&proxy.resolver,"query",tls.certificate.clone()).unwrap();
            let mut first = query(&proxy, &format!("first.{flavor}.{kind}.mux.actual.test"));
            let (_, first_answer) = tokio::select! {
                request=tokio::time::timeout_at(deadline(),tls.requests.recv())=>request.unwrap().unwrap(),
                result=&mut first=>panic!("{flavor}/{kind}: first real HTTP did not arrive: {result:?}"),
            };
            let original = pending(&proxy).await;
            let child = original.original.as_ref().unwrap();
            let donor = physical(child);
            assert!(!original.task.scope_identity().same_scope(&donor.driver().unwrap().scope_identity()));
            let mut second = query(&proxy, &format!("second.{flavor}.{kind}.mux.actual.test"));
            let (bytes, answer) = tokio::select! {
                request=tokio::time::timeout_at(deadline(),tls.requests.recv())=>request.unwrap().unwrap(),
                result=&mut second=>panic!("{flavor}/{kind}: second real HTTP did not arrive: {result:?}"),
            };
            leases(&proxy,2).await;
            assert!(!first.is_finished() && !second.is_finished());
            first.abort(); assert!(first.await.unwrap_err().is_cancelled()); drop(first_answer);
            leases(&proxy,1).await;
            assert!(physical(child).same_original(&donor));
            answer.send(zero_dns::udp::build_dns_response(&bytes,&[IpAddress::V4([127,0,0,1])])).unwrap();
            assert_eq!(second.await.unwrap().unwrap(),vec![IpAddress::V4([127,0,0,1])]);
            leases(&proxy,0).await;
            let mut reuse = query(&proxy, &format!("reuse.{flavor}.{kind}.mux.actual.test"));
            let (bytes,answer) = tokio::select! {
                request=tokio::time::timeout_at(deadline(),tls.requests.recv())=>request.unwrap().unwrap(),
                result=&mut reuse=>panic!("{flavor}/{kind}: same pool reuse failed: {result:?}"),
            };
            let current = pending(&proxy).await;
            assert!(current.task.same_registration(&original.task));
            assert!(current.original.as_ref().unwrap().same_original(child));
            assert!(physical(child).same_original(&donor));
            assert_eq!(tls.connections.load(std::sync::atomic::Ordering::SeqCst),1);
            answer.send(zero_dns::udp::build_dns_response(&bytes,&[IpAddress::V4([127,0,0,1])])).unwrap();
            reuse.await.unwrap().unwrap();
            leases(&proxy,0).await;
            running.shutdown().await.unwrap();
            let joined = original.task.try_joined().unwrap();
            assert!(original.task.matches_joined(&joined) && joined.registration_released());
            joined.result().unwrap(); returned(child); retired(&donor);
            assert!(child.owner().closed().unwrap().matches(child.owner()));
            assert!(child.owner().users_closed().unwrap().matches(child.owner()));
            assert!(proxy.engine().active_sessions().is_empty() && proxy.engine().completed_sessions().is_empty());
            assert_eq!(proxy.protocols.runtime_host().original_budgets().carrier.snapshot().charged,0);
            remote.close().await; tls.close().await;
        }
    }).await.unwrap();
}
