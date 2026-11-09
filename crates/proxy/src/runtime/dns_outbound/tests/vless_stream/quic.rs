//! Ordinary owning QUIC loans enter the existing DNS provider before first IO.
use super::*;
use zero_platform_tokio::controlled_carrier::CarrierOriginOwner;
mod fixture;
use fixture::*;
mod datagram;
mod h3_returned;
mod pending;
#[tokio::test]
async fn ordinary_quic_native_and_borrowed_doh_use_the_same_real_query_and_endpoint_owners() {
    tokio::time::timeout(Duration::from_secs(30), async {
        for borrowed in [false, true] {
            let material = Material::new();
            let remote = Source::start(serde_json::json!({"inbounds":[{"tag":"remote","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"vless","users":[{"id":ID}],"quic":{"cert_path":material.ca,"key_path":material.key}}}],"route":{"final":{"type":"direct"}}}), "remote").await;
            let prefix = prefix(borrowed).await;
            let mut tls = crate::dns_fixture::TlsDns::new(true).await;
            let (proxy, running) = client(remote.address, &material.ca, prefix.as_ref(), tls.server(true)).await;
            zero_dns::test_support::install_doh_ca(&proxy.resolver, "query", tls.certificate.clone()).unwrap();
            let mut first = query(&proxy, "first.real.quic.doh.test");
            let (_, answer) = tokio::select! {
                request=tokio::time::timeout_at(deadline(),tls.requests.recv())=>request.unwrap().unwrap(),
                result=&mut first=>{
                    crate::component::stream_bridge::capacity_debug::books(&proxy);
                    if let Ok(Err(source))=&result {crate::component::stream_bridge::capacity_debug::source("actual DNS before TLS peer",source);}
                    panic!("actual QUIC DNS failed before the declared TLS peer: {result:?}")
                },
            };
            let actual = super::pending(&proxy).await;
            let child = actual.original.as_ref().unwrap();
            let donor = child.parent().expect("actual owning QUIC loan").clone();
            let plan = proxy.tcp_runtime_services().components.clone().unwrap();
            let slot = plan.configured("node").unwrap();
            assert_eq!(donor.instance(), slot.instance_id());
            assert!(plan.has_metadata_stream_provider(slot.role_id()));
            assert!(plan.stream_dependencies_supported(slot.role_id()));
            if !borrowed {
                let metadata = donor.metadata().expect("actual native QUIC Metadata original");
                assert!(donor.driver().is_none() && donor.parent().is_none());
                let initializer = metadata.initializer_registration().unwrap();
                let joined = initializer.wait_registration_released().await;
                assert!(initializer.matches_joined(&joined) && joined.registration_released());
                assert!(!initializer.scope_identity().same_scope(&actual.task.scope_identity()));
                assert!(metadata.closed().is_none() && metadata.registration_observation().released().is_none());
            } else {
                let CarrierOriginOwner::Carrier(owner) = donor.owner() else { panic!("actual borrowed QUIC carrier"); };
                assert!(owner.same_owner(&slot.original_carrier_owner(1).unwrap()));
                let observed = owner.observe().unwrap();
                assert_eq!(observed.retained(), 1);
                assert!(observed.slots()[0].native().is_none());
                assert!(donor.parent().is_some() && observed.slots()[0].borrowed_origin().is_some());
                let driver = donor.driver().expect("real borrowed packet driver");
                assert!(!driver.is_joined() && !driver.scope_identity().same_scope(&actual.task.scope_identity()));
            }
            let mut second = query(&proxy, "second.real.quic.doh.test");
            let (bytes, reply) = tokio::select! {
                request=tokio::time::timeout_at(deadline(),tls.requests.recv())=>request.unwrap().unwrap(),
                result=&mut second=>panic!("actual shared DoH stream failed before wire: {result:?}"),
            };
            let handle = crate::ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()), proxy.clone());
            let mut target = (*proxy.engine().config()).clone(); target.runtime.native_stream_max_originals = Some(2);
            assert!(handle.apply_runtime_config_and_wait(target.clone(), Duration::from_secs(5)).await.is_err(), "a budget replacement cannot cancel either actual pending DNS request");
            assert!(!actual.task.is_joined() && donor.closed().is_none());
            first.abort(); assert!(first.await.unwrap_err().is_cancelled()); drop(answer);
            reply.send(zero_dns::udp::build_dns_response(&bytes, &[IpAddress::V4([127,0,0,1])])).unwrap();
            assert_eq!(second.await.unwrap().unwrap(), vec![IpAddress::V4([127,0,0,1])]);
            assert!(donor.closed().is_none() && !actual.task.is_joined());
            let current = super::pending(&proxy).await;
            assert!(current.task.same_registration(&actual.task));
            assert!(current.original.as_ref().unwrap().same_original(child));
            assert_eq!(tls.connections.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert!(proxy.engine().active_sessions().is_empty() && proxy.engine().completed_sessions().is_empty());
            tokio::time::timeout_at(deadline(), async { loop { if zero_dns::test_support::doh_pool_witness(&proxy.resolver, "query").unwrap().active_leases == 0 { break; } tokio::task::yield_now().await; } }).await.unwrap();
            assert!(proxy.resolver.native_donor_instances().unwrap().contains(&donor.instance()));
            handle.apply_runtime_config_and_wait(target, Duration::from_secs(5)).await.unwrap();
            assert!(donor.closed().unwrap().matches(&donor)); returned(child);
            let old_join = actual.task.try_joined().unwrap(); assert!(old_join.registration_released()); old_join.result().unwrap();
            // Budget replacement constructs a fresh actual DoH backend; its
            // fixture-only trust is intentionally not copied from the retired
            // pool. Keep the SAME CA and real hostname/certificate checks.
            zero_dns::test_support::install_doh_ca(&proxy.resolver, "query", tls.certificate.clone()).unwrap();
            let mut next = query(&proxy, "replacement.real.quic.doh.test");
            let (bytes, reply) = tokio::select! {
                request=tokio::time::timeout_at(deadline(),tls.requests.recv())=>request.unwrap().unwrap(),
                result=&mut next=>panic!("real QUIC DNS did not recover after exact donor replacement: {result:?}"),
            };
            let successor = super::pending(&proxy).await; let next_child = successor.original.as_ref().unwrap();
            let fresh = next_child.parent().unwrap(); assert!(!fresh.same_original(&donor));
            assert!(!successor.task.same_registration(&actual.task));
            assert_eq!(tls.connections.load(std::sync::atomic::Ordering::SeqCst), 2);
            reply.send(zero_dns::udp::build_dns_response(&bytes, &[IpAddress::V4([127,0,0,1])])).unwrap(); next.await.unwrap().unwrap();
            running.shutdown().await.unwrap();
            let next_join = successor.task.try_joined().unwrap(); assert!(next_join.registration_released()); next_join.result().unwrap();
            returned(next_child); assert!(fresh.closed().unwrap().matches(fresh));
            let joined = actual.task.wait().await;
            assert!(actual.task.matches_joined(&joined) && joined.registration_released()); joined.result().unwrap();
            returned(child);
            assert!(donor.closed().unwrap().matches(&donor));
            assert_eq!(donor.closed().unwrap().had_native_acquisition(), !borrowed);
            if let Some(metadata) = donor.metadata() {
                let observed = metadata.registration_observation();
                assert!(observed.released().unwrap().matches(&observed));
            } else {
                let driver = donor.driver().unwrap(); let joined = driver.try_joined().unwrap();
                assert!(driver.matches_joined(&joined) && joined.registration_released()); joined.result().unwrap();
            }
            assert_eq!(proxy.protocols.runtime_host().original_budgets().carrier.snapshot().charged, 0);
            if let Some(prefix) = prefix { prefix.close().await; }
            remote.close().await; tls.close().await;
        }
    }).await.unwrap();
}
