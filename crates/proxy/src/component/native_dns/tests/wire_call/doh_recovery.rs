//! The real H2 driver fault is received before a fresh same-Host DNS pool starts.
use super::*;
use std::sync::Arc;
#[tokio::test]
async fn physically_closed_doh_panic_keeps_original_faults_and_recovers_the_actual_backend() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut tls = crate::dns_fixture::TlsDns::new(true).await;
        let config = zero_config::RuntimeConfig::parse(&serde_json::json!({
            "runtime":{"native_dns_max_originals":1,"dns":{"servers":{"wire":tls.server(true)},"default_server":"wire"}},
            "route":{"final":{"type":"direct"}}
        }).to_string()).unwrap();
        let proxy = Proxy::new(config).unwrap();
        let config = proxy.engine().config();
        proxy.protocols.start_components(config.clone(), end()).await.unwrap();
        proxy.protocols.commit_components(&config, || Ok(())).unwrap();
        zero_dns::test_support::install_doh_ca(&proxy.resolver, "wire", tls.certificate.clone()).unwrap();
        let request = proxy.resolver.resolve_real_type("panic.actual.doh.test", 1);
        tokio::pin!(request);
        let (_, answer) = proxy.protocols.drive_component_supervision(async {
            tokio::select! {
                request = tls.requests.recv() => request.unwrap(),
                result = &mut request => panic!("DoH returned before actual HTTP Pending: {result:?}"),
            }
        }).await;
        let before = proxy.resolver.native_query_observations();
        let failed = zero_dns::test_support::panic_original_doh_driver(&proxy.resolver, "wire").unwrap();
        let original = before.into_iter().find(|actual| actual.task.same_registration(&failed)).unwrap();
        assert!(original.io_pending);
        let native = original.original.as_ref().unwrap();
        assert!(native.closed().is_none());
        let plan = proxy.tcp_runtime_services().components.clone().unwrap();
        let canonical = plan.component(native.instance().component).unwrap();
        assert_eq!(canonical.instance_id(), native.instance());
        let joined = proxy.protocols.drive_component_supervision(failed.wait()).await;
        assert!(failed.matches_joined(&joined));
        let panic = joined.result().unwrap_err();
        drop(answer);
        assert!(proxy.protocols.drive_component_supervision(&mut request).await.is_err());
        let received = proxy.protocols.drive_component_supervision(canonical.shutdown(end())).await.unwrap();
        assert!(received.is_complete(), "original physical cleanup: {received:?}");
        assert!(received.failed_tasks.iter().any(|fault| fault.id == joined.id() && fault.class == joined.class()
            && super::recovery::contains_source(&fault.error, &panic)), "same actual driver TaskId/class/error Arc must have a lifecycle receiver: {received:?}");
        assert!(received.resource_failures.is_empty(),
            "waiting for that actual worker's Host ACK must not mint a second protocol cleanup fault: {received:?}");
        assert!(joined.registration_released());
        assert!(native.closed().unwrap().matches(native));
        assert!(native.owner().closed().unwrap().matches(native.owner()));
        assert!(native.owner().users_closed().unwrap().matches(native.owner()));
        assert!(!original.task.scope_identity().resource_open());
        // Closing the canonical native/Scope does not consume its independent
        // cached DoH record. The original reload token must retire that pool.
        assert_eq!(zero_dns::test_support::doh_pool_witness(&proxy.resolver, "wire").unwrap().active_leases, 0);

        // The same original Host constructs the actual fresh query generation.
        // Its exact changed Scope excludes the failed backend from reuse.
        proxy.protocols.start_components(config.clone(), end()).await.unwrap();
        let fresh_plan = proxy.tcp_runtime_services().components.clone().unwrap();
        let fresh = fresh_plan.component(canonical.id).unwrap();
        assert_ne!(fresh.instance_id(), canonical.instance_id());
        let snapshot = proxy.engine().runtime_snapshot();
        let token = proxy.resolver.prepare_reload_resources_with_host_scopes(
            config.runtime.dns.as_ref(), snapshot.dns_dispatch(), &std::collections::BTreeSet::new(),
            |instance, scope| fresh_plan.component(instance.component).is_some_and(|slot| slot.instance_id() == instance && slot.matches_live_scope(scope)),
        ).unwrap();
        proxy.protocols.drive_component_supervision(proxy.resolver.retire_reload_resources(&token, end())).await.unwrap();
        let retired = zero_dns::test_support::doh_pool_witness(&proxy.resolver, "wire").unwrap();
        assert_eq!(retired.registered_driver_tasks, 0);
        assert_eq!(retired.active_leases, 0);
        let retained_panic = joined.result().unwrap_err();
        assert!(Arc::ptr_eq(&retained_panic, &panic));
        assert!(joined.failure_accepted() && joined.registration_released());
        assert!(received.failed_tasks.iter().any(|fault| super::recovery::contains_source(&fault.error, &retained_panic)),
            "successful pool retirement cannot erase its real original fault receiver");
        proxy.protocols.commit_components(&config, || proxy.resolver.commit_reload_resources(&token).map_err(|source| source.to_string())).unwrap();
        // The committed backend is now owned by the real current snapshot.
        // Return this transaction's snapshot before the test-only exclusive CA
        // mutation; keeping it would correctly retain a second backend borrow.
        drop(token);
        zero_dns::test_support::install_doh_ca(&proxy.resolver, "wire", tls.certificate.clone()).unwrap();
        let next = proxy.resolver.resolve_real_type("recovered.actual.doh.test", 1); tokio::pin!(next);
        let (bytes, answer) = proxy.protocols.drive_component_supervision(async {
            tokio::select! {
                request = tls.requests.recv() => request.unwrap(),
                result = &mut next => panic!("fresh same-Host DoH did not reach actual HTTP: {result:?}"),
            }
        }).await;
        let new_original = proxy.resolver.native_query_observations().into_iter()
            .find(|actual| actual.io_pending && actual.original.is_some()).unwrap();
        let new_native = new_original.original.as_ref().unwrap();
        assert_eq!(new_native.instance(), fresh.instance_id());
        assert!(!native.same_original(new_native));
        assert!(!new_original.task.same_registration(&failed));
        assert!(!new_original.task.scope_identity().same_scope(&failed.scope_identity()));
        answer.send(zero_dns::udp::build_dns_response(&bytes, &[zero_traits::IpAddress::V4([203,0,113,19])])).unwrap();
        assert_eq!(proxy.protocols.drive_component_supervision(&mut next).await.unwrap(), vec![zero_traits::IpAddress::V4([203,0,113,19])]);
        proxy.protocols.shutdown_components(end()).await.unwrap();
        assert!(new_native.closed().unwrap().matches(new_native));
        assert!(new_native.owner().closed().unwrap().matches(new_native.owner()));
        assert!(new_native.owner().users_closed().unwrap().matches(new_native.owner()));
        assert!(new_original.task.try_joined().unwrap().registration_released());
        assert!(received.failed_tasks.iter().any(|fault| super::recovery::contains_source(&fault.error, &panic)), "the original receiver survives actual recovery");
        assert!(Arc::strong_count(&panic) > 1);
        tls.close().await;
    }).await.unwrap();
}
