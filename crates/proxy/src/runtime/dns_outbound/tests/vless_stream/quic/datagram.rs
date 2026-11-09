//! UDP DNS uses the installed Metadata donor's Datagram loan, never TCP framing.
use super::*;
#[tokio::test]
async fn ordinary_quic_dns_datagram_cancellation_returns_the_same_original_before_n1_reuse() {
    tokio::time::timeout(Duration::from_secs(25), async {
        let material = Material::new();
        let remote = Source::start(serde_json::json!({"inbounds":[{"tag":"remote","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"vless","users":[{"id":ID}],"quic":{"cert_path":material.ca,"key_path":material.key}}}],"route":{"final":{"type":"direct"}}}), "remote").await;
        let mut dns = crate::dns_fixture::Dns::new().await;
        let (proxy, running) = client(remote.address, &material.ca, None, dns.server()).await;
        let plan = proxy.tcp_runtime_services().components.clone().unwrap();
        let slot = plan.configured("node").unwrap();
        assert!(plan.has_metadata_stream_provider(slot.role_id()));
        assert!(!plan.has_device_carrier_provider(slot.role_id()));
        let owner = slot.original_metadata_owner(0).unwrap();
        assert_eq!(owner.limit().get(), 1);
        let mut originals = Vec::new();
        for (index, cancel) in [false, true, false].into_iter().enumerate() {
            let mut query = query(&proxy, &format!("{index}.ordinary.quic.udp.dns.test"));
            let request = tokio::select! {
                request = dns.request() => request,
                result = &mut query => panic!("Metadata Datagram DNS failed before actual UDP: {result:?}"),
            };
            assert!(!request.tcp, "the standard Datagram role must send actual UDP DNS");
            let actual = super::super::pending(&proxy).await;
            assert!(!actual.task.is_joined());
            let child = actual.original.as_ref().unwrap();
            let donor = child.parent().expect("the actual Query child keeps its selected donor");
            assert!(child.closed().is_none());
            let metadata = donor.metadata().expect("actual same-slot Metadata origin").clone();
            assert_eq!(donor.instance(), slot.instance_id());
            assert!(donor.driver().is_none() && donor.parent().is_none());
            assert!(owner.current_original().unwrap().same_original(&metadata));
            assert!(actual.metadata.as_ref().unwrap().same_original(&metadata));
            assert!(metadata.closed().is_none());
            let initializer = metadata.initializer_registration().unwrap();
            let joined = tokio::time::timeout_at(deadline(), initializer.wait_registration_released()).await.unwrap();
            assert!(initializer.matches_joined(&joined) && joined.registration_released());
            joined.result().unwrap();
            assert!(!initializer.scope_identity().same_scope(&actual.task.scope_identity()));
            if cancel {
                query.abort();
                assert!(query.await.unwrap_err().is_cancelled());
                drop(request);
            } else {
                request.answer(false, false);
                assert_eq!(query.await.unwrap().unwrap(), vec![IpAddress::V4([127, 0, 0, 1])]);
            }
            let joined = tokio::time::timeout_at(deadline(), actual.task.wait_registration_released()).await.unwrap();
            assert!(actual.task.matches_joined(&joined) && joined.registration_released());
            joined.result().unwrap();
            returned(child);
            let closed = metadata.closed().expect("query finalizer waits for its actual native");
            assert!(closed.matches(&metadata) && closed.had_native_acquisition());
            let registrations = metadata.registration_observation();
            assert!(registrations.released().unwrap().matches(&registrations));
            for previous in &originals {
                assert!(!metadata.same_original(previous), "a fresh query acquires a fresh original after return");
            }
            originals.push(metadata);
            dns.no_new_request();
        }
        assert!(proxy.engine().active_sessions().is_empty() && proxy.engine().completed_sessions().is_empty());
        running.shutdown().await.unwrap();
        assert!(owner.closed().unwrap().matches(&owner));
        assert_eq!(proxy.protocols.runtime_host().original_budgets().carrier.snapshot().charged, 0);
        for original in originals {
            assert!(original.closed().unwrap().matches(&original));
            let registrations = original.registration_observation();
            assert!(registrations.released().unwrap().matches(&registrations));
        }
        remote.close().await;
        dns.close().await;
    }).await.unwrap();
}
