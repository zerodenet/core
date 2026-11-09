//! A returned DNS Loan does not retire the cached H3 physical original.
use super::*;
#[tokio::test]
async fn h3_query_cancellation_returns_its_loan_and_ack_while_the_same_cached_original_survives() {
    tokio::time::timeout(Duration::from_secs(30), async {
        for borrowed in [false, true] {
            let material = Material::new();
            let remote = Source::start(serde_json::json!({"inbounds":[{"tag":"remote","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"vless","users":[{"id":ID}],"quic":{"cert_path":material.ca,"key_path":material.key},"split_http":{"path":"/query-original/","mode":"auto"}}}],"route":{"final":{"type":"direct"}}}), "remote").await;
            let prefix = prefix(borrowed).await;
            let mut dns = crate::dns_fixture::Dns::new().await;
            let (proxy, running) = client_with_mode(remote.address, &material.ca, prefix.as_ref(), dns.server(), Some("stream-one")).await;
            let plan = proxy.tcp_runtime_services().components.clone().unwrap();
            let slot = plan.configured("node").unwrap();
            assert!(plan.has_metadata_stream_provider(slot.role_id()));
            let mut previous = None::<zero_platform_tokio::controlled_carrier::CarrierOrigin>;
            for (index, cancel) in [true, false, false].into_iter().enumerate() {
                let mut query = query(&proxy, &format!("{index}.cached.h3.query.test"));
                let request = tokio::select! {
                    request = dns.request() => request,
                    result = &mut query => panic!("cached H3 DNS failed before actual UDP: {result:?}"),
                };
                assert!(!request.tcp);
                let actual = super::super::pending(&proxy).await;
                let child = actual.original.as_ref().unwrap();
                let donor = child.parent().expect("the actual H3 original").clone();
                assert_eq!(donor.instance(), slot.instance_id());
                if let Some(previous) = &previous {
                    assert!(donor.same_original(previous), "logical Query return must reuse the same physical H3 original");
                }
                assert!(donor.closed().is_none() && !actual.task.is_joined());
                if cancel {
                    query.abort(); assert!(query.await.unwrap_err().is_cancelled()); drop(request);
                } else {
                    request.answer(false, false);
                    assert_eq!(query.await.unwrap().unwrap(), vec![IpAddress::V4([127,0,0,1])]);
                }
                let joined = tokio::time::timeout_at(deadline(), actual.task.wait_registration_released()).await.unwrap();
                assert!(actual.task.matches_joined(&joined) && joined.registration_released());
                joined.result().unwrap();
                returned(child);
                assert!(donor.closed().is_none(), "Query ACK is independent of cached physical native closure");
                assert!(proxy.protocols.runtime_host().original_budgets().carrier.snapshot().charged > 0);
                previous = Some(donor);
                dns.no_new_request();
            }
            let donor = previous.unwrap();
            running.shutdown().await.unwrap();
            assert!(donor.closed().unwrap().matches(&donor));
            if let Some(metadata) = donor.metadata() {
                let registrations = metadata.registration_observation();
                assert!(registrations.released().unwrap().matches(&registrations));
            } else {
                let driver = donor.driver().unwrap();
                let joined = driver.try_joined().unwrap();
                assert!(driver.matches_joined(&joined) && joined.registration_released()); joined.result().unwrap();
            }
            assert_eq!(proxy.protocols.runtime_host().original_budgets().carrier.snapshot().charged, 0);
            assert!(proxy.engine().active_sessions().is_empty() && proxy.engine().completed_sessions().is_empty());
            if let Some(prefix) = prefix { prefix.close().await; }
            remote.close().await; dns.close().await;
        }
    }).await.unwrap();
}
