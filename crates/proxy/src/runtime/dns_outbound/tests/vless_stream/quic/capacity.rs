//! A SOCKS TCP-control/UDP pair never fits into one actual original credit.
use super::*;
#[tokio::test]
async fn one_actual_original_rejects_borrowed_quic_before_any_prefix_native_acquisition() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let material = Material::new();
        let remote = Source::start(serde_json::json!({"inbounds":[{"tag":"remote","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"vless","users":[{"id":ID}],"quic":{"cert_path":material.ca,"key_path":material.key}}}],"route":{"final":{"type":"direct"}}}), "remote").await;
        let prefix = prefix(true).await.unwrap();
        let prefix_owner = prefix.original.clone();
        let tls = crate::dns_fixture::TlsDns::new(true).await;
        let (proxy, running) = client_with_original_limit(remote.address, &material.ca, Some(&prefix), tls.server(true), None, 1).await;
        zero_dns::test_support::install_doh_ca(&proxy.resolver, "query", tls.certificate.clone()).unwrap();
        let failure = query(&proxy, "one.credit.borrowed.quic.test").await.unwrap().unwrap_err();
        let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&failure);
        let mut capacity = false;
        while let Some(actual) = source {
            if let Some(error) = actual.downcast_ref::<zero_platform_tokio::contract::ChannelError>() {
                capacity |= error.stage == zero_platform_tokio::contract::Stage::Prepare
                    && error.reason == zero_platform_tokio::contract::Reason::Capacity
                    && error.delivery == zero_platform_tokio::contract::DeliveryState::NotSubmitted;
            }
            source = actual.source();
        }
        assert!(capacity, "retain actual pre-I/O pair admission error: {failure:?}");
        let plan = proxy.tcp_runtime_services().components.clone().unwrap();
        let owner = plan.configured("prefix").unwrap().original_carrier_owner(0).unwrap();
        assert_eq!(owner.limit().get(), 1);
        assert_eq!(owner.observe().unwrap().retained(), 0, "no half-pair reservation or native acquisition");
        assert_eq!(proxy.protocols.runtime_host().original_budgets().carrier.snapshot().charged, 0);
        assert!(proxy.engine().active_sessions().is_empty() && proxy.engine().completed_sessions().is_empty());
        running.shutdown().await.unwrap();
        prefix.close().await;
        let closed = prefix_owner.terminal_for_test().unwrap();
        assert_eq!(closed.joined_tasks(), 1, "only the actual listening Source; no accepted TCP control");
        assert_eq!(closed.closed_io_receipts(), 1, "original native listener only");
        remote.close().await;
    }).await.unwrap();
}
