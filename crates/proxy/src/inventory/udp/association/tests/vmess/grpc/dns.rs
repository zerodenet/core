//! Ordinary Proxy domain -> actual host DNS -> trusted TLS/H2 -> business UDP.
use super::*;
use crate::dns_fixture::Dns;
#[path = "dns/fixtures.rs"]
mod fixtures;
use fixtures::*;
#[path = "dns/admission.rs"]
mod admission;
#[path = "dns/network.rs"]
mod network;
#[path = "dns/replacement.rs"]
mod replacement;
#[tokio::test]
async fn ordinary_vmess_grpc_domain_native_dns_tcp_fallback_and_trusted_tls_preserve_udp_accounting(
) {
    timeout(Duration::from_secs(20), async {
        for multi in [false, true] {
            let mut dns = Dns::new().await;
            let endpoint = Endpoint::new().await;
            let config = endpoint.config(&dns, multi);
            let (proxy, running, _) = start_domain(config).await;
            let server = endpoint.serve(multi, 1, vec![b"owned".to_vec()]);
            let opening = tokio::spawn(open(proxy.clone(), 1, b"owned"));
            let request = dns.request().await;
            assert!(!request.tcp);
            let udp = pending(&proxy).await;
            request.answer(true, false);
            let request = dns.request().await;
            assert!(request.tcp);
            let first = udp.original.as_ref().unwrap();
            assert!(first.closed().unwrap().matches(first));
            let tcp = pending(&proxy).await;
            assert!(udp.task.same_registration(&tcp.task));
            request.answer(false, false);
            let dispatch = opening.await.unwrap();
            tcp.task.wait().await;
            dns_closed(&tcp);
            dns_closed(&udp);
            let sessions = proxy.engine().active_sessions();
            assert_eq!(sessions.len(), 1);
            let session = &sessions[0];
            // Flow path reports actual IP; completion retains the configured hostname.
            assert_eq!(session.path.remote.as_ref().unwrap().host, "127.0.0.1");
            assert_eq!(session.outbound_tx_bytes, 14);
            assert_eq!(session.outbound_rx_bytes, 5);
            let registration = original(&proxy);
            let observed = registration.observe().unwrap();
            let native = observed.slots()[0].original().clone();
            let driver = observed.slots()[0].driver().unwrap().1.clone();
            retire(&proxy, dispatch).await;
            assert!(driver.is_joined());
            assert!(native.matches_closed(&native.closed().unwrap()));
            server.await.unwrap();
            running.shutdown().await.unwrap();
            assert!(registration.closed().unwrap().matches(&registration));
            assert!(first.owner().closed().unwrap().matches(first.owner()));
            dns.close().await;
        }
    })
    .await
    .unwrap();
}

#[path = "dns/candidates.rs"]
mod candidates;
