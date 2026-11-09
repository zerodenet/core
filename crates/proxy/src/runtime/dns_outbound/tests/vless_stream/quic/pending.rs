//! The UDP Initial is the actual pre-delivery suspension point, not a delay.
use super::*;
#[tokio::test]
async fn cancelling_before_quic_loan_delivery_waits_for_the_same_native_and_borrowed_originals() {
    tokio::time::timeout(Duration::from_secs(30), async {
        for borrowed in [false, true] {
            let material = Material::new(); let prefix = prefix(borrowed).await;
            let blackhole = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let tls = crate::dns_fixture::TlsDns::new(true).await;
            let (proxy, running) = client(blackhole.local_addr().unwrap(), &material.ca, prefix.as_ref(), tls.server(true)).await;
            zero_dns::test_support::install_doh_ca(&proxy.resolver, "query", tls.certificate.clone()).unwrap();
            let plan = proxy.tcp_runtime_services().components.clone().unwrap(); let slot = plan.configured("node").unwrap();
            assert!(plan.has_metadata_stream_provider(slot.role_id()));
            let mut previous = None;
            let mut prior_connection = None::<Vec<u8>>;
            for index in 0..2 {
                let mut query = query(&proxy, &format!("{index}.pending.quic.test"));
                let mut bytes = [0; 65536];
                let wire_end = deadline();
                loop {
                    let (size, _) = tokio::select! {
                        result=tokio::time::timeout_at(wire_end,blackhole.recv_from(&mut bytes))=>result.unwrap().unwrap(),
                        result=&mut query=>panic!("QUIC query returned before actual Initial transmission: {result:?}"),
                    };
                    assert!(size > 6 && bytes[0] & 0x80 != 0, "real QUIC Initial reached the original peer");
                    let len = bytes[5] as usize; assert!(len >= 8 && len <= 20 && size >= 6 + len);
                    let connection = &bytes[6..6 + len];
                    // The old closed connection may have left another Initial
                    // in the real peer queue. It cannot establish the next
                    // attempt's suspension point or extend its deadline.
                    if prior_connection.as_deref() == Some(connection) { continue; }
                    prior_connection = Some(connection.to_vec()); break;
                }
                assert!(!query.is_finished());
                let actual = tokio::time::timeout_at(deadline(), async { loop { if let Some(actual) = proxy.resolver.native_query_observations().into_iter().find(|actual| actual.io_pending) { break actual; } tokio::task::yield_now().await; } }).await.unwrap();
                assert!(!actual.task.is_joined());
                let metadata = if !borrowed {
                    let metadata = actual.metadata.clone().expect("pre-delivery native Metadata recorded for this actual Query");
                    assert_eq!(metadata.instance(), slot.instance_id());
                    assert!(metadata.closed().is_none() && metadata.registration_observation().released().is_none());
                    Some(metadata)
                } else { None };
                let carrier = if borrowed {
                    let owner = slot.original_carrier_owner(1).unwrap(); let observed = owner.observe().unwrap();
                    assert_eq!(observed.retained(), 1); let row = &observed.slots()[0];
                    assert!(row.native().is_none() && row.borrowed_origin().is_some());
                    assert!(row.original().closed().is_none()); Some((owner, row.original().clone()))
                } else { None };
                query.abort(); assert!(query.await.unwrap_err().is_cancelled());
                let joined = tokio::time::timeout_at(deadline(), actual.task.wait()).await.unwrap(); joined.result().unwrap();
                if let Some(metadata) = metadata {
                    let closed = metadata.closed().expect("query finalizer waits for same original native close");
                    assert!(closed.matches(&metadata) && closed.had_native_acquisition());
                    let observed = metadata.registration_observation(); assert!(observed.released().unwrap().matches(&observed));
                    if let Some(previous) = &previous { assert!(!metadata.same_original(previous)); } previous = Some(metadata);
                }
                if let Some((owner, origin)) = carrier {
                    let closed = origin.closed().expect("query finalizer waits for real borrowed Root settlement");
                    assert!(origin.matches_closed(&closed) && !closed.had_native_acquisition());
                    assert_eq!(owner.observe().unwrap().retained(), 0, "actual N1 cannot refund before physical/user/Scope ACK settlement");
                }
                assert_eq!(tls.connections.load(std::sync::atomic::Ordering::SeqCst), 0, "no delivered DoH stream");
            }
            running.shutdown().await.unwrap();
            assert_eq!(proxy.protocols.runtime_host().original_budgets().carrier.snapshot().charged, 0);
            assert!(proxy.engine().active_sessions().is_empty() && proxy.engine().completed_sessions().is_empty());
            if let Some(prefix) = prefix { prefix.close().await; } tls.close().await;
        }
    }).await.unwrap();
}
