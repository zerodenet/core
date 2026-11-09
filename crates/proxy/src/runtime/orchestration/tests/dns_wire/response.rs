//! Actual Query ACK does not refund a still-held reply or its shared clone.
use super::*;
fn question() -> Vec<u8> {
    let mut bytes = vec![0x12, 0x34, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0];
    for label in ["held", "wire", "test"] {
        bytes.push(label.len() as u8);
        bytes.extend_from_slice(label.as_bytes());
    }
    bytes.extend_from_slice(&[0, 0, 1, 0, 1]);
    bytes
}
#[tokio::test]
async fn actual_reply_backing_survives_query_host_ack_and_shrink_until_its_final_receiver_returns()
{
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut dns = crate::dns_fixture::Dns::new().await;
        let initial = config(&dns, 262144, None);
        let proxy = Proxy::new(initial.clone()).unwrap();
        let originals = proxy.protocols.runtime_host().original_budgets();
        let book = originals.dns_wire.clone();
        let baseline = book.snapshot().occupied;
        let running = proxy.clone().spawn();
        let handle = ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()), proxy.clone());
        handle.apply_runtime_config_and_wait(initial.clone(), Duration::from_secs(5)).await.unwrap();
        let wire = question();
        let mut answer = Box::pin(proxy.resolver.answer_udp_query(&wire));
        let request = tokio::select! {
            request = dns.request() => request,
            result = &mut answer => panic!("actual original DNS must reach its UDP peer: {result:?}"),
        };
        assert!(!request.tcp);
        let original = proxy.resolver.native_query_observations().into_iter()
            .find(|actual| actual.io_pending).expect("actual receive-pending Query registration");
        request.answer(false, false);
        let response = answer.await.unwrap();
        assert_eq!(u16::from_be_bytes([response[0], response[1]]), 0x1234);
        assert_eq!(response[3] & 0x0f, 0);
        let joined = tokio::time::timeout(Duration::from_secs(3), original.task.wait_registration_released()).await.unwrap();
        assert!(original.task.matches_joined(&joined) && joined.registration_released());
        joined.result().unwrap();
        let held = book.snapshot().occupied;
        assert!(held.bytes() >= baseline.bytes() + response.allocation_bytes());
        let second = response.clone();
        assert_eq!(book.snapshot().occupied, held, "cloning keeps the same original backing");
        drop(response);
        assert_eq!(book.snapshot().occupied, held, "another actual reply receiver remains");
        let mut small = initial.clone();
        small.runtime.dns_wire_total_max_buffered_bytes = Some(baseline.bytes());
        let accepted = proxy.engine().runtime_snapshot();
        assert!(handle.apply_runtime_config_and_wait(small.clone(), Duration::from_secs(5)).await.is_err());
        assert!(Arc::ptr_eq(&accepted, &proxy.engine().runtime_snapshot()));
        assert_eq!(book.snapshot().occupied, held);
        drop(second);
        assert_eq!(book.snapshot().occupied, baseline,
            "the last real receiver frees backing before returning its original credit");
        handle.apply_runtime_config_and_wait(small, Duration::from_secs(5)).await.unwrap();
        let error = proxy.resolver.answer_udp_query(&wire).await.err().unwrap();
        assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
        dns.no_new_request();
        handle.apply_runtime_config_and_wait(initial, Duration::from_secs(5)).await.unwrap();
        let mut answer = Box::pin(proxy.resolver.answer_udp_query(&wire));
        let request = tokio::select! {
            request = dns.request() => request,
            result = &mut answer => panic!("same real Source must recover after committed growth: {result:?}"),
        };
        assert!(!request.tcp); request.answer(false, false);
        let recovered = answer.await.unwrap();
        assert_eq!(recovered[3] & 0x0f, 0); drop(recovered);
        running.shutdown().await.unwrap();
        assert_eq!(originals.carrier.snapshot().charged, 0);
        dns.close().await;
    }).await.unwrap();
}
