//! Actual Source/workspaces stay distinct from the opaque outer native ancestor.
use super::*;
mod fixture;
use fixture::Client;

#[tokio::test]
async fn standalone_outer_dns_udp_cancel_and_tc_consume_actual_software_provider_and_query_ack() {
    tokio::time::timeout(Duration::from_secs(25), async {
        let mut dns=crate::dns_fixture::Dns::new().await;
        let peer=peer::Peer::new(dns.address).await;
        let client=Client::start(&peer,dns.server()).await;
        let mut children=Vec::new(); let mut queries=Vec::new();
        for (index,cancel) in [false,true,false].into_iter().enumerate() {
            let mut work=query(&client.proxy,&format!("{index}.software.outer.dns.test"));
            let request=tokio::select! {
                request=dns.request()=>request,
                result=&mut work=>panic!("actual software WG DNS failed before UDP: {result:?}"),
            };
            assert!(!request.tcp);
            let actual=pending(&client.proxy).await;
            client.logical_pending(&actual);
            let child=actual.original.as_ref().unwrap();
            if cancel {
                assert!(!work.is_finished());
                work.abort(); assert!(work.await.unwrap_err().is_cancelled()); drop(request);
            } else {
                request.answer(index==2,false);
                if index==2 {
                    let request=tokio::select! {
                        request=dns.request()=>request,
                        result=&mut work=>panic!("same actual software WG TC fallback failed: {result:?}"),
                    };
                    assert!(request.tcp); returned(child);
                    let next=pending(&client.proxy).await;
                    assert!(next.task.same_registration(&actual.task));
                    assert!(!next.original.as_ref().unwrap().same_original(child));
                    client.logical_pending(&next);
                    request.answer(false,false);
                    assert_eq!(work.await.unwrap().unwrap(),vec![IpAddress::V4([127,0,0,1])]);
                    let joined=tokio::time::timeout_at(end(),next.task.wait_registration_released()).await.unwrap();
                    assert!(next.task.matches_joined(&joined) && joined.registration_released()); joined.result().unwrap();
                    returned(next.original.as_ref().unwrap()); children.push(next.original.unwrap());
                } else {
                    assert_eq!(work.await.unwrap().unwrap(),vec![IpAddress::V4([127,0,0,1])]);
                }
            }
            let joined=tokio::time::timeout_at(end(),actual.task.wait_registration_released()).await.unwrap();
            assert!(actual.task.matches_joined(&joined) && joined.registration_released()); joined.result().unwrap();
            returned(child); children.push(child.clone()); queries.push(actual.task.clone());
            assert!(client.original.closed().is_none()); dns.no_new_request();
        }
        client.close(&children,&queries).await;
        peer.close().await; dns.close().await;
    }).await.unwrap();
}

#[tokio::test]
async fn standalone_outer_doh_shares_real_pool_while_one_query_cancels_and_physical_debt_stays_unknown(
) {
    tokio::time::timeout(Duration::from_secs(25), async {
        let mut tls = crate::dns_fixture::TlsDns::new(true).await;
        let peer = peer::Peer::new(tls.address).await;
        let client = Client::start(&peer, tls.server(true)).await;
        zero_dns::test_support::install_doh_ca(
            &client.proxy.resolver,
            "query",
            tls.certificate.clone(),
        )
        .unwrap();
        let mut first = query(&client.proxy, "first.software.outer.doh.test");
        let (_, first_answer) = tokio::select! {
            request=tokio::time::timeout_at(end(),tls.requests.recv())=>request.unwrap().unwrap(),
            result=&mut first=>panic!("real outer WG DoH failed before TLS: {result:?}"),
        };
        let actual = pending(&client.proxy).await;
        client.logical_pending(&actual);
        let child = actual.original.as_ref().unwrap();
        let mut second = query(&client.proxy, "second.software.outer.doh.test");
        let (bytes, answer) = tokio::select! {
            request=tokio::time::timeout_at(end(),tls.requests.recv())=>request.unwrap().unwrap(),
            result=&mut second=>panic!("actual shared outer WG DoH pool failed: {result:?}"),
        };
        leases(&client.proxy, 2).await;
        assert!(!first.is_finished() && !second.is_finished());
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        drop(first_answer);
        leases(&client.proxy, 1).await;
        answer
            .send(zero_dns::udp::build_dns_response(
                &bytes,
                &[IpAddress::V4([127, 0, 0, 1])],
            ))
            .unwrap();
        assert_eq!(
            second.await.unwrap().unwrap(),
            vec![IpAddress::V4([127, 0, 0, 1])]
        );
        leases(&client.proxy, 0).await;
        let mut reused = query(&client.proxy, "reused.software.outer.doh.test");
        let (bytes, answer) = tokio::select! {
            request=tokio::time::timeout_at(end(),tls.requests.recv())=>request.unwrap().unwrap(),
            result=&mut reused=>panic!("same outer WG DoH original failed to reuse: {result:?}"),
        };
        let same = pending(&client.proxy).await;
        assert!(same.task.same_registration(&actual.task));
        assert!(same.original.as_ref().unwrap().same_original(child));
        client.logical_pending(&same);
        answer
            .send(zero_dns::udp::build_dns_response(
                &bytes,
                &[IpAddress::V4([127, 0, 0, 1])],
            ))
            .unwrap();
        assert_eq!(
            reused.await.unwrap().unwrap(),
            vec![IpAddress::V4([127, 0, 0, 1])]
        );
        leases(&client.proxy, 0).await;
        assert_eq!(tls.connections.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(!actual.task.is_joined() && child.closed().is_none());
        client.close(&[child.clone()], &[actual.task.clone()]).await;
        peer.close().await;
        tls.close().await;
    })
    .await
    .unwrap();
}
async fn leases(proxy: &Proxy, expected: usize) {
    tokio::time::timeout_at(end(), async {
        loop {
            let actual =
                zero_dns::test_support::doh_pool_witness(&proxy.resolver, "query").unwrap();
            assert_eq!(actual.registered_driver_tasks, 1);
            if actual.active_leases == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
