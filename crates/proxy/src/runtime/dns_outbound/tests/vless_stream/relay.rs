//! Ordinary three-role Service consumes each real registered Node original.
use super::*;
mod fixture;
mod originals;
#[tokio::test]
async fn actual_three_grpc_role_dns_success_cancel_and_new_call_close_each_original_scope_before_reuse(
) {
    tokio::time::timeout(Duration::from_secs(25),async {
        for multi in [false,true] {
            let remote = Remote::for_protocol("vmess",if multi {"grpc-multi"} else {"grpc-single"}).await;
            let mut dns = crate::dns_fixture::Dns::new().await;
            let (proxy,running) = fixture::start(&remote,multi,dns.server()).await;
            let mut previous: Option<Vec<CarrierOrigin>> = None;
            for (index,cancel) in [false,true,false].into_iter().enumerate() {
                let mut query = query(&proxy,&format!("{index}.{multi}.relay.actual.dns.test"));
                let request = tokio::select! {
                    request=dns.request()=>request,
                    result=&mut query=>panic!("real Relay DNS failed before its actual request: {result:?}"),
                };
                assert!(request.tcp);
                let call = pending(&proxy).await;
                let child = call.original.as_ref().unwrap();
                let layers = originals::layers(&proxy,&call);
                if let Some(previous) = &previous {
                    for (old,new) in previous.iter().zip(&layers) {
                        assert!(old.owner().same_owner(new.owner()));
                        assert!(!old.same_original(new));
                        assert!(!old.driver().unwrap().same_registration(new.driver().unwrap()));
                    }
                }
                assert!(!query.is_finished(),"the received real request is deliberately unanswered");
                if cancel {
                    query.abort();assert!(query.await.unwrap_err().is_cancelled());drop(request);
                } else {
                    request.answer(false,false);
                    assert_eq!(query.await.unwrap().unwrap(),vec![IpAddress::V4([127,0,0,1])]);
                }
                let joined = tokio::time::timeout_at(deadline(),call.task.wait_registration_released()).await.unwrap();
                assert!(call.task.matches_joined(&joined) && joined.registration_released());joined.result().unwrap();
                originals::returned(child,&layers).await;
                fixture::no_business(&proxy);
                previous = Some(layers);
            }
            running.shutdown().await.unwrap();
            originals::owner_closed(&previous.unwrap());
            assert_eq!(proxy.protocols.runtime_host().original_budgets().carrier.snapshot().charged,0);
            remote.close().await;dns.close().await;
        }
    }).await.unwrap();
}
#[tokio::test]
async fn actual_three_grpc_role_doh_retains_each_original_when_one_real_http_query_is_cancelled() {
    tokio::time::timeout(Duration::from_secs(25),async {
        for multi in [false,true] {
            let remote = Remote::for_protocol("vmess",if multi {"grpc-multi"} else {"grpc-single"}).await;
            let mut tls = crate::dns_fixture::TlsDns::new(true).await;
            let (proxy,running) = fixture::start(&remote,multi,tls.server(true)).await;
            zero_dns::test_support::install_doh_ca(&proxy.resolver,"query",tls.certificate.clone()).unwrap();
            let mut first = query(&proxy,"first.relay.actual.doh.test");
            let (_, first_answer) = tokio::select! {
                result=tokio::time::timeout_at(deadline(),tls.requests.recv())=>result.unwrap().unwrap(),
                result=&mut first=>panic!("real first Relay DoH wire failed: {result:?}"),
            };
            let call = pending(&proxy).await;
            let child = call.original.as_ref().unwrap();
            let layers = originals::layers(&proxy,&call);
            let mut second = query(&proxy,"second.relay.actual.doh.test");
            let (bytes,answer) = tokio::select! {
                result=tokio::time::timeout_at(deadline(),tls.requests.recv())=>result.unwrap().unwrap(),
                result=&mut second=>panic!("real concurrent Relay DoH wire failed: {result:?}"),
            };
            fixture::leases(&proxy,2).await;
            assert!(!first.is_finished() && !second.is_finished());
            first.abort();assert!(first.await.unwrap_err().is_cancelled());drop(first_answer);
            fixture::leases(&proxy,1).await;
            let same = originals::layers(&proxy,&call);
            for (old,current) in layers.iter().zip(&same) {assert!(old.same_original(current));}
            answer.send(zero_dns::udp::build_dns_response(&bytes,&[IpAddress::V4([127,0,0,1])])).unwrap();
            assert_eq!(second.await.unwrap().unwrap(),vec![IpAddress::V4([127,0,0,1])]);
            fixture::leases(&proxy,0).await;
            let mut reused = query(&proxy,"reused.relay.actual.doh.test");
            let (bytes,answer) = tokio::select! {
                result=tokio::time::timeout_at(deadline(),tls.requests.recv())=>result.unwrap().unwrap(),
                result=&mut reused=>panic!("same original Relay DoH could not be reused: {result:?}"),
            };
            let current = pending(&proxy).await;
            assert!(current.task.same_registration(&call.task));
            assert!(current.original.as_ref().unwrap().same_original(child));
            for (old,current) in layers.iter().zip(originals::layers(&proxy,&current)) {assert!(old.same_original(&current));}
            answer.send(zero_dns::udp::build_dns_response(&bytes,&[IpAddress::V4([127,0,0,1])])).unwrap();
            reused.await.unwrap().unwrap();fixture::leases(&proxy,0).await;
            assert_eq!(tls.connections.load(std::sync::atomic::Ordering::SeqCst),1);
            fixture::no_business(&proxy);
            running.shutdown().await.unwrap();
            let joined = call.task.try_joined().unwrap();
            assert!(call.task.matches_joined(&joined) && joined.registration_released());joined.result().unwrap();
            originals::returned(child,&layers).await;originals::owner_closed(&layers);
            assert!(child.owner().closed().unwrap().matches(child.owner()));
            assert!(child.owner().users_closed().unwrap().matches(child.owner()));
            assert_eq!(proxy.protocols.runtime_host().original_budgets().carrier.snapshot().charged,0);
            remote.close().await;tls.close().await;
        }
    }).await.unwrap();
}
