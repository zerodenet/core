//! Existing DoT and persistent DoH use the original WG virtual TCP socket.
use super::*;
use std::sync::atomic::Ordering;
#[tokio::test]
async fn ordinary_linked_wireguard_dot_and_doh_keep_native_and_query_retirement_separate() {
    exercise(true).await;
}
#[tokio::test]
async fn ordinary_standalone_wireguard_dot_and_doh_share_the_actual_bound_wire_and_persistent_dns_pool(
) {
    exercise(false).await;
}
async fn exercise(linked: bool) {
    tokio::time::timeout(Duration::from_secs(25),async {
        for doh in [false,true] {
            let mut tls=crate::dns_fixture::TlsDns::new(doh).await;
            let peer=peer::Peer::new(tls.address).await;
            let (proxy,running)=start_server_mode(&peer,tls.server(doh),linked).await;
            if doh {zero_dns::test_support::install_doh_ca(&proxy.resolver,"query",tls.certificate.clone()).unwrap();}
            else {zero_dns::test_support::install_dot_ca(&proxy.resolver,"query",tls.certificate.clone()).unwrap();}
            let root=proxy.protocols.test_packet_source_origins(proxy.engine().config().as_ref()).remove(0);
            let mut physical:Option<zero_dns::OwnedDnsQueryObservation>=None;
            let mut children=Vec::new();
            for (index,cancel) in [false,true,false].into_iter().enumerate() {
                let mut query=query(&proxy,&format!("{index}.encrypted.linked.wg.test"));
                let (bytes,answer)=tokio::select! {
                    request=tokio::time::timeout_at(end(),tls.requests.recv())=>request.unwrap().unwrap(),
                    result=&mut query=>panic!("WG encrypted DNS failed before its real TLS request: {result:?}"),
                };
                let actual=pending(&proxy).await;let child=actual.original.as_ref().unwrap();
                native_pending(child,&root);
                if let Some(old)=&physical {
                    if doh {
                        assert!(child.same_original(old.original.as_ref().unwrap()));
                        assert!(actual.task.same_registration(&old.task));
                    }
                }
                if cancel {query.abort();assert!(query.await.unwrap_err().is_cancelled());drop(answer);}
                else {
                    answer.send(zero_dns::udp::build_dns_response(&bytes,&[IpAddress::V4([127,0,0,1])])).unwrap();
                    assert_eq!(query.await.unwrap().unwrap(),vec![IpAddress::V4([127,0,0,1])]);
                }
                if doh {
                    tokio::time::timeout_at(end(),async {
                        loop {
                            let actual=zero_dns::test_support::doh_pool_witness(&proxy.resolver,"query").unwrap();
                            assert_eq!(actual.registered_driver_tasks,1);
                            if actual.active_leases==0 {break;}
                            tokio::task::yield_now().await;
                        }
                    }).await.unwrap();
                    assert!(!actual.task.is_joined()&&child.closed().is_none(),"logical request return cannot close the persistent DoH original");
                    assert_eq!(tls.connections.load(Ordering::SeqCst),1);
                } else {
                    tokio::time::timeout_at(end(),actual.task.wait()).await.unwrap().result().unwrap();
                    returned(child);
                    assert_eq!(tls.connections.load(Ordering::SeqCst),index+1);
                }
                native_pending(child,&root);children.push(child.clone());physical=Some(actual);
            }
            let driver=physical.unwrap().task;
            if linked {close(&proxy,running,&root,&children).await;}
            else {standalone::close(&proxy,running,&root,&children).await;}
            assert!(driver.is_joined());driver.try_joined().unwrap().result().unwrap();
            peer.close().await;tls.close().await;
        }
    }).await.unwrap();
}
