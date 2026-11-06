//! Existing VLESS H2 and HTTPUpgrade DNS consumes the same taskless original.
use super::*;
use std::{sync::Arc, time::Duration};
use zero_platform_tokio::controlled_carrier::CarrierOrigin;
mod fixture;
use fixture::*;
fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(3)
}
#[tokio::test]
async fn ordinary_vless_h2_and_upgrade_dns_cancellation_retires_the_exact_taskless_native_before_next_query()
 {
    tokio::time::timeout(Duration::from_secs(25),async {
        for kind in ["h2","http_upgrade"] {
            let remote=Remote::new(kind).await;
            let mut dns=crate::dns_fixture::Dns::new().await;
            let (proxy,running)=start(&remote,kind,dns.server()).await;
            let mut originals=Vec::<CarrierOrigin>::new();
            let mut children=Vec::new();
            for (index,cancel) in [false,true,false].into_iter().enumerate() {
                let mut query=query(&proxy,&format!("{index}.{kind}.native.dns.test"));
                let request=tokio::select! {request=dns.request()=>request,result=&mut query=>panic!("ordinary {kind} DNS failed before actual framed request: {result:?}")};
                assert!(request.tcp,"existing UDP backend detour retains framed TCP wire");
                let actual=pending(&proxy).await;let child=actual.original.as_ref().unwrap();
                let donor=donor(&proxy,child);
                for old in &originals {native_closed(old);assert!(!old.same_original(&donor));}
                if cancel {query.abort();assert!(query.await.unwrap_err().is_cancelled());drop(request);}
                else {request.answer(false,false);assert_eq!(query.await.unwrap().unwrap(),vec![IpAddress::V4([127,0,0,1])]);}
                tokio::time::timeout_at(deadline(),actual.task.wait()).await.unwrap().result().unwrap();
                returned(child);native_closed(&donor);originals.push(donor);children.push(child.clone());
            }
            assert!(proxy.engine().active_sessions().is_empty()&&proxy.engine().completed_sessions().is_empty());
            running.shutdown().await.unwrap();
            for child in children {
                assert!(child.owner().closed().unwrap().matches(child.owner()));
                assert!(child.owner().users_closed().unwrap().matches(child.owner()));
            }
            remote.close().await;dns.close().await;
        }
    }).await.unwrap();
}
#[tokio::test]
async fn ordinary_vless_h2_and_upgrade_dot_and_doh_keep_persistent_dns_driver_separate_from_taskless_transport()
 {
    tokio::time::timeout(Duration::from_secs(25),async {
        for kind in ["h2","http_upgrade"] {
            for doh in [false,true] {
                let remote=Remote::new(kind).await;
                let mut tls=crate::dns_fixture::TlsDns::new(doh).await;
                let (proxy,running)=start(&remote,kind,tls.server(doh)).await;
                if doh {zero_dns::test_support::install_doh_ca(&proxy.resolver,"query",tls.certificate.clone()).unwrap();}
                else {zero_dns::test_support::install_dot_ca(&proxy.resolver,"query",tls.certificate.clone()).unwrap();}
                let mut prior:Option<zero_dns::OwnedDnsQueryObservation>=None;
                let mut donors=Vec::new();
                for (index,cancel) in [false,true,false].into_iter().enumerate() {
                    let mut query=query(&proxy,&format!("{index}.{kind}.{doh}.encrypted.dns.test"));
                    let (bytes,answer)=tokio::select! {
                        request=tokio::time::timeout_at(deadline(),tls.requests.recv())=>request.unwrap().unwrap(),
                        result=&mut query=>panic!("ordinary {kind} encrypted DNS failed before TLS request: {result:?}"),
                    };
                    let actual=pending(&proxy).await;let child=actual.original.as_ref().unwrap();
                    let original=donor(&proxy,child);
                    if let Some(old)=&prior {
                        if doh {assert!(child.same_original(old.original.as_ref().unwrap())&&actual.task.same_registration(&old.task));}
                    }
                    if cancel {query.abort();assert!(query.await.unwrap_err().is_cancelled());drop(answer);}
                    else {answer.send(zero_dns::udp::build_dns_response(&bytes,&[IpAddress::V4([127,0,0,1])])).unwrap();assert_eq!(query.await.unwrap().unwrap(),vec![IpAddress::V4([127,0,0,1])]);}
                    if doh {
                        tokio::time::timeout_at(deadline(),async {
                            loop {
                                let pool=zero_dns::test_support::doh_pool_witness(&proxy.resolver,"query").unwrap();
                                assert_eq!(pool.registered_driver_tasks,1);
                                if pool.active_leases==0 {break;}
                                tokio::task::yield_now().await;
                            }
                        }).await.unwrap();
                        assert!(!actual.task.is_joined() && child.closed().is_none() && original.closed().is_none());
                        assert_eq!(tls.connections.load(std::sync::atomic::Ordering::SeqCst),1);
                    } else {
                        tokio::time::timeout_at(deadline(),actual.task.wait()).await.unwrap().result().unwrap();
                        returned(child);native_closed(&original);
                        assert_eq!(tls.connections.load(std::sync::atomic::Ordering::SeqCst),index+1);
                    }
                    donors.push(original);prior=Some(actual);
                }
                let last=prior.unwrap();running.shutdown().await.unwrap();
                assert!(last.task.is_joined());last.task.try_joined().unwrap().result().unwrap();
                returned(last.original.as_ref().unwrap());
                for original in &donors {native_closed(original);}
                assert!(proxy.engine().active_sessions().is_empty()&&proxy.engine().completed_sessions().is_empty());
                remote.close().await;tls.close().await;
            }
        }
    }).await.unwrap();
}

mod wire_pending;

mod domain;

mod quiescence;
