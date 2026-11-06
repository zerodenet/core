//! Cancel an actual ClientHello wait before the per-connection loan is returned.
use super::*;
#[tokio::test]
async fn ordinary_vless_h2_and_upgrade_first_tls_await_retains_actual_taskless_native_until_query_cleanup(
) {
    tokio::time::timeout(Duration::from_secs(15),async {
        for kind in ["h2","http_upgrade"] {
            let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let material=tempfile::tempdir().unwrap();
            let certified=rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
            let ca=material.path().join("ca.pem");std::fs::write(&ca,certified.cert.pem()).unwrap();
            let mut dns=crate::dns_fixture::Dns::new().await;
            let (proxy,running)=start_at(listener.local_addr().unwrap(),&ca,kind,dns.server()).await;
            let mut query=query(&proxy,&format!("firstawait.{kind}.native.dns.test"));
            let (mut wire,_)=tokio::select! {
                accepted=listener.accept()=>accepted.unwrap(),
                result=&mut query=>panic!("ordinary {kind} failed before actual TLS ClientHello: {result:?}"),
            };
            let mut header=[0;5];wire.read_exact(&mut header).await.unwrap();
            assert_eq!(&header[..2],&[22,3]);
            let mut hello=vec![0;u16::from_be_bytes([header[3],header[4]]) as usize];
            wire.read_exact(&mut hello).await.unwrap();assert_eq!(hello[0],1,"actual complete ClientHello");
            assert!(!query.is_finished(),"native peer has not returned its TLS ServerHello");
            let caller=proxy.resolver.native_query_observations().into_iter().find(|actual|!actual.task.is_joined()).unwrap();
            assert!(caller.original.is_none(),"TLS framing has not returned a DNS child loan");
            let components=proxy.tcp_runtime_services().components.clone().unwrap();
            let node=components.configured("node").unwrap();
            let owner=(0..node.recipe.spec().resources.len()).filter_map(|ordinal|node.original_carrier_owner(ordinal))
                .find(|owner|owner.observe().is_some_and(|state|state.slots().iter().any(|original|original.native().is_some_and(|native|!native.is_closed())))).unwrap();
            let observed=owner.observe().unwrap();assert_eq!(observed.retained(),1);
            let held=&observed.slots()[0];
            assert!(held.driver().is_none(),"TLS/H2 protocol has no detached driver");
            let auxiliary=held.auxiliary().expect("actual retained preparation Aux").clone();
            assert!(auxiliary.scope_identity().same_scope(&owner.bound().unwrap().scope_identity()));
            assert!(!auxiliary.is_joined(),"actual TLS ServerHello wait remains pending in the original Aux");
            assert!(!auxiliary.same_registration(&caller.task));
            let original=held.original().clone();assert!(original.closed().is_none());
            query.abort();assert!(query.await.unwrap_err().is_cancelled());
            tokio::time::timeout_at(deadline(),caller.task.wait()).await.unwrap().result().unwrap();
            let joined=tokio::time::timeout_at(deadline(),auxiliary.wait_registration_released()).await.unwrap();
            assert!(auxiliary.matches_joined(&joined)&&joined.registration_released());joined.result().unwrap();
            let closed=original.closed().expect("exact undelivered native closes without a later dial");
            assert!(original.matches_closed(&closed)&&closed.had_native_acquisition());
            tokio::time::timeout_at(deadline(),async {
                while owner.observe().unwrap().retained()!=0 {tokio::task::yield_now().await;}
            }).await.unwrap();
            let mut remaining=Vec::new();wire.read_to_end(&mut remaining).await.unwrap();
            dns.no_new_request();
            assert!(proxy.engine().active_sessions().is_empty()&&proxy.engine().completed_sessions().is_empty());
            running.shutdown().await.unwrap();
            assert!(owner.closed().unwrap().matches(&owner));
            assert!(owner.users_closed().unwrap().matches(&owner));
            dns.close().await;
        }
    }).await.unwrap();
}
