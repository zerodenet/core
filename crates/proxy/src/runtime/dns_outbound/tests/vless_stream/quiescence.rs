//! Same existing DoH driver/loan is retired before the donor can be replaced.
use super::*;
#[tokio::test]
async fn actual_pending_dns_is_busy_but_idle_doh_releases_same_donor_and_rolls_back_query_admission()
 {
    tokio::time::timeout(Duration::from_secs(20),async {
        let remote=Remote::new("h2").await;
        let mut tls=crate::dns_fixture::TlsDns::new(true).await;
        let (proxy,running)=start(&remote,"h2",tls.server(true)).await;
        zero_dns::test_support::install_doh_ca(&proxy.resolver,"query",tls.certificate.clone()).unwrap();
        let mut first=query(&proxy,"warm.quiescence.actual.test");
        let (bytes,answer)=tokio::select! {
            request=tokio::time::timeout_at(deadline(),tls.requests.recv())=>request.unwrap().unwrap(),
            result=&mut first=>panic!("actual DoH failed before real HTTP Pending: {result:?}"),
        };
        let actual=pending(&proxy).await;let child=actual.original.as_ref().unwrap();let original=donor(child);
        let replaced=[original.instance()];
        assert!(proxy.resolver.native_donor_instances().unwrap().contains(&replaced[0]));
        let unrelated=proxy.resolver.try_pause_donor_replacement(&[]).unwrap();
        unrelated.retire(deadline()).await.unwrap();drop(unrelated);
        assert!(!actual.task.is_joined() && original.closed().is_none());
        let rejected=match proxy.resolver.try_pause_donor_replacement(&replaced) {
            Err(error)=>error,Ok(_)=>panic!("actual pending request was evicted to change the donor budget"),
        };
        assert_eq!(rejected.kind(),std::io::ErrorKind::WouldBlock);
        answer.send(zero_dns::udp::build_dns_response(&bytes,&[IpAddress::V4([127,0,0,1])])).unwrap();
        assert_eq!(first.await.unwrap().unwrap(),vec![IpAddress::V4([127,0,0,1])],"Busy preserves the original real pending wire");
        let paused=tokio::time::timeout_at(deadline(),async {
            loop {
                match proxy.resolver.try_pause_donor_replacement(&replaced) {
                    Ok(paused)=>break paused,
                    Err(error) if error.kind()==std::io::ErrorKind::WouldBlock=>tokio::task::yield_now().await,
                    Err(error)=>panic!("actual idle-query admission failed: {error}"),
                }
            }
        }).await.unwrap();
        paused.retire(deadline()).await.unwrap();
        let joined=actual.task.try_joined().unwrap();
        assert!(actual.task.matches_joined(&joined)&&joined.registration_released());joined.result().unwrap();
        returned(child);native_closed(&original);
        let blocked=proxy.resolver.resolve_real_type("paused.quiescence.actual.test",1).await.unwrap_err();
        assert_eq!(blocked.kind(),std::io::ErrorKind::WouldBlock);
        assert!(tls.requests.try_recv().is_err());
        assert_eq!(tls.connections.load(std::sync::atomic::Ordering::SeqCst),1);
        drop(paused); // Restore only admission, never manufacture a reopened native.
        let mut next=query(&proxy,"resumed.quiescence.actual.test");
        let (bytes,answer)=tokio::select! {
            request=tokio::time::timeout_at(deadline(),tls.requests.recv())=>request.unwrap().unwrap(),
            result=&mut next=>panic!("same actual backend did not recover its admission: {result:?}"),
        };
        let fresh=pending(&proxy).await;let successor=fresh.original.as_ref().unwrap();let fresh_donor=donor(successor);
        assert!(!fresh.task.same_registration(&actual.task));
        assert!(fresh.task.scope_identity().same_scope(&actual.task.scope_identity()),"same backend/provider Scope reconnects without a second owner");
        assert!(!original.same_original(&fresh_donor));
        answer.send(zero_dns::udp::build_dns_response(&bytes,&[IpAddress::V4([127,0,0,1])])).unwrap();next.await.unwrap().unwrap();
        assert_eq!(tls.connections.load(std::sync::atomic::Ordering::SeqCst),2);
        running.shutdown().await.unwrap();native_closed(&fresh_donor);returned(successor);
        assert!(proxy.engine().active_sessions().is_empty()&&proxy.engine().completed_sessions().is_empty());
        remote.close().await;tls.close().await;
    }).await.unwrap();
}

#[tokio::test]
async fn ordinary_total_budget_replacement_rejects_pending_dns_then_retires_idle_same_doh_before_new_native()
 {
    tokio::time::timeout(Duration::from_secs(25),async {
        let remote=Remote::new("h2").await;
        let mut tls=crate::dns_fixture::TlsDns::new(true).await;
        let (proxy,running)=start(&remote,"h2",tls.server(true)).await;
        zero_dns::test_support::install_doh_ca(&proxy.resolver,"query",tls.certificate.clone()).unwrap();
        let mut first=query(&proxy,"pending.actual.budget.test");
        let (bytes,answer)=tokio::select! {
            request=tokio::time::timeout_at(deadline(),tls.requests.recv())=>request.unwrap().unwrap(),
            result=&mut first=>panic!("original DoH failed before actual Pending: {result:?}"),
        };
        let actual=pending(&proxy).await;let child=actual.original.as_ref().unwrap();let old=donor(child);
        let old_plan=proxy.tcp_runtime_services().components.clone().unwrap();
        let old_slot=old_plan.component(old.instance().component).unwrap();
        let owner=(0..old_slot.recipe.spec().resources.len()).filter_map(|ordinal|old_slot.original_carrier_owner(ordinal))
            .find(|owner|owner.instance()==old.instance()).unwrap();
        let root=proxy.protocols.runtime_host().original_budgets().carrier.clone();
        let previous_limit=root.snapshot().limit;
        let handle=crate::ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()),proxy.clone());
        let mut candidate=(*proxy.engine().config()).clone();candidate.runtime.carrier_max_total_originals=Some(32);
        assert!(handle.apply_runtime_config_and_wait(candidate.clone(),Duration::from_secs(5)).await.is_err(),"active original DNS work must reject cap replacement before StopOld");
        assert_eq!(root.snapshot().limit,previous_limit);
        assert!(old.closed().is_none()&&!actual.task.is_joined());
        answer.send(zero_dns::udp::build_dns_response(&bytes,&[IpAddress::V4([127,0,0,1])])).unwrap();
        assert_eq!(first.await.unwrap().unwrap(),vec![IpAddress::V4([127,0,0,1])],"Busy must preserve actual old request/wire");
        tokio::time::timeout_at(deadline(),async {
            loop {
                if zero_dns::test_support::doh_pool_witness(&proxy.resolver,"query").unwrap().active_leases==0 {break;}
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        assert!(old.closed().is_none()&&!actual.task.is_joined(),"the idle warm driver is still real custody before apply");
        handle.apply_runtime_config_and_wait(candidate,Duration::from_secs(5)).await.unwrap();
        assert!(actual.task.try_joined().unwrap().registration_released());actual.task.try_joined().unwrap().result().unwrap();
        returned(child);native_closed(&old);
        assert!(owner.closed().unwrap().matches(&owner)&&owner.users_closed().unwrap().matches(&owner));
        assert_eq!(root.snapshot().limit.get(),32);assert_eq!(root.snapshot().charged,0,"old actual originals are gone before a new configured cap can admit IO");
        zero_dns::test_support::install_doh_ca(&proxy.resolver,"query",tls.certificate.clone()).unwrap();
        let mut next=query(&proxy,"after.actual.budget.test");
        let (bytes,answer)=tokio::select! {
            request=tokio::time::timeout_at(deadline(),tls.requests.recv())=>request.unwrap().unwrap(),
            result=&mut next=>panic!("actual successor DNS failed after old native/Scope closure: {result:?}"),
        };
        let successor=pending(&proxy).await;let next_child=successor.original.as_ref().unwrap();let fresh=donor(next_child);
        assert!(!old.same_original(&fresh));
        assert!(!actual.task.scope_identity().same_scope(&successor.task.scope_identity()));
        answer.send(zero_dns::udp::build_dns_response(&bytes,&[IpAddress::V4([127,0,0,1])])).unwrap();next.await.unwrap().unwrap();
        running.shutdown().await.unwrap();native_closed(&fresh);returned(next_child);
        assert!(successor.task.try_joined().unwrap().registration_released());
        assert_eq!(root.snapshot().charged,0);
        assert!(proxy.engine().active_sessions().is_empty()&&proxy.engine().completed_sessions().is_empty());
        remote.close().await;tls.close().await;
    }).await.unwrap();
}
