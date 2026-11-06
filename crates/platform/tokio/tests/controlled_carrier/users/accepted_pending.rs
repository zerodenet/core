//! Pending cleanup uses the original accepted graph and its actual task exit.
use super::*;
use crate::contract::component::ResourceDeclaration;

#[tokio::test]
async fn actual_accepted_caller_retires_pending_service_native_after_its_real_view_and_join_return()
{
    timeout(Duration::from_secs(5),async {
        let mut fixture=Fixture::cold(1).await;
        let mut native_scope=TaskScope::empty(tokio::runtime::Handle::current());
        let owner=fixture.owner.registration();
        owner.bind_declared(&ResourceDeclaration::ControlledServiceStream {
            endpoint:fixture.destination.clone(),dial:fixture.transport.binding().dial.clone(),max_sockets:NonZeroUsize::new(1).unwrap()
        },&native_scope.registrar(),&Arc::new(CancellationToken::new())).unwrap();
        fixture.owner.start(deadline()).await.unwrap();
        let mut source_scope=TaskScope::new(tokio::runtime::Handle::current(),NonZeroUsize::new(1).unwrap());
        let cancel=CancellationToken::new();let body_cancel=cancel.clone();
        let (send,receive)=tokio::sync::oneshot::channel::<crate::controlled_carrier::loan::LoanIo>();
        let pending=Arc::new(tokio::sync::Notify::new());let observed=pending.clone();
        let (id,witness)=source_scope.registrar().spawn_tracked(TaskClass::Auxiliary,async move {
            let mut io=receive.await.unwrap();
            let mut byte=[0;1];let read=io.read_exact(&mut byte);tokio::pin!(read);
            std::future::poll_fn(|cx| { use std::future::Future;assert!(read.as_mut().poll(cx).is_pending());observed.notify_one();std::task::Poll::Ready(()) }).await;
            tokio::select! { biased; _=body_cancel.cancelled()=>{}, result=read=>panic!("actual peer withheld bytes: {result:?}"), }
            Ok(())
        }).unwrap();
        let mut caller_graph=crate::AcceptedPeerScope::for_carrier(&source_scope.registrar().downgrade(),id,witness.clone(),cancel,NonZeroUsize::new(1).unwrap(),1,fixture.listener.local_addr().unwrap()).unwrap();
        let caller=caller_graph.tasks().caller().unwrap();
        let mut grant=fixture.grants.reserve().unwrap();let view=grant.claim_transferred_view().unwrap();let adoption=grant.adoption();
        assert_eq!(caller.observe_carrier_with_owner(&owner,adoption.clone()).unwrap_err().reason,Reason::StalePlan,"cleanup custody must not relax actual Business caller binding");
        caller.observe_pending_carrier(adoption.clone()).unwrap();
        let carrier=fixture.transport.prepare_controlled_stream(fixture.destination.clone(),context(),grant).unwrap().open(deadline()).await.unwrap();
        let (mut peer,_)=fixture.listener.accept().await.unwrap();let (io,_)=carrier.into_parts();
        let loan=io.into_stream_loan(view).unwrap();let mut control=loan.owner;
        send.send(loan.io).unwrap_or_else(|_|panic!("real source still alive"));pending.notified().await;
        assert!(!witness.is_joined());assert!(adoption.closed().is_none());
        assert_eq!(fixture.grants.reserve().err().unwrap().reason,Reason::Capacity);
        caller_graph.revoke(crate::contract::CloseReason::Cancelled);
        let exit=source_scope.next_exit().await.unwrap();assert_eq!(exit.id,id);assert!(exit.result.is_ok());
        assert!(caller_graph.observe_joined(id).is_some());
        caller_graph.retire_logical_resources(deadline()).await.unwrap();
        assert!(witness.is_joined());assert!(adoption.closed().unwrap().matches(&adoption));
        assert!(adoption.users_closed().unwrap().matches(&adoption));
        assert!(adoption.users_closed().unwrap().caller_exit().is_none(),"actual accepted source join is independent of Service transferred custody");
        assert_eq!(peer.read(&mut [0;1]).await.unwrap(),0);
        assert!(control.try_close().unwrap().is_complete());assert!(control.closed().unwrap().matches(&control));
        drop(fixture.grants.reserve().unwrap());
        let report=caller_graph.shutdown(deadline()).await.unwrap();assert!(report.is_complete(),"{report:?}");assert_eq!(report.closed_io.len(),1);
        assert!(caller_graph.shutdown(deadline()).await.unwrap().closed_io.is_empty());
        assert!(fixture.owner.shutdown(deadline()).await.unwrap().is_complete());assert!(owner.closed().unwrap().matches(&owner));
        assert!(source_scope.shutdown(deadline()).await.is_complete());assert!(native_scope.shutdown(deadline()).await.is_complete());
    }).await.unwrap();
}
