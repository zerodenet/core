//! Native/users receipts precede real Host ACK; one physical N remains charged.
use super::*;
use crate::contract::{component::ResourceDeclaration, network::UpstreamOpener};
use crate::controlled_carrier::CarrierBusinessDemand;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Wake, Waker};
use zero_core::component::SessionId;
struct Demand(CancellationToken);
impl CarrierBusinessDemand for Demand {
    fn check(&self) -> Result<(), ChannelError> {
        if self.0.is_cancelled() {
            Err(error(
                instance(),
                Stage::Prepare,
                Reason::Revoked,
                DeliveryState::NotSubmitted,
            ))
        } else {
            Ok(())
        }
    }
    fn session(&self) -> SessionId {
        SessionId(943)
    }
    fn cancellation(&self) -> CancellationToken {
        self.0.clone()
    }
    fn deadline(&self) -> Option<Instant> {
        None
    }
}
#[derive(Default)]
struct WakeCount(AtomicUsize);
impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[tokio::test]
async fn original_n1_native_and_users_close_remain_visible_while_deferred_actual_ack_retains_credit_and_wakes_after_timeout(
) {
    timeout(Duration::from_secs(8),async {
        for fail in [false,true] {
            let mut fixture=Fixture::cold(1).await;
            let mut scope=TaskScope::suspended(tokio::runtime::Handle::current(),None);scope.enable_registration().unwrap();
            let mut caller=TaskScope::suspended_hosted(tokio::runtime::Handle::current(),NonZeroUsize::new(1));caller.enable_registration().unwrap();
            let totals=super::CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());let host=Arc::new(CancellationToken::new());let registration=fixture.owner.registration();
            let bound=registration.bind_declared_with_totals(&ResourceDeclaration::ControlledNodeCarrier{endpoint:fixture.destination.clone(),dial:fixture.transport.binding().dial.clone(),max_sockets:NonZeroUsize::new(1).unwrap()},&scope.registrar(),&host,&totals).unwrap();fixture.owner.start(deadline()).await.unwrap();
            let mut grant=fixture.grants.reserve().unwrap();let view=grant.claim_user_view().unwrap();let adoption=grant.adoption();
            let(binding,endpoint,egress)=(fixture.transport.binding().clone(),fixture.destination.clone(),fixture.egress.clone());
            let(begin,started)=tokio::sync::oneshot::channel::<crate::contract::scope::TaskJoinWitness>();
            let(_,witness)=caller.registrar().spawn_tracked(TaskClass::Auxiliary,async move {
                let caller=started.await.unwrap();let demand=Arc::new(Demand(CancellationToken::new()));
                let ticket=bound.node_business_issuer(&host,&grant,&endpoint,&binding).unwrap().authorize_with_caller(demand.clone(),caller).unwrap();
                let native=NativeCarrierOpener::business(ticket,egress,None).unwrap();
                let carrier=native.prepare_controlled_stream(&binding,endpoint,CarrierContext{session:Some(SessionId(943)),deadline:None,cancel:demand.0.clone()},grant).unwrap().open_with_deadline(None).await.unwrap();
                let(mut io,_)=carrier.into_parts();io.write_all(b"caller").await.unwrap();let mut bytes=[0;5];io.read_exact(&mut bytes).await.unwrap();assert_eq!(&bytes,b"reply");drop(io);drop(view);
                if fail {Err(error(instance(),Stage::Receive,Reason::Io,DeliveryState::Unknown).with_source(std::io::Error::new(std::io::ErrorKind::BrokenPipe,"original caller failure")))}else{Ok(())}
            }).unwrap();begin.send(witness.clone()).unwrap();
            let(mut peer,_)=fixture.listener.accept().await.unwrap();let mut bytes=[0;6];peer.read_exact(&mut bytes).await.unwrap();assert_eq!(&bytes,b"caller");peer.write_all(b"reply").await.unwrap();
            let receipt=caller.next_receipt().await.unwrap();let joined=witness.try_joined().unwrap();assert!(witness.matches_joined(receipt.joined()));assert!(!joined.registration_released());assert_eq!(receipt.result().is_err(),fail);
            let native=adoption.retire_native(deadline()).await.unwrap();assert!(native.report().is_complete());assert!(native.closed().unwrap().matches(&adoption));assert!(native.closed().unwrap().had_native_acquisition());
            let users=adoption.users_closed().expect("real view returned and actual caller joined before ACK");assert!(users.matches(&adoption)&&users.matches_caller(&witness));assert!(!users.caller_exit().unwrap().registration_released());
            let original=registration.observe().unwrap().slots()[0].original().clone();let closed=original.closed().expect("same original native completion precedes credit return");assert!(original.matches_closed(&closed));
            let report=adoption.try_retire().unwrap();assert!(!report.is_complete());assert_eq!(fixture.owner.retained(),1);assert_eq!(totals.snapshot().charged,1);assert_eq!(fixture.grants.reserve().err().unwrap().reason,Reason::Capacity);assert!(registration.closed().is_none());
            assert_eq!(peer.read(&mut [0;1]).await.unwrap(),0);
            let callbacks=caller.defer_callbacks();if fail {witness.acknowledge_hosted_failure(receipt.joined()).unwrap();}caller.acknowledge(&receipt).unwrap();assert!(!joined.registration_released());assert_eq!(fixture.grants.reserve().err().unwrap().reason,Reason::Capacity);
            let wakes=Arc::new(WakeCount::default());let waker=Waker::from(wakes.clone());let mut cx=Context::from_waker(&waker);
            registration.poll_cancelled_retirements(&mut cx,Duration::from_millis(5));tokio::time::sleep(Duration::from_millis(10)).await;registration.poll_cancelled_retirements(&mut cx,Duration::from_millis(5));
            assert_eq!(fixture.owner.retained(),1);assert_eq!(totals.snapshot().charged,1);assert!(adoption.closed().unwrap().matches(&adoption));assert!(adoption.users_closed().unwrap().matches_caller(&witness));let before=wakes.0.load(Ordering::SeqCst);drop(callbacks);assert!(wakes.0.load(Ordering::SeqCst)>before,"actual ACK must wake the original continuation even after its immutable cleanup deadline");
            let released=witness.wait_registration_released().await;assert!(released.registration_released()&&witness.matches_joined(&released));registration.poll_cancelled_retirements(&mut cx,Duration::from_millis(5));assert_eq!(fixture.owner.retained(),0);assert_eq!(totals.snapshot().charged,0);
            let mut grant=fixture.grants.reserve().unwrap();let view=grant.claim_transferred_view().unwrap();let next=grant.adoption();let carrier=fixture.transport.prepare_controlled_stream(fixture.destination.clone(),context(),grant).unwrap().open(deadline()).await.unwrap();let(mut peer,_)=fixture.listener.accept().await.unwrap();let(mut io,_)=carrier.into_parts();io.write_all(b"service").await.unwrap();let mut bytes=[0;7];peer.read_exact(&mut bytes).await.unwrap();assert_eq!(&bytes,b"service");drop(io);drop(view);assert!(next.retire(deadline()).await.unwrap().is_complete());
            assert!(fixture.owner.shutdown(deadline()).await.unwrap().is_complete());assert!(registration.closed().unwrap().matches(&registration));let generation=registration.users_closed().unwrap();assert!(generation.matches(&registration));
            match(receipt.result(),generation.first_caller_failure()) { (Ok(()),None)=>{},(Err(actual),Some(retained))=>assert!(Arc::ptr_eq(&actual,&retained.result().unwrap_err())),_=>panic!("same original caller failure was lost") }
            caller.begin_shutdown();assert!(caller.is_drained());assert!(scope.shutdown(deadline()).await.is_complete());
        }
    }).await.unwrap();
}
