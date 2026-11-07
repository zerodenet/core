//! A sealed direct dependency cannot substitute a different acquired binding.
use super::*;
use crate::contract::{
    component::ResourceDeclaration,
    lifecycle::{CloseReport, DynChannelControl},
    network::UpstreamOpener,
    BoxFuture, CloseReason,
};
use crate::controlled_carrier::loan::StreamLoan;
use zero_core::component::{OutboundId, RoleId, RoleInstanceId};
struct ActualControl(CarrierAdoption);
impl DynChannelControl for ActualControl {
    fn cancel(&self, _: CloseReason) {
        self.0.request_retirement();
    }
    fn wait_closed(&mut self, end: Instant) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        Box::pin(self.0.retire(end))
    }
}
#[tokio::test]
async fn direct_dependency_prefix_loan_requires_exact_actual_owner_binding_slot_and_private_fence()
{
    timeout(Duration::from_secs(8), async {
        for case in ["valid", "binding", "generation", "slot", "fence"] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let endpoint = destination(address);
            let egress = Arc::new(crate::EgressInterfaceControl::default());
            let root_binding = UpstreamBinding { prefix:vec![], dial:Default::default(), network_generation:NetworkGeneration(egress.generation()) };
            let mut root_scope = TaskScope::new(tokio::runtime::Handle::current(), NonZeroUsize::new(1).unwrap());
            let mut lower_scope = TaskScope::new(tokio::runtime::Handle::current(), NonZeroUsize::new(1).unwrap());
            let mut upper_scope = TaskScope::new(tokio::runtime::Handle::current(), NonZeroUsize::new(1).unwrap());
            let host = Arc::new(CancellationToken::new());
            let one = NonZeroUsize::new(1).unwrap();
            let mut root = CarrierOwner::new(instance(), one, &host);
            root.registration().bind_declared(&ResourceDeclaration::ControlledNodeCarrier { endpoint:endpoint.clone(),dial:Default::default(),max_sockets:one }, &root_scope.registrar(), &host).unwrap();
            root.start(deadline()).await.unwrap();
            let mut grant = root.grants().reserve().unwrap();
            let view = grant.claim_transferred_view().unwrap();
            let root_original = grant.adoption();
            let io = NativeCarrierOpener::new(instance(), root_binding.clone(), address, egress, None).unwrap()
                .prepare_controlled_stream(&root_binding,endpoint.clone(),context(),grant).unwrap()
                .open(deadline()).await.unwrap().into_parts().0;
            let (mut peer, _) = listener.accept().await.unwrap();
            let loan = io.into_stream_loan(view).unwrap();
            let lower_id = ComponentInstanceId { component:ComponentId(772), ..instance() };
            let mut lower = CarrierOwner::new(lower_id, one, &host);
            let declaration = ResourceDeclaration::ControlledCarrierPool { endpoint:endpoint.clone(),dial:Default::default(),datagram:false,max_sockets:one };
            lower.registration().bind_declared(&declaration,&lower_scope.registrar(),&host).unwrap();
            lower.start(deadline()).await.unwrap();
            let binding = UpstreamBinding { prefix:vec![RoleInstanceId { component:instance().component,role:RoleId::Outbound(OutboundId(1)),generation:instance().generation }], ..root_binding };
            let mut grant = lower.grants().reserve().unwrap();
            let view = grant.claim_driver_view().unwrap();
            let lower_original = grant.adoption();
            let stop = grant.cancellation();
            let io = grant.adopt_stream_loan(&binding,endpoint.clone(),&context(),loan).unwrap();
            let (logical, mut driver_side) = tokio::io::duplex(64);

            let (_, driver) = lower_original.spawn_driver(&lower_scope.registrar(),TaskClass::Auxiliary,io.into_driver(),move |mut io|async move {
                let _view = view;
                tokio::select! { biased;
                    _=stop.cancelled()=>Ok(()),
                    result=tokio::io::copy_bidirectional(&mut io,&mut driver_side)=>result.map(|_|()).map_err(|source|ChannelError::new(Stage::Receive,Reason::Io,Impact::Component).with_source(source)),
                }
            }).unwrap();
            let original = lower_original.origin(&driver).unwrap();
            assert_eq!(original.selected_binding().unwrap(),(endpoint.clone(),binding.clone()));
            let mut loan = Some(StreamLoan::new(Box::pin(logical),Box::new(ActualControl(lower_original.clone())),original));
            let upper_id = ComponentInstanceId { component:ComponentId(773), ..instance() };
            let mut upper = CarrierOwner::new(upper_id,NonZeroUsize::new(2).unwrap(),&host);
            let upper_declaration = ResourceDeclaration::ControlledCarrierPool { endpoint:endpoint.clone(),dial:Default::default(),datagram:false,max_sockets:NonZeroUsize::new(2).unwrap() };
            upper.registration().bind_declared(&upper_declaration,&upper_scope.registrar(),&host).unwrap();
            upper.start(deadline()).await.unwrap();
            let mut requested = binding.clone();
            if case=="binding" { requested.prefix[0].generation = ResourceGeneration(20); }
            let mut other = CarrierOwner::new(ComponentInstanceId {generation:ResourceGeneration(20),..lower_id},one,&host);
            other.registration().bind_declared(&declaration,&lower_scope.registrar(),&host).unwrap();
            other.start(deadline()).await.unwrap();
            let donor = if case=="generation" { other.registration() } else { lower.registration() };
            let mut grant = upper.grants().reserve().unwrap();
            let fence = if case=="fence" {Arc::new(CancellationToken::new())} else {host.clone()};
            let bound = upper.registration().bound().unwrap();
            let issuer = bound.dependency_stream_loan_issuer(&fence,&grant,&donor,&endpoint,&requested);
            if case=="slot" { grant=upper.grants().reserve().unwrap(); }
            let receiver = grant.adoption();
            let view = grant.claim_transferred_view().unwrap();
            let mut accepted = None;
            if case=="fence" {
                assert!(issuer.is_err());
                drop(grant);
            } else {
                let borrowed = loan.take().unwrap();
                let observed = borrowed.receiver_observation();
                let result = grant.adopt_dependency_stream_loan(&requested,endpoint.clone(),&context(),borrowed,issuer.unwrap());
                if case=="valid" {
                    accepted=Some(result.unwrap());
                    let io = accepted.as_mut().unwrap();
                    io.write_all(b"same dependency").await.unwrap();
                    let mut bytes=[0;15]; peer.read_exact(&mut bytes).await.unwrap(); assert_eq!(&bytes,b"same dependency");
                    peer.write_all(b"reply").await.unwrap();
                    let mut reply=[0;5]; io.read_exact(&mut reply).await.unwrap(); assert_eq!(&reply,b"reply");
                } else { assert!(result.is_err()); }
                let retained=observed.retained().expect("even rejected acquired loans stay in the original receiver");
                assert!(retained.receiver_adoption().unwrap().same_slot(&receiver));
                assert!(retained.retains(&lower_original));
                assert!(receiver.closed().is_none() && lower_original.closed().is_none());
            }
            drop(accepted); drop(view);
            lower_original.request_retirement();
            let cleanup = async {
                if let Some(loan) = loan {
                    drop(loan.io); let mut owner=loan.owner;
                    assert!(owner.close(deadline()).await.unwrap().is_complete());
                }
                assert!(receiver.retire(deadline()).await.unwrap().is_complete());
                assert!(lower_original.retire(deadline()).await.unwrap().is_complete());
            };
            let (_, exit) = tokio::join!(cleanup,lower_scope.next_exit());
            let exit=exit.unwrap(); assert_eq!(driver.task_id(),Some(exit.id)); assert!(exit.result.is_ok());
            let joined=driver.try_joined().unwrap(); assert!(driver.matches_joined(&joined) && joined.registration_released());
            assert!(root_original.closed().unwrap().matches(&root_original));
            assert!(root_original.users_closed().unwrap().matches(&root_original));
            assert!(lower_original.closed().unwrap().matches(&lower_original));
            assert!(lower_original.users_closed().unwrap().matches(&lower_original));
            assert_eq!(peer.read(&mut [0;1]).await.unwrap(),0);
            for owner in [&mut upper,&mut lower,&mut root,&mut other] { assert!(owner.shutdown(deadline()).await.unwrap().is_complete()); }
            for scope in [&mut upper_scope,&mut lower_scope,&mut root_scope] { assert!(scope.shutdown(deadline()).await.is_complete()); }
        }
    }).await.expect("same actual native and all driver/Scope cleanup");
}
