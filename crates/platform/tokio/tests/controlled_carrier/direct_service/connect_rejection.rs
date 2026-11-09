//! Real refused TCP original: received result versus lost result and panic.
use super::*;
#[tokio::test]
async fn only_received_native_connect_rejection_avoids_duplicate_component_fault_after_original_ack(
) {
    timeout(Duration::from_secs(5), async {
        for mode in ["received", "undelivered", "panic"] {
            let mut fixture=Fixture::cold(1).await;
            let bad=tokio::net::TcpSocket::new_v4().unwrap();
            bad.bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let peer=bad.local_addr().unwrap();
            let endpoint=Destination::new(Address::Ipv4([127,0,0,1]),peer.port());
            let mut scope=TaskScope::suspended_hosted(tokio::runtime::Handle::current(),NonZeroUsize::new(1));
            scope.enable_registration().unwrap();
            let tasks=scope.registrar();let fence=Arc::new(CancellationToken::new());
            let binding=fixture.transport.binding().clone();
            let bound=fixture.owner.registration().bind_declared(
                &crate::contract::component::ResourceDeclaration::ControlledDirectStream {dial:binding.dial.clone(),max_sockets:NonZeroUsize::new(1).unwrap()},
                &tasks,&fence,
            ).unwrap();
            fixture.owner.start(deadline()).await.unwrap();
            let mut grant=fixture.grants.reserve().unwrap();let original=grant.adoption();
            let(view,preparation)=grant.claim_preparing_transferred_view().unwrap();
            let(begin,started)=tokio::sync::oneshot::channel::<crate::contract::scope::TaskJoinWitness>();
            let(send,mut receive)=tokio::sync::oneshot::channel::<Arc<ChannelError>>();
            let selection=fixture.egress.clone();
            let(_,actual)=original.spawn_auxiliary(&tasks,async move {
                let actual=started.await.unwrap();
                let call=context();
                let ticket=bound.prepared_direct_service_issuer(&fence,&grant,actual,&endpoint,&binding).unwrap()
                    .authorize(call.cancel.clone(),call.deadline).unwrap();
                let native=NativeCarrierOpener::service(ticket,selection,None).unwrap();
                let failed=native.prepare_controlled_stream(&binding,endpoint,call,grant).unwrap().open(deadline()).await;
                let source=Arc::new(failed.err().expect("real bound nonlistening peer refuses connect"));
                assert!(crate::controlled_carrier::original_connect_error(&source).is_some());
                let _=send.send(source.clone());
                if mode=="panic" {panic!("real initializer panic after an independently returned native rejection");}
                Err(ChannelError::retained(source))
            }).unwrap();
            begin.send(actual.clone()).unwrap();
            let delivered=if mode=="undelivered" {receive.close();None}else{Some(receive.await.unwrap())};
            let receipt=scope.next_receipt().await.unwrap();
            let joined=original.accept_auxiliary().await.unwrap().unwrap();
            assert!(actual.matches_joined(&joined)&&actual.matches_joined(receipt.joined()));
            assert!(joined.result().is_err()&&!joined.registration_released());
            if let Some(source)=&delivered {assert!(preparation.receive_connect_rejection(source).is_err(),"actual Host ACK is still held");}
            assert_eq!(fixture.grants.reserve().err().unwrap().reason,Reason::Capacity);
            scope.acknowledge(&receipt).unwrap();
            if mode=="received" {
                let source=delivered.as_ref().unwrap();
                let foreign=Arc::new(ChannelError::new(Stage::Dial,Reason::Io,Impact::Component));
                assert!(preparation.receive_connect_rejection(&foreign).is_err());
                preparation.receive_connect_rejection(source).unwrap();
                preparation.receive_connect_rejection(source).unwrap();
            }else if mode=="panic" {
                assert!(preparation.receive_connect_rejection(delivered.as_ref().unwrap()).is_err(),"actual task panic cannot be marked as its earlier operation result");
            }
            drop(view);
            let report=original.retire(deadline()).await.unwrap();
            assert!(report.is_complete()&&report.exited_tasks.contains(&joined.id()));
            if mode=="received" {assert!(report.failed_tasks.is_empty());}
            else {
                let failed=report.failed_tasks.iter().find(|failed|failed.id==joined.id()).unwrap();
                assert!(Arc::ptr_eq(failed.error.retained_source().unwrap(),&joined.result().unwrap_err()));
            }
            assert!(original.closed().unwrap().matches(&original));
            assert!(original.users_closed().unwrap().matches(&original));
            assert!(joined.registration_released());
            let next=fixture.grants.reserve().unwrap();let next_original=next.adoption();drop(next);
            assert!(next_original.retire(deadline()).await.unwrap().is_complete());
            assert!(fixture.owner.shutdown(deadline()).await.unwrap().is_complete());
            scope.begin_shutdown();assert!(scope.is_drained());drop(bad);
        }
    }).await.unwrap();
}
