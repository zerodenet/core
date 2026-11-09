use super::*;
#[tokio::test]
async fn actual_original_two_task_graph_polls_udp_without_a_third_task_and_keeps_fault_source_before_ack(
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        for mode in ["data", "failure"] {
            let (_proxy, memory)=memory();
            let source=sources(&memory);
            let mut scope=TaskScope::suspended_hosted(tokio::runtime::Handle::current(),NonZeroUsize::new(2));
            scope.enable_registration().unwrap();
            let (primary_done, primary_wait)=oneshot::channel();
            let (primary_id, _)=scope.registrar().spawn_tracked(TaskClass::Auxiliary,async move {
                let _=primary_wait.await; Ok(())
            }).unwrap();
            let socket=UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let address=socket.local_addr().unwrap();
            let peer=UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let refused=tokio::net::TcpSocket::new_v4().unwrap();
            refused.bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let bad_address=refused.local_addr().unwrap();
            let actual=Arc::new(std::sync::OnceLock::<zero_platform_tokio::contract::scope::TaskJoinWitness>::new());
            let original=actual.clone();
            let (begin, begun)=oneshot::channel();
            let (ready, receiving)=oneshot::channel();
            let stop=CancellationToken::new();let driver_stop=stop.clone();
            let (packets, mut delivered)=mpsc::channel(1);
            let worker_memory=memory.clone();let worker_source=source.clone();
            let (driver_id,witness)=scope.registrar().spawn_tracked(TaskClass::Critical,async move {
                begun.await.unwrap();
                let (owner,mut lease)=ResponseSetOwner::new(JoinSet::new());
                let reply=lease.packet_sender(&worker_memory).unwrap();
                let cleanup=CancellationToken::new();let body_stop=cleanup.clone();
                let body_sources=worker_source.clone();
                let data_memory=worker_memory.clone();
                lease.install_packet_receiver(&worker_memory,original.get().unwrap().clone(),cleanup,
                    worker_source.into_error(), async move {
                        let mut bytes=[0;32];
                        loop {
                            let read=tokio::select! { biased;
                                _=body_stop.cancelled()=>return Ok(()),
                                packet=socket.recv_from(&mut bytes)=>packet.unwrap().0,
                            };
                            if mode=="failure" {
                                let error=tokio::net::TcpStream::connect(bad_address).await.unwrap_err();
                                let source=Arc::new(error);let _=body_sources.0.set(source.clone());
                                return Err(ChannelError::new(Stage::Receive,Reason::Io,Impact::Session).with_source(source));
                            }
                            let frame=data_memory.datagram(AssociationId(17),&Address::Ipv4([203,0,113,1]),53,&bytes[..read]).unwrap();
                            reply.send((frame.address.address,53,frame.payload,Some(7))).unwrap();
                        }
                    }).unwrap();
                poll_fn(|cx| {
                    let mut next=std::pin::pin!(lease.join_next_with_id());
                    assert!(next.as_mut().poll(cx).is_pending());Poll::Ready(())
                }).await;
                assert!(lease.is_empty(),"pure RX work registers no native task");
                ready.send(owner).unwrap_or_else(|_| panic!("actual receiver installed"));
                loop {
                    tokio::select! { biased;
                        _=driver_stop.cancelled()=>break,
                        Some(packet)=lease.join_next_with_id()=>{packets.send(packet.unwrap().unwrap()).await.unwrap();}
                    }
                }
                Ok(())
            }).unwrap();
            actual.set(witness.clone()).unwrap();begin.send(()).unwrap();
            let owner=receiving.await.unwrap();
            assert!(!witness.is_joined());
            assert!(scope.registrar().spawn_tracked(TaskClass::Auxiliary,async {panic!("third task must be denied")}).is_err());
            peer.send_to(b"real UDP",address).await.unwrap();
            if mode=="data" {
                let packet=delivered.recv().await.unwrap();
                assert_eq!(packet.2.as_ref(),b"real UDP");assert!(memory.bytes.used_bytes()>0);drop(packet);
                assert!(owner.receiver.get().unwrap().failure().is_none());
            } else {
                while owner.receiver.get().unwrap().failure().is_none() {tokio::task::yield_now().await;}
                let retained=owner.receiver.get().unwrap().failure().unwrap();
                let actual=std::error::Error::source(retained.as_ref()).unwrap().downcast_ref::<Sources>().unwrap();
                assert!(Arc::ptr_eq(actual.0.get().unwrap(),source.0.get().unwrap()));
            }
            assert_eq!(owner.evidence.statistics.lock().unwrap().joined,0);
            assert!(!witness.is_joined(),"packet result is not the original driver exit");
            stop.cancel();
            let receipt=scope.next_receipt().await.unwrap();assert_eq!(receipt.id(),driver_id);
            assert!(receipt.result().is_ok());assert!(!receipt.joined().registration_released());
            let closed=owner.shutdown(end()).await.unwrap();
            assert_eq!(closed.joined_tasks(),0,"no forged native join");
            if mode=="failure" {
                let retained=closed.packet_failure().unwrap().clone();
                let same=owner.shutdown(end()).await.unwrap();
                assert!(same.packet_failure().unwrap().ptr_eq(&retained));
                let receipt=retained.cleanup_failure_receipt().unwrap();
                assert!(!receipt.is_acknowledged());assert!(receipt.acknowledge_retained(&retained));
            } else { assert!(closed.packet_failure().is_none()); }
            assert!(!receipt.joined().registration_released());
            scope.acknowledge(&receipt).unwrap();
            assert!(receipt.joined().registration_released());
            primary_done.send(()).unwrap();
            let primary=scope.next_receipt().await.unwrap();assert_eq!(primary.id(),primary_id);
            scope.acknowledge(&primary).unwrap();scope.begin_shutdown();
        assert!(scope.is_drained());
            assert_eq!(memory.bytes.used_bytes(),0);
        }
    }).await.unwrap();
}
