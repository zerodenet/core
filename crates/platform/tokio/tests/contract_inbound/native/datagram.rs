use super::*;
use zero_platform_tokio::contract::component::AcceptedDatagramAuthority;

#[tokio::test]
async fn accepted_datagram_original_survives_cancel_and_retires_only_after_real_scope_join() {
    timeout(Duration::from_secs(5), async {
        let one = NonZeroUsize::new(1).unwrap();
        let mut scope = TaskScope::new(tokio::runtime::Handle::current(), one);
        let foreign = TaskScope::new(tokio::runtime::Handle::current(), one);
        let declaration = ResourceDeclaration::TcpListener {
            destination: Destination::new(Address::Ipv4([127, 0, 0, 1]), 0),
            accepted: Some(AcceptedTcpLimits {
                max_sockets: one,
                max_tasks_per_socket: one,
                max_resources_per_socket: 1,
            }),
            max_bind_attempts: one,
        };
        let cancel = CancellationToken::new();
        let mut original = ControlledTcpListenerOwner::new_with_accepted_graph(
            &declaration,
            one,
            cancel.clone(),
            one,
            one,
            &scope.registrar(),
        )
        .unwrap();
        let mut view = original
            .bind_listener("127.0.0.1:0".parse().unwrap())
            .unwrap();
        let client = tokio::net::TcpStream::connect(view.local_addr().unwrap())
            .await
            .unwrap();
        let (accepted, _) = view.accept_owned().await.unwrap();
        let pending = accepted
            .into_pending(|socket| SocketHandshake {
                socket,
                started: Arc::default(),
                pending: Arc::new(Notify::new()),
            })
            .unwrap();
        let (claim, receiver) = pending.accepted_tcp().unwrap();
        let udp = ResourceDeclaration::AcceptedDatagram {
            max_sockets: one,
            max_pending_sends: one,
        };
        let authority =
            AcceptedDatagramAuthority::new(&udp, &scope.registrar(), cancel.clone()).unwrap();
        let foreign =
            AcceptedDatagramAuthority::new(&udp, &foreign.registrar(), cancel.clone()).unwrap();
        let (send, received) = tokio::sync::oneshot::channel();
        let release = Arc::new(Notify::new());
        let (id, witness) = pending
            .spawn(TaskClass::Auxiliary, {
                let release = release.clone();
                move |pending, tasks| async move {
                    let caller = tasks.caller().unwrap();
                    assert!(
                        foreign.open(&caller).is_err(),
                        "a different actual Scope grants no native access"
                    );
                    let io = authority.open(&caller).unwrap();
                    assert!(
                        authority.open(&caller).is_err(),
                        "the same declared association slot is not reusable"
                    );
                    send.send(io).ok().unwrap();
                    release.notified().await;
                    drop(pending);
                    Ok(())
                }
            })
            .unwrap();
        let GrantedDatagramListener { mut rx, tx } = received.await.unwrap();
        let endpoint = rx.local_addr().unwrap();
        assert_eq!(endpoint.ip(), client.peer_addr().unwrap().ip());
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut buffer = [0; 16];
        let mut waiting = Box::pin(rx.recv_from_addr(&mut buffer));
        poll_fn(|cx| match waiting.as_mut().poll(cx) {
            Poll::Pending => Poll::Ready(()),
            Poll::Ready(_) => panic!("actual UDP receive must be pending before cancellation"),
        })
        .await;
        drop(waiting);
        peer.send_to(b"request", endpoint).await.unwrap();
        let (read, sender) = rx.recv_from_addr(&mut buffer).await.unwrap();
        assert_eq!(&buffer[..read], b"request");
        tx.send_to_addr(b"response", sender).await.unwrap();
        let (read, source) = peer.recv_from(&mut buffer).await.unwrap();
        assert_eq!(&buffer[..read], b"response");
        assert_eq!(source, endpoint);

        // Cancellation/deadline never sign original native or Scope completion.
        assert!(receiver
            .close(&claim, Instant::now())
            .await
            .unwrap()
            .is_none());
        assert!(!witness.is_joined());
        assert_eq!(original.accepted_evidence().unwrap().retained, 1);
        release.notify_one();
        assert_eq!(scope.next_exit().await.unwrap().id, id);
        receiver.observe_joined(id).unwrap();
        assert!(witness.is_joined());
        let receipt = receiver
            .close(&claim, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .unwrap();
        assert!(receipt.matches(&claim));
        assert!(tx.send_to_addr(b"retired", sender).await.is_err());
        assert!(rx.recv_from_addr(&mut buffer).await.is_err());
        let report = original.take_accepted_report();
        assert!(
            report
                .closed_io
                .iter()
                .any(|io| io.contains("accepted auxiliary original 0")),
            "consume the actual original UDP provider receipt"
        );
        assert!(report
            .closed_io
            .iter()
            .any(|io| io.contains("accepted TCP originals")));
        drop(view);
        assert!(original
            .wait_closed(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .is_complete());
        assert!(original.accepted_evidence().unwrap().provider_confirmed());
    })
    .await
    .unwrap();
}
