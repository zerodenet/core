//! Ordinary UDP and borrowed packets use the same actual gRPC original/driver.
use super::*;
use zero_core::{component::AssociationId, packet::Datagram, request::DatagramTargets};
use zero_platform_tokio::contract::{
    datagram::{recv, send, DatagramRequest},
    CloseReason,
};

#[tokio::test]
async fn ordinary_and_borrowed_datagrams_keep_empty_packets_and_actual_logical_custody_on_one_grpc_pool(
) {
    timeout(Duration::from_secs(8), async {
        let runtime = crate::transport::VlessTransportRuntime::default();
        let grpc = OwnedGrpcProfile {
            service_names: vec!["actual".into()],
            multi_mode: true,
            ..Default::default()
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let profile = grpc.clone();
        let (finish, finished) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (io, _) = listener.accept().await.unwrap();
            let incoming = zero_transport::grpc::accept_grpc_connection(io, &profile).unwrap();
            let mut workers = tokio::task::JoinSet::new();
            for _ in 0..2 {
                let mut stream = incoming.accept().await.unwrap();
                workers.spawn(async move {
                    let expected = golden(2);
                    let mut header = vec![0; expected.len()];
                    stream.read_exact(&mut header).await.unwrap();
                    assert_eq!(header, expected);
                    // Real VLESS inbound UDP acceptance precedes business data.
                    stream.write_all(&[0, 0]).await.unwrap();
                    stream.flush().await.unwrap();
                    for expected in [b"".as_slice(), b"real".as_slice(), b"odd".as_slice()] {
                        let mut len = [0; 2];
                        stream.read_exact(&mut len).await.unwrap();
                        assert_eq!(u16::from_be_bytes(len) as usize, expected.len());
                        let mut payload = vec![0; expected.len()];
                        stream.read_exact(&mut payload).await.unwrap();
                        assert_eq!(payload, expected);
                        stream.write_all(&len).await.unwrap();
                        stream.write_all(&payload).await.unwrap();
                        stream.flush().await.unwrap();
                    }
                    let mut extra = [0; 1];
                    match stream.read(&mut extra).await {
                        Ok(n) => {
                            assert_eq!(n, 0, "no extra application data after the three packets")
                        }
                        Err(_) => {} // The actual logical cancellation sends H2 reset.
                    }
                });
            }
            while let Some(done) = workers.join_next().await {
                done.unwrap();
            }
            finished.await.unwrap();
            incoming.close();
        });
        let mut assembly = Assembly::cold(prepare(&runtime, peer.port(), &grpc), peer);
        assembly.start().await;
        let role = assembly.role.roles.datagram.clone().unwrap();
        let association = AssociationId(91);
        let request = DatagramRequest {
            association,
            targets: DatagramTargets::Fixed(destination()),
        };
        let mut ordinary = role
            .open(request.clone(), assembly.context())
            .await
            .unwrap();
        assert_eq!(ordinary.limits.targets, request.targets);
        let foreign = Datagram {
            association: AssociationId(92),
            address: destination(),
            payload: b"must-not-send".to_vec().into(),
        };
        let failure = send(
            ordinary.tx.as_mut(),
            ordinary.control.as_mut(),
            &foreign,
            &ordinary.limits,
        )
        .await
        .unwrap_err();
        assert_eq!(failure.error.reason, Reason::StalePlan);
        assert_eq!(
            failure.delivery,
            zero_platform_tokio::contract::DeliveryState::NotSubmitted
        );
        for payload in [b"".as_slice(), b"real".as_slice(), b"odd".as_slice()] {
            let frame = Datagram {
                association,
                address: destination(),
                payload: payload.to_vec().into(),
            };
            let receipt = send(
                ordinary.tx.as_mut(),
                ordinary.control.as_mut(),
                &frame,
                &ordinary.limits,
            )
            .await
            .unwrap();
            assert_eq!(receipt.accepted_bytes, payload.len());
            let response = recv(ordinary.rx.as_mut()).await.unwrap().unwrap();
            assert_eq!(response.association, association);
            assert_eq!(response.address, destination());
            assert_eq!(&*response.payload, payload);
        }
        drop(ordinary.tx);
        drop(ordinary.rx);
        ordinary.control.cancel(CloseReason::Completed);
        assert!(ordinary
            .control
            .wait_closed(deadline())
            .await
            .unwrap()
            .is_complete());
        let mut context = assembly.context();
        context.deadline = None;
        let mut borrowed = role.open_carrier(request, context).await.unwrap();
        let original = borrowed.owner.original().clone();
        assert_eq!(original.instance(), assembly.actual);
        let driver = original.driver().unwrap().clone();
        for payload in [b"".as_slice(), b"real".as_slice(), b"odd".as_slice()] {
            borrowed.io.send(payload).await.unwrap();
            let mut bytes = [0; 16];
            let received = borrowed.io.receive(&mut bytes).await.unwrap();
            assert_eq!(&bytes[..received], payload);
        }
        {
            let mut bytes = [0; 16];
            let mut receive = Box::pin(borrowed.io.receive(&mut bytes));
            let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
            assert!(std::future::Future::poll(receive.as_mut(), &mut cx).is_pending());
            borrowed.owner.cancel(CloseReason::Cancelled);
        }
        drop(borrowed.io);
        assert!(borrowed
            .owner
            .close(deadline())
            .await
            .unwrap()
            .is_complete());
        assert!(borrowed.owner.closed().unwrap().matches(&borrowed.owner));
        assert!(
            original.closed().is_none(),
            "logical packet close is not native pool close"
        );
        assert!(!driver.is_joined());
        let (protocol, tasks) = assembly.close().await;
        assert!(protocol.is_complete(), "{protocol:?}");
        assert!(tasks.is_complete(), "{tasks:?}");
        let closed = original.closed().unwrap();
        assert!(closed.matches(&original));
        assert!(driver.is_joined());
        finish.send(()).unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
