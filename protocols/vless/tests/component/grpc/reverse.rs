//! The actual shared gRPC owner executes Rvs as Service traffic, authenticates
//! only after real controller transfer, with separate logical/native receipts.
use super::*;
use std::{future::Future, task::Poll};

#[tokio::test]
async fn reverse_service_installs_real_loan_before_authentication_and_shares_the_original() {
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
        let (header_sent, header_seen) = tokio::sync::oneshot::channel();
        let (respond, response_allowed) = tokio::sync::oneshot::channel();
        let (finish, finished) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (io, _) = listener.accept().await.unwrap();
            let incoming = zero_transport::grpc::accept_grpc_connection(io, &profile).unwrap();
            let mut reverse = incoming.accept().await.unwrap();
            let mut actual = [0; 19];
            reverse.read_exact(&mut actual).await.unwrap();
            let mut expected = vec![0];
            expected.extend(crate::parse_uuid(ID).unwrap());
            expected.extend([0, crate::reverse::COMMAND]);
            assert_eq!(actual.as_slice(), expected);
            reverse.write_all(&[0]).await.unwrap();
            header_sent.send(()).unwrap();
            response_allowed.await.unwrap();
            reverse.write_all(&[3, 9, 8, 7]).await.unwrap();
            reverse.write_all(b"reverse").await.unwrap();
            let mut ordinary = incoming.accept().await.unwrap();
            let expected = golden(1);
            let mut header = vec![0; expected.len()];
            ordinary.read_exact(&mut header).await.unwrap();
            assert_eq!(header, expected);
            ordinary.write_all(&[0, 0]).await.unwrap();
            ordinary.write_all(b"forward").await.unwrap();
            finished.await.unwrap();
            incoming.close();
        });
        let prepared = prepare(&runtime, peer.port(), &grpc);
        let mut assembly = Assembly::cold(prepared, peer);
        assembly.start().await;
        let role = assembly.role.roles.stream.clone().unwrap();
        let mut context = assembly.context();
        context.identity = RequestIdentity::Service(
            Arc::new(ServiceRequestFacts {
                origin: ServiceOrigin::Runtime,
                purpose: ServicePurpose::DialedIngress,
            })
            .into(),
        );
        let destination = Destination::new(Address::Ipv4([127, 0, 0, 1]), peer.port());
        let connected = role
            .connect_carrier(ConnectRequest { destination }, context)
            .await
            .unwrap();
        assert_eq!(connected.peer_confirmation, PeerConfirmation::Deferred);
        let zero_platform_tokio::controlled_carrier::loan::StreamLoan { mut io, owner } =
            connected.io;
        // This same persistent record retains the original controller before
        // polling any Rvs write/response. No fabricated completion is installed.
        let owner = Arc::new(tokio::sync::Mutex::new(owner));
        assert_eq!(owner.lock().await.original().instance(), assembly.actual);
        assert!(assembly.observations.facts.lock().unwrap().is_empty());
        let mut bytes = [0; 7];
        let mut authentication = Box::pin(io.read_exact(&mut bytes));
        tokio::select! {biased;
            result = &mut authentication => panic!("response was not sent: {}", result.is_ok()),
            result = header_seen => result.unwrap(),
        }
        std::future::poll_fn(|cx| {
            assert!(authentication.as_mut().poll(cx).is_pending());
            assert_eq!(
                assembly.observations.facts.lock().unwrap().last(),
                Some(&(None, ConnectFact::RequestSubmitted))
            );
            // The real same controller is retained while the actual native
            // response read is pending; a timer/child marker is not used.
            let retained = owner.try_lock().unwrap();
            assert!(retained.closed().is_none());
            assert_eq!(retained.original().instance(), assembly.actual);
            assert!(!retained.original().driver().unwrap().is_joined());
            Poll::Ready(())
        })
        .await;
        // Interrupt only this response waiter after the first real response
        // byte. The retained IO keeps that exact preparation future and header.
        drop(authentication);
        respond.send(()).unwrap();
        io.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"reverse");
        assert_eq!(
            assembly.observations.facts.lock().unwrap().last(),
            Some(&(None, ConnectFact::PeerConfirmed))
        );
        let mut forward = role
            .connect(
                ConnectRequest {
                    destination: fixtures::destination(),
                },
                assembly.context(),
            )
            .await
            .unwrap();
        assert_eq!(forward.peer_confirmation, PeerConfirmation::Deferred);
        forward.io.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"forward");
        drop(forward);
        drop(io);
        let mut owner = owner.lock().await;
        let returned = owner.close(deadline()).await.unwrap();
        assert!(returned.is_complete());
        assert!(owner.closed().unwrap().matches(&owner));
        assert!(owner.original().closed().is_none());
        assert!(!owner.original().driver().unwrap().is_joined());
        let (pool, scope) = assembly.close().await;
        assert!(pool.is_complete() && scope.is_complete());
        assert_eq!(
            pool.closed_io.len(),
            1,
            "one real native original for both services"
        );
        assert_eq!(scope.exited_tasks.len(), 1);
        assert!(scope.failed_tasks.is_empty());
        finish.send(()).unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}

mod source;
