use super::*;

#[tokio::test]
async fn resolved_candidates_share_one_original_owner_and_retry_only_after_confirmed_native_close()
{
    timeout(Duration::from_secs(3), async {
        let available = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = available.local_addr().unwrap();
        let occupied =
            match tokio::net::TcpListener::bind((std::net::Ipv6Addr::LOCALHOST, address.port()))
                .await
            {
                Ok(listener) => listener,
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::AddrNotAvailable | std::io::ErrorKind::Unsupported
                    ) =>
                {
                    eprintln!("SKIP multi-family candidate fixture: {error}");
                    return;
                }
                Err(error) => panic!("reserve separate IPv6 endpoint: {error}"),
            };
        let first = occupied.local_addr().unwrap();
        let one = NonZeroUsize::new(1).unwrap();
        let two = NonZeroUsize::new(2).unwrap();
        let mut scope = TaskScope::new(tokio::runtime::Handle::current(), one);
        let declaration = ResourceDeclaration::TcpListener {
            destination: Destination::new(Address::Domain("localhost".into()), address.port()),
            accepted: Some(AcceptedTcpLimits {
                max_sockets: one,
                max_tasks_per_socket: one,
                max_resources_per_socket: 0,
            }),
            max_bind_attempts: two,
        };
        let mut original = ControlledTcpListenerOwner::new_resolving_with_accepted_graph(
            &declaration,
            two,
            CancellationToken::new(),
            one,
            one,
            &scope.registrar(),
        )
        .unwrap();
        assert!(
            original.bind_listener(first).is_err(),
            "unresolved owner grants no native attempt"
        );
        assert_eq!(original.remaining_bind_attempts(), 2);
        original
            .install_resolved_addresses(Arc::from([first, address]))
            .unwrap();
        assert!(
            original
                .install_resolved_addresses(Arc::from([address]))
                .is_err(),
            "frozen generation cannot be retargeted"
        );
        let error = match original.bind_listener(first) {
            Err(error) => error,
            Ok(_) => panic!("occupied original"),
        };
        assert!(error.retry_permitted());
        assert_eq!(
            original.confirmed_close_count(),
            1,
            "consume the failed original's actual close outcome"
        );
        assert!(!original.native_unconfirmed());
        drop(available);
        let mut view = original.bind_listener(address).unwrap();
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        client.write_all(b"x").await.unwrap();
        let (accepted, _) = view.accept_owned().await.unwrap();
        let claim = accepted.claim();
        let receiver = original.accepted_task_receiver().unwrap();
        let (id, witness) = accepted
            .spawn_handler_bound(TaskClass::Auxiliary, |mut socket, tasks| async move {
                tasks.caller().unwrap().check_open().unwrap();
                let mut bytes = [0];
                socket.read_exact(&mut bytes).await.unwrap();
                socket.write_all(&bytes).await.unwrap();
                Ok(())
            })
            .unwrap();
        let mut response = [0];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(response, *b"x");
        assert_eq!(scope.next_exit().await.unwrap().id, id);
        receiver.observe_joined(id).unwrap();
        assert!(witness.is_joined());
        assert!(receiver
            .close(&claim, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .unwrap()
            .matches(&claim));
        drop(view);
        let report = original
            .wait_closed(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(report.is_complete());
        assert!(original.confirmed_close_count() >= 2);
        assert!(original.accepted_evidence().unwrap().provider_confirmed());
        assert!(
            report.closed_io.len() >= 2,
            "original native receipts, not a port-availability inference"
        );
        drop(occupied);
    })
    .await
    .unwrap();
}
