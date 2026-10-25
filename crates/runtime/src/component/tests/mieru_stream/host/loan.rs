//! Actual protocol close and donor original evidence, never a Drop receipt.
use super::*;
use zero_core::{request::Destination, Address};
use zero_platform_tokio::contract::{stream::ConnectRequest, CloseReason};

#[tokio::test]
async fn actual_remote_logical_eof_still_requires_the_live_donor_writers_ordered_close_acknowledgement(
) {
    tokio::time::timeout(Duration::from_secs(12), async {
        let server = Server::closing_logical().await;
        let fixture = Assembly::new(server.address);
        let config = fixture.config();
        fixture
            .catalog
            .drive_supervised(async {
                fixture
                    .catalog
                    .start(config.clone(), deadline())
                    .await
                    .unwrap();
                fixture.catalog.commit(&config, || Ok(())).unwrap();
                let slot = fixture
                    .catalog
                    .current(&config)
                    .unwrap()
                    .direct(Some("a"))
                    .unwrap();
                let mut loan = slot
                    .stream(deadline())
                    .await
                    .unwrap()
                    .connect_carrier(
                        ConnectRequest {
                            destination: Destination::new(Address::Ipv4([203, 0, 113, 1]), 443),
                        },
                        fixture.execution_context(&slot),
                    )
                    .await
                    .unwrap()
                    .io;
                let original = loan.owner.original().clone();
                loan.io.write_all(b"done").await.unwrap();
                let mut bytes = Vec::new();
                loan.io.read_to_end(&mut bytes).await.unwrap();
                assert_eq!(bytes, b"done");
                assert!(
                    !original.driver().expect("actual Mieru driver").is_joined(),
                    "only the actual logical peer closed"
                );
                drop(loan.io);
                let mut closing = Box::pin(loan.owner.close(deadline()));
                std::future::poll_fn(|cx| {
                    assert!(
                        closing.as_mut().poll(cx).is_pending(),
                        "reader EOF/removal cannot bypass the real live writer barrier"
                    );
                    Poll::Ready(())
                })
                .await;
                assert!(closing.await.unwrap().is_complete());
                assert!(loan.owner.closed().unwrap().matches(&loan.owner));
                assert!(!original.driver().expect("actual Mieru driver").is_joined());
                assert!(original.closed().is_none());
                fixture.catalog.shutdown(deadline()).await.unwrap();
                assert!(original.closed().unwrap().matches(&original));
            })
            .await;
        server.close().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn dropping_logical_loan_control_requests_real_session_retirement_without_orphaning_the_donor_book(
) {
    tokio::time::timeout(Duration::from_secs(20), async {
        let server = Server::new().await;
        let fixture = Assembly::new(server.address);
        let mut config = (*fixture.config()).clone();
        config.outbounds.truncate(1);
        let config = Arc::new(config);
        fixture
            .catalog
            .drive_supervised(async {
                fixture
                    .catalog
                    .start(config.clone(), deadline())
                    .await
                    .unwrap();
                fixture.catalog.commit(&config, || Ok(())).unwrap();
                let slot = fixture
                    .catalog
                    .current(&config)
                    .unwrap()
                    .direct(Some("a"))
                    .unwrap();
                let role = slot.stream(deadline()).await.unwrap();
                let mut original = None;
                // The real donor reader has 256 session slots. Exceed that lifetime
                // count on one healthy physical connection; each abandoned control
                // must request actual book removal, never claim native closure.
                for _ in 0..258 {
                    let loan = role
                        .connect_carrier(
                            ConnectRequest {
                                destination: Destination::new(Address::Ipv4([203, 0, 113, 1]), 443),
                            },
                            fixture.execution_context(&slot),
                        )
                        .await
                        .unwrap()
                        .io;
                    let current = loan.owner.original().clone();
                    if let Some(prior) = &original {
                        assert!(current.same_original(prior));
                    } else {
                        original = Some(current);
                    }
                    drop(loan.io);
                    drop(loan.owner);
                }
                assert_eq!(server.accepts.load(Ordering::SeqCst), 1);
                let original = original.unwrap();
                assert!(!original.driver().expect("actual Mieru driver").is_joined());
                assert!(original.closed().is_none());
                fixture.catalog.shutdown(deadline()).await.unwrap();
                assert!(original.driver().expect("actual Mieru driver").is_joined());
                assert!(original.closed().unwrap().matches(&original));
            })
            .await;
        server.close().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn mieru_tcp_and_packet_loans_return_one_session_without_claiming_or_stopping_the_donor_native(
) {
    tokio::time::timeout(Duration::from_secs(20), async {
        for udp in [false, true] {
            let server = if udp {
                Server::packet().await
            } else {
                Server::new().await
            };
            let fixture = Assembly::new(server.address);
            let mut config = (*fixture.config()).clone();
            config.outbounds.truncate(1);
            if udp {
                if let zero_config::OutboundProtocolConfig::Mieru { transport, .. } =
                    &mut config.outbounds[0].protocol
                {
                    *transport = zero_config::MieruTransport::Udp;
                }
            }
            let config = Arc::new(config);
            fixture
                .catalog
                .drive_supervised(async {
                    fixture
                        .catalog
                        .start(config.clone(), deadline())
                        .await
                        .unwrap();
                    fixture.catalog.commit(&config, || Ok(())).unwrap();
                    let slot = fixture
                        .catalog
                        .current(&config)
                        .unwrap()
                        .direct(Some("a"))
                        .unwrap();
                    let connected = slot
                        .stream(deadline())
                        .await
                        .unwrap()
                        .connect_carrier(
                            ConnectRequest {
                                destination: Destination::new(Address::Ipv4([203, 0, 113, 1]), 443),
                            },
                            fixture.execution_context(&slot),
                        )
                        .await
                        .unwrap();
                    let mut loan = connected.io;
                    let original = loan.owner.original().clone();
                    assert_eq!(original.instance(), slot.dependency_instances()[0]);
                    assert!(!original.driver().expect("actual Mieru driver").is_joined());
                    let mut other = fixture.open(&slot).await;
                    loan.io.write_all(b"borrow").await.unwrap();
                    loan.io.read_exact(&mut [0; 6]).await.unwrap();
                    assert_eq!(server.accepts.load(Ordering::SeqCst), 1);
                    let report = loan
                        .owner
                        .close(tokio::time::Instant::now() + Duration::from_millis(25))
                        .await
                        .unwrap();
                    assert!(
                        !report.is_complete(),
                        "the actual transferred view is still retained"
                    );
                    assert!(loan.owner.closed().is_none());
                    assert_eq!(
                        loan.io.write_all(b"revoked").await.unwrap_err().kind(),
                        std::io::ErrorKind::BrokenPipe
                    );
                    drop(loan.io);
                    let report = loan.owner.close(deadline()).await.unwrap();
                    assert!(report.is_complete(), "{report:?}");
                    assert!(report.closed_io.is_empty() && report.exited_tasks.is_empty());
                    let returned = loan.owner.closed().unwrap();
                    assert!(returned.matches(&loan.owner));
                    assert!(returned.original().same_original(&original));
                    assert!(
                        original.owner().closed().is_none(),
                        "a logical return is not original native close"
                    );
                    assert!(
                        !original.driver().expect("actual Mieru driver").is_joined(),
                        "the shared real driver is still running"
                    );
                    other.write_all(b"other").await.unwrap();
                    let mut bytes = [0; 5];
                    other.read_exact(&mut bytes).await.unwrap();
                    assert_eq!(&bytes, b"other");
                    drop(other);
                    fixture.catalog.shutdown(deadline()).await.unwrap();
                    assert!(original.driver().expect("actual Mieru driver").is_joined());
                    assert!(original.owner().closed().unwrap().matches(original.owner()));
                    assert!(original
                        .owner()
                        .users_closed()
                        .unwrap()
                        .matches(original.owner()));
                })
                .await;
            server.close().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn retained_logical_loan_controller_blocks_actual_mieru_successor_even_after_view_and_driver_exit(
) {
    tokio::time::timeout(Duration::from_secs(12), async {
        let server = Server::new().await;
        let fixture = Assembly::new(server.address);
        let config = fixture.config();
        fixture
            .catalog
            .drive_supervised(async {
                fixture
                    .catalog
                    .start(config.clone(), deadline())
                    .await
                    .unwrap();
                fixture.catalog.commit(&config, || Ok(())).unwrap();
                let slot = fixture
                    .catalog
                    .current(&config)
                    .unwrap()
                    .direct(Some("a"))
                    .unwrap();
                let connected = slot
                    .stream(deadline())
                    .await
                    .unwrap()
                    .connect_carrier(
                        ConnectRequest {
                            destination: Destination::new(Address::Ipv4([203, 0, 113, 1]), 443),
                        },
                        fixture.execution_context(&slot),
                    )
                    .await
                    .unwrap();
                let mut loan = connected.io;
                let original = loan.owner.original().clone();
                let mut changed = (*config).clone();
                if let zero_config::OutboundProtocolConfig::Mieru { password, .. } =
                    &mut changed.outbounds[1].protocol
                {
                    *password = "unused credential changed".into();
                }
                let changed = Arc::new(changed);
                let mut replacing = Box::pin(fixture.catalog.start(changed.clone(), deadline()));
                assert!(
                    tokio::time::timeout(Duration::from_millis(25), replacing.as_mut())
                        .await
                        .is_err()
                );
                drop(loan.io);
                assert!(
                    tokio::time::timeout(Duration::from_millis(25), replacing.as_mut())
                        .await
                        .is_err()
                );
                assert!(original.driver().expect("actual Mieru driver").is_joined());
                assert!(loan.owner.closed().is_none());
                assert_eq!(fixture.builds.load(Ordering::SeqCst), 1);
                loan.owner.cancel(CloseReason::Shutdown);
                assert!(loan.owner.close(deadline()).await.unwrap().is_complete());
                assert!(loan.owner.closed().unwrap().matches(&loan.owner));
                replacing.await.unwrap();
                fixture.catalog.commit(&changed, || Ok(())).unwrap();
                assert_eq!(fixture.builds.load(Ordering::SeqCst), 2);
                assert!(original.owner().closed().unwrap().matches(original.owner()));
                assert!(original
                    .owner()
                    .users_closed()
                    .unwrap()
                    .matches(original.owner()));
                fixture.catalog.shutdown(deadline()).await.unwrap();
            })
            .await;
        server.close().await;
    })
    .await
    .unwrap();
}
