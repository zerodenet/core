use super::*;
use zero_config::OutboundProtocolConfig;
use zero_core::{component::AssociationId, packet::Datagram, request::DatagramTargets};
use zero_core::{request::Destination, Address};
use zero_platform_tokio::contract::{
    datagram::{recv, send, DatagramRequest},
    CloseReason,
};

#[tokio::test]
async fn tcp_and_udp_roles_share_one_authenticated_connection_and_logical_close_preserves_tcp() {
    tokio::time::timeout(Duration::from_secs(12), async {
        let server = Server::new().await;
        let fixture = Assembly::new(server.address);
        let mut config = (*fixture.config()).clone();
        config.outbounds.truncate(1);
        let config = Arc::new(config);
        let host = &fixture.catalog;
        host.drive_supervised(async {
            host.start(config.clone(), deadline()).await.unwrap();
            host.commit(&config, || Ok(())).unwrap();
            let slot = host.current(&config).unwrap().direct(Some("a")).unwrap();
            let mut tcp = fixture.open(&slot).await;
            let mut udp = slot
                .datagram(deadline())
                .await
                .unwrap()
                .open(
                    DatagramRequest {
                        association: AssociationId(7),
                        targets: DatagramTargets::Multiple,
                    },
                    fixture.execution_context(&slot),
                )
                .await
                .unwrap();
            assert_eq!(server.accepts.load(Ordering::SeqCst), 1);
            assert_eq!(fixture.pool.snapshot().connections, 1);
            for payload in [b"udp".as_slice(), b"".as_slice()] {
                let frame = Datagram {
                    address: Destination::new(Address::Ipv4([203, 0, 113, 9]), 53),
                    association: AssociationId(7),
                    payload: payload.to_vec().into(),
                };
                send(udp.tx.as_mut(), udp.control.as_mut(), &frame, &udp.limits)
                    .await
                    .unwrap();
                assert_eq!(recv(udp.rx.as_mut()).await.unwrap().unwrap(), frame);
            }
            udp.control.cancel(CloseReason::Shutdown);
            drop(udp.tx);
            let report = udp
                .control
                .wait_closed(tokio::time::Instant::now() + Duration::from_millis(30))
                .await
                .unwrap();
            assert!(
                !report.is_complete(),
                "retained Rx is still an actual logical user"
            );
            drop(udp.rx);
            let report = udp.control.wait_closed(deadline()).await.unwrap();
            assert!(report.is_complete(), "{report:?}");
            assert!(
                report.closed_io.is_empty(),
                "logical control cannot sign a native FD receipt"
            );
            tcp.write_all(b"still-alive").await.unwrap();
            let mut echoed = [0; 11];
            tcp.read_exact(&mut echoed).await.unwrap();
            assert_eq!(&echoed, b"still-alive");
            assert_eq!(server.accepts.load(Ordering::SeqCst), 1);
            drop(tcp);
            drop(udp.control);
            host.shutdown(deadline()).await.unwrap();
            assert_eq!(fixture.pool.snapshot().connections, 0);
        })
        .await;
        server.close().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn retained_udp_receive_view_prevents_whole_pool_replacement() {
    tokio::time::timeout(Duration::from_secs(12), async {
        let server = Server::new().await;
        let fixture = Assembly::new(server.address);
        let config = fixture.config();
        let host = &fixture.catalog;
        host.drive_supervised(async {
            host.start(config.clone(), deadline()).await.unwrap();
            host.commit(&config, || Ok(())).unwrap();
            let slot = host.current(&config).unwrap().direct(Some("a")).unwrap();
            let udp = slot
                .datagram(deadline())
                .await
                .unwrap()
                .open(
                    DatagramRequest {
                        association: AssociationId(8),
                        targets: DatagramTargets::Multiple,
                    },
                    fixture.execution_context(&slot),
                )
                .await
                .unwrap();
            let mut changed = (*config).clone();
            if let OutboundProtocolConfig::Mieru { password, .. } =
                &mut changed.outbounds[1].protocol
            {
                *password = "changed-unused".into();
            }
            let changed = Arc::new(changed);
            let mut replacing = Box::pin(host.start(changed.clone(), deadline()));
            std::future::poll_fn(|cx| {
                assert!(replacing.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            drop(udp.tx);
            drop(udp.control);
            assert!(
                tokio::time::timeout(Duration::from_millis(30), replacing.as_mut())
                    .await
                    .is_err()
            );
            assert_eq!(fixture.builds.load(Ordering::SeqCst), 1);
            drop(udp.rx);
            replacing.await.unwrap();
            host.commit(&changed, || Ok(())).unwrap();
            assert_eq!(fixture.builds.load(Ordering::SeqCst), 2);
            host.shutdown(deadline()).await.unwrap();
        })
        .await;
        server.close().await;
    })
    .await
    .unwrap();
}
