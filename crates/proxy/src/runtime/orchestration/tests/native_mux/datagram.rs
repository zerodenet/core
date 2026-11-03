//! Real standard Datagram calls through the actual Proxy assembly and wire peers.
use super::*;
use zero_core::{
    component::AssociationId,
    packet::Datagram,
    request::{DatagramTargets, Destination},
};
use zero_platform_tokio::contract::{
    datagram::{recv, send, BoxDatagramSession, DatagramRequest},
    DeliveryState,
};
mod ownership;

mod fixtures;
use fixtures::*;

#[tokio::test]
async fn native_mux_standard_datagram_preserves_packets_and_shares_actual_tcp_pool() {
    timeout(Duration::from_secs(40), async {
        for protocol in ["vless", "vmess", "trojan"] {
            let server = Server::start(protocol).await;
            let udp = UdpEcho::start().await;
            let proxy = Proxy::from_engine_with_background(
                zero_engine::Engine::new(config(protocol, server.fixture.front.port())).unwrap(),
                true,
            )
            .unwrap();
            start_components(&proxy).await;
            let mut tcp = connect(&proxy, server.fixture.echo.port()).await.unwrap();
            echo(&mut tcp).await;
            let registration = registration(&proxy);
            let observed = registration.observe().unwrap();
            let original = observed.slots()[0].original().clone();
            let driver = observed.slots()[0].driver().unwrap().1.clone();
            let mut udp_channel = open(&proxy, 10, DatagramTargets::Fixed(udp.target()))
                .await
                .unwrap();
            // Back-to-back differently sized wire packets must never merge or truncate.
            let large = vec![b'Z'; 8192];
            send_packet(&mut udp_channel, 10, udp.target(), b"first").await;
            send_packet(&mut udp_channel, 10, udp.target(), &large).await;
            receive(&mut udp_channel, 10, udp.target(), b"first").await;
            receive(&mut udp_channel, 10, udp.target(), &large).await;
            if protocol != "trojan" {
                send_packet(&mut udp_channel, 10, udp.target(), b"").await;
                receive(&mut udp_channel, 10, udp.target(), b"").await;
            }
            // The current-thread executor cannot drain the actual physical
            // queue during this single poll: older accepted frames stay queued.
            std::future::poll_fn(|cx| {
                for value in 0..9_u8 {
                    let frame = Datagram {
                        association: AssociationId(10),
                        address: udp.target(),
                        payload: vec![value].into(),
                    };
                    let polled = udp_channel.tx.as_mut().poll_send(cx, &frame);
                    if value < 8 {
                        assert!(matches!(polled, std::task::Poll::Ready(Ok(_))));
                    } else {
                        let std::task::Poll::Ready(Err(failure)) = polled else {
                            panic!("bounded native packet queue accepted a ninth frame")
                        };
                        assert_eq!(failure.delivery, DeliveryState::NotSubmitted);
                        assert_eq!(
                            failure.error.reason,
                            zero_platform_tokio::contract::Reason::Capacity
                        );
                    }
                }
                std::task::Poll::Ready(())
            })
            .await;
            for value in 0..8_u8 {
                receive(&mut udp_channel, 10, udp.target(), &[value]).await;
            }
            assert_eq!(server.fixture.connections(), 1);
            assert_eq!(registration.observe().unwrap().slots().len(), 1);
            assert!(connect(&proxy, server.fixture.echo.port()).await.is_err());
            close(udp_channel).await;
            assert!(
                !driver.is_joined() && original.closed().is_none(),
                "closing UDP must leave the shared TCP physical owner alive"
            );
            echo(&mut tcp).await;
            drop(tcp);
            proxy
                .protocols
                .shutdown_components(deadline())
                .await
                .unwrap();
            assert!(driver.matches_joined(&driver.try_joined().unwrap()));
            assert!(original.matches_closed(&original.closed().unwrap()));
            assert!(registration.closed().unwrap().matches(&registration));
            assert!(registration.users_closed().unwrap().matches(&registration));
            udp.close().await;
            server.close().await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn native_mux_datagram_multiple_targets_and_invalid_packets_do_not_change_pool_custody() {
    timeout(Duration::from_secs(40), async {
        for protocol in ["vless", "vmess", "trojan"] {
            let server = Server::start(protocol).await;
            let udp = UdpEcho::start().await;
            let other = UdpEcho::start().await;
            let proxy = Proxy::from_engine_with_background(
                zero_engine::Engine::new(config(protocol, server.fixture.front.port())).unwrap(),
                true,
            )
            .unwrap();
            start_components(&proxy).await;
            let multiple = open(&proxy, 30, DatagramTargets::Multiple).await;
            if protocol == "trojan" {
                assert!(multiple.is_err());
                assert!(registration(&proxy).observe().unwrap().slots().is_empty());
            } else {
                let mut channel = multiple.unwrap();
                send_packet(&mut channel, 30, udp.target(), b"one").await;
                receive(&mut channel, 30, udp.target(), b"one").await;
                send_packet(&mut channel, 30, other.target(), b"two").await;
                receive(&mut channel, 30, other.target(), b"two").await;
                close(channel).await;
            }
            let mut channel = open(&proxy, 31, DatagramTargets::Fixed(udp.target()))
                .await
                .unwrap();
            let invalid = Datagram {
                association: AssociationId(32),
                address: udp.target(),
                payload: vec![1].into(),
            };
            let failed = send(
                channel.tx.as_mut(),
                channel.control.as_mut(),
                &invalid,
                &channel.limits,
            )
            .await
            .unwrap_err();
            assert_eq!(failed.delivery, DeliveryState::NotSubmitted);
            let wrong = Datagram {
                association: AssociationId(31),
                address: other.target(),
                payload: vec![1].into(),
            };
            assert_eq!(
                send(
                    channel.tx.as_mut(),
                    channel.control.as_mut(),
                    &wrong,
                    &channel.limits
                )
                .await
                .unwrap_err()
                .delivery,
                DeliveryState::NotSubmitted
            );
            if protocol == "trojan" {
                let empty = Datagram {
                    association: AssociationId(31),
                    address: udp.target(),
                    payload: Vec::new().into(),
                };
                assert_eq!(
                    send(
                        channel.tx.as_mut(),
                        channel.control.as_mut(),
                        &empty,
                        &channel.limits
                    )
                    .await
                    .unwrap_err()
                    .delivery,
                    DeliveryState::NotSubmitted
                );
            }
            let oversized = Datagram {
                association: AssociationId(31),
                address: udp.target(),
                payload: vec![0; channel.limits.max_payload + 1].into(),
            };
            assert_eq!(
                send(
                    channel.tx.as_mut(),
                    channel.control.as_mut(),
                    &oversized,
                    &channel.limits
                )
                .await
                .unwrap_err()
                .delivery,
                DeliveryState::NotSubmitted
            );
            send_packet(&mut channel, 31, udp.target(), b"still-live").await;
            receive(&mut channel, 31, udp.target(), b"still-live").await;
            close(channel).await;
            proxy
                .protocols
                .shutdown_components(deadline())
                .await
                .unwrap();
            udp.close().await;
            other.close().await;
            server.close().await;
        }
    })
    .await
    .unwrap();
}
