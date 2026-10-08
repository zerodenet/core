use std::time::{Duration, Instant};

use zero_api::OutboundDeviceHealthState;

use super::{DeviceHealth, SharedRawIpDevice};

#[tokio::test]
async fn owned_packet_handoff_moves_buffers_and_rejects_full_queue_without_consuming() {
    use tokio::sync::{mpsc, watch};
    use zero_stack::packet;
    let (sender, mut received) = mpsc::channel(128);
    let (_endpoint, endpoint) = watch::channel(sender);
    let device =
        SharedRawIpDevice::start_on_endpoint(vec!["10.0.0.1".parse().unwrap()], 1420, 3, endpoint)
            .unwrap();
    let source = "10.0.0.2".parse().unwrap();
    let (replies, _receiver) = mpsc::channel(1);
    let original = packet::build_udp(source, "192.0.2.1".parse().unwrap(), 40000, 443, b"owned");
    let mut outgoing = vec![original.clone()];
    let pointer = outgoing[0].as_ptr() as usize;
    device
        .forward_packets(
            &mut outgoing,
            source,
            1,
            replies.clone(),
            None,
            packet::packet_conversation_key(&original),
        )
        .unwrap();
    assert!(outgoing.is_empty());
    // No yields until all 128 slots are reserved on this current-thread runtime.
    for _ in 1..128 {
        device
            .forward_packets(
                &mut vec![original.clone()],
                source,
                1,
                replies.clone(),
                None,
                None,
            )
            .unwrap();
    }
    let mut refused = vec![original.clone()];
    let refused_pointer = refused[0].as_ptr();
    assert!(device
        .forward_packets(&mut refused, source, 1, replies.clone(), None, None)
        .is_err());
    assert_eq!(refused[0], original);
    assert_eq!(refused[0].as_ptr(), refused_pointer);
    let received = tokio::time::timeout(Duration::from_secs(1), received.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received.peer, 3);
    assert_eq!(received.packet.as_ptr() as usize, pointer);
    assert_eq!(received.packet, original);
    device.close_now();
    assert!(device
        .forward_packets(&mut refused, source, 1, replies, None, None)
        .is_err());
    assert_eq!(refused[0].as_ptr(), refused_pointer);
}

#[tokio::test]
async fn wireguard_opaque_carrier_does_not_invent_a_wire_source() {
    use std::{
        io,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::Arc,
    };
    use tokio::sync::{mpsc, Mutex};
    use zero_engine::EngineError;

    use super::super::{RawIpAction, RawIpTunnel, RawIpWireCarrier};

    struct Carrier {
        incoming: Mutex<mpsc::Receiver<(Vec<u8>, Option<SocketAddr>)>>,
    }

    #[async_trait::async_trait]
    impl RawIpWireCarrier for Carrier {
        async fn recv(&self, buf: &mut [u8]) -> io::Result<(usize, Option<SocketAddr>)> {
            let (packet, source) = self.incoming.lock().await.recv().await.unwrap();
            buf[..packet.len()].copy_from_slice(&packet);
            Ok((packet.len(), source))
        }

        async fn send(&self, _: &[u8], _: SocketAddr) -> io::Result<()> {
            Ok(())
        }
    }

    struct Tunnel(mpsc::Sender<Option<SocketAddr>>);

    impl RawIpTunnel for Tunnel {
        fn initiate_handshake(&mut self) -> Result<Vec<RawIpAction>, EngineError> {
            Ok(Vec::new())
        }

        fn send_ip_packet(&mut self, _: &[u8]) -> Result<Vec<RawIpAction>, EngineError> {
            Ok(Vec::new())
        }

        fn receive_datagram(
            &mut self,
            source: Option<SocketAddr>,
            _: &[u8],
        ) -> Result<Vec<RawIpAction>, EngineError> {
            self.0.try_send(source).unwrap();
            Ok(Vec::new())
        }

        fn tick(&mut self) -> Result<Vec<RawIpAction>, EngineError> {
            Ok(Vec::new())
        }

        fn allows_source(&self, _: IpAddr) -> bool {
            true
        }
    }

    let configured_endpoint: SocketAddr = "192.0.2.1:51820".parse().unwrap();
    let (incoming, receiver) = mpsc::channel(2);
    let (seen, mut observed) = mpsc::channel(2);
    let device = SharedRawIpDevice::start_with_carrier(
        vec![IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))],
        1_420,
        configured_endpoint,
        Arc::new(Carrier {
            incoming: Mutex::new(receiver),
        }),
        Box::new(Tunnel(seen)),
    )
    .unwrap();
    device.wait_ready().await.unwrap();
    incoming.send((vec![1], None)).await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), observed.recv())
            .await
            .unwrap(),
        Some(None)
    );
    let actual_source: SocketAddr = "192.0.2.2:51821".parse().unwrap();
    incoming.send((vec![2], Some(actual_source))).await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), observed.recv())
            .await
            .unwrap(),
        Some(Some(actual_source))
    );
    device.close_now();
}

#[tokio::test]
async fn endpoint_stack_reconnects_packet_queue_after_listener_restart() {
    use std::net::{IpAddr, Ipv4Addr};
    use tokio::sync::{mpsc, watch};
    use zero_stack::packet::Endpoint;

    let (sender, mut packets) = mpsc::channel(2);
    let (endpoint, receiver) = watch::channel(sender);
    let device = SharedRawIpDevice::start_on_endpoint(
        vec![IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))],
        1_420,
        3,
        receiver,
    )
    .unwrap();
    let socket = device
        .bind_udp(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)))
        .unwrap();
    let target = Endpoint {
        ip: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
        port: 53,
    };
    socket.send_to(b"first", target).await.unwrap();
    let first = tokio::time::timeout(Duration::from_secs(1), packets.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.peer, 3);
    drop(packets);
    socket.send_to(b"second", target).await.unwrap();
    let (sender, mut packets) = mpsc::channel(2);
    endpoint.send_replace(sender);
    let second = tokio::time::timeout(Duration::from_secs(1), packets.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.peer, 3);
    assert!(!device.is_closed());
    device.close_now();
}

#[test]
fn authenticated_data_and_stale_handshake_have_distinct_states() {
    let now = Instant::now();
    let mut health = DeviceHealth {
        last_handshake: now.checked_sub(Duration::from_secs(181)),
        last_authenticated_packet: None,
    };
    assert_eq!(
        health.snapshot("wg".to_owned(), 0, false).state,
        OutboundDeviceHealthState::Degraded
    );

    health.last_authenticated_packet = Some(now);
    assert_eq!(
        health.snapshot("wg".to_owned(), 0, false).state,
        OutboundDeviceHealthState::Reachable
    );

    health.last_handshake = Some(Instant::now());
    assert_eq!(
        health.snapshot("wg".to_owned(), 0, false).state,
        OutboundDeviceHealthState::RecentlyHandshaken
    );
}
