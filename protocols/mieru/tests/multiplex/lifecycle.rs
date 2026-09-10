use super::fixture::*;
use mieru::{inbound::MieruInboundAcceptedSession, metadata::*};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::timeout,
};
use zero_core::{InboundRouteMultiplexer, InboundStreamUdpRelay, Network};

#[tokio::test]
async fn concurrent_tcp_udp_and_incomplete_handshake_are_isolated() {
    timeout(Duration::from_secs(5), async {
        let (mut peer, connection) = connect(&[5]).await;
        let first = connection.accept_next().await.unwrap().unwrap();
        let mut request = REQUEST.to_vec();
        request.extend_from_slice(b"early");
        peer.control(202, OPEN_SESSION_REQUEST, &request).await;
        assert_eq!(peer.next().await.session_meta.unwrap().session_id, 202);
        let second = connection.accept_next().await.unwrap().unwrap();
        let MieruInboundAcceptedSession::Tcp {
            session,
            mut stream,
        } = connection.accept_route(second).await.unwrap()
        else {
            panic!("expected tcp")
        };
        assert_eq!(
            session.auth.unwrap().principal_key.as_deref(),
            Some("principal-1")
        );
        assert_eq!(peer.next().await.payload, [5, 0, 0, 1, 0, 0, 0, 0, 0, 0]);
        let mut early = [0; 5];
        stream.read_exact(&mut early).await.unwrap();
        assert_eq!(&early, b"early");
        let udp = [5, 3, 0, 1, 0, 0, 0, 0, 0, 0];
        peer.control(303, OPEN_SESSION_REQUEST, &udp).await;
        assert_eq!(peer.next().await.session_meta.unwrap().session_id, 303);
        let third = connection.accept_next().await.unwrap().unwrap();
        let MieruInboundAcceptedSession::Udp { session, relay } =
            connection.accept_route(third).await.unwrap()
        else {
            panic!("expected udp")
        };
        assert_eq!(session.network, Network::Udp);
        let (mut udp_stream, _, _) = relay.into_stream_udp_parts();
        peer.next().await;
        peer.data(303, b"udp").await;
        peer.data(202, b"tcp").await;
        let mut bytes = [0; 3];
        stream.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"tcp");
        udp_stream.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"udp");
        drop(first);
        assert_eq!(peer.next().await.session_meta.unwrap().session_id, 101);
        stream.write_all(b"still alive").await.unwrap();
        assert_eq!(peer.next().await.payload, b"still alive");
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn peer_close_and_unknown_id_do_not_close_other_sessions() {
    timeout(Duration::from_secs(5), async {
        let (mut peer, connection) = connect(b"one").await;
        let mut first = connection.accept_next().await.unwrap().unwrap();
        peer.control(202, OPEN_SESSION_REQUEST, b"two").await;
        peer.next().await;
        let mut second = connection.accept_next().await.unwrap().unwrap();
        peer.control(101, CLOSE_SESSION_REQUEST, &[]).await;
        let close = peer.next().await.session_meta.unwrap();
        assert_eq!(
            (close.protocol_type, close.session_id),
            (CLOSE_SESSION_RESPONSE, 101)
        );
        let mut bytes = Vec::new();
        first.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"one");
        peer.data(999, b"unknown").await;
        assert_eq!(peer.next().await.session_meta.unwrap().session_id, 999);
        peer.data(202, b"alive").await;
        let mut bytes = [0; 8];
        second.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"twoalive");
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn large_writes_fragment_and_shutdown_is_logical() {
    timeout(Duration::from_secs(5), async {
        let (mut peer, connection) = connect(&[]).await;
        let mut stream = connection.accept_next().await.unwrap().unwrap();
        let payload: Vec<u8> = (0..100_000).map(|n| (n % 251) as u8).collect();
        stream.write_all(&payload).await.unwrap();
        let mut received = Vec::new();
        let mut seq = 1;
        while received.len() < payload.len() {
            let frame = peer.next().await;
            assert_eq!(frame.data_meta.unwrap().sequence_number, seq);
            seq += 1;
            received.extend(frame.payload);
        }
        assert_eq!(received, payload);
        stream.shutdown().await.unwrap();
        assert_eq!(
            peer.next().await.session_meta.unwrap().protocol_type,
            CLOSE_SESSION_REQUEST
        );
        assert!(stream.write_all(b"after close").await.is_err());
        peer.control(202, OPEN_SESSION_REQUEST, b"new").await;
        assert_eq!(peer.next().await.session_meta.unwrap().session_id, 202);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn slow_session_fails_explicitly_while_other_sessions_continue() {
    timeout(Duration::from_secs(5), async {
        // Exercise an explicit short policy; default 30s behavior has a virtual-time unit test.
        let mut options = mieru::config::MieruTransportOptions::default();
        options.receive.stall_timeout_ms = 1000;
        let (mut peer, connection) =
            super::fixture::connect_with_options(&[], 1 << 20, options).await;
        let mut slow = connection.accept_next().await.unwrap().unwrap();
        for _ in 0..9 {
            peer.data(101, b"buffered").await;
        }
        peer.control(202, OPEN_SESSION_REQUEST, b"healthy").await;
        // The healthy OPEN is answered before the stalled session's deadline.
        let reply = peer.next().await.session_meta.unwrap();
        assert_eq!(reply.protocol_type, OPEN_SESSION_RESPONSE);
        assert_eq!(reply.session_id, 202);
        let mut other = connection.accept_next().await.unwrap().unwrap();
        let mut bytes = [0; 7];
        other.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"healthy");
        let close = peer.next().await.session_meta.unwrap();
        assert_eq!(close.protocol_type, CLOSE_SESSION_REQUEST);
        assert_eq!(close.session_id, 101);
        let mut bytes = Vec::new();
        let error = slow.read_to_end(&mut bytes).await.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::ConnectionAborted);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn truncated_underlay_and_connection_drop_wake_readers() {
    timeout(Duration::from_secs(5), async {
        let (mut peer, connection) = connect(&[]).await;
        let mut stream = connection.accept_next().await.unwrap().unwrap();
        peer.socket.write_all(&[1, 2, 3]).await.unwrap();
        peer.socket.shutdown().await.unwrap();
        assert!(connection.accept_next().await.is_err());
        assert!(stream.read(&mut [0; 1]).await.is_err());
        let (_peer, connection) = connect(&[]).await;
        let mut stream = connection.accept_next().await.unwrap().unwrap();
        drop(connection);
        assert!(stream.read(&mut [0; 1]).await.is_err());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn dropping_retired_session_cannot_remove_a_reused_wire_id() {
    timeout(Duration::from_secs(5), async {
        let (mut peer, connection) = connect(&[]).await;
        let old = connection.accept_next().await.unwrap().unwrap();
        peer.control(101, CLOSE_SESSION_REQUEST, &[]).await;
        peer.next().await;
        peer.control(101, OPEN_SESSION_REQUEST, b"new").await;
        peer.next().await;
        let mut current = connection.accept_next().await.unwrap().unwrap();
        drop(old);
        // Another accepted frame acts as a scheduling boundary while the drop
        // event is handled; the reused ID must remain usable in either ordering.
        peer.control(202, OPEN_SESSION_REQUEST, &[]).await;
        peer.next().await;
        peer.data(101, b"data").await;
        let mut bytes = [0; 7];
        current.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"newdata");
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn reserved_or_duplicate_ids_fail_the_underlay_without_cross_delivery() {
    for id in [0, 101] {
        timeout(Duration::from_secs(5), async {
            let (mut peer, connection) = connect(&[]).await;
            let mut stream = connection.accept_next().await.unwrap().unwrap();
            peer.control(id, OPEN_SESSION_REQUEST, b"must not arrive")
                .await;
            assert!(connection.accept_next().await.is_err());
            assert!(stream.read(&mut [0; 20]).await.is_err());
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn flush_waits_for_the_underlay_and_sender_applies_backpressure() {
    timeout(Duration::from_secs(5), async {
        let (mut peer, connection) = connect_with_capacity(&[], 128).await;
        let mut stream = connection.accept_next().await.unwrap().unwrap();
        let payload = vec![37; 100_000];
        stream.write_all(&payload).await.unwrap();
        assert!(
            timeout(Duration::from_millis(20), stream.flush())
                .await
                .is_err(),
            "flush acknowledged unsent bytes"
        );
        let read = async {
            let mut response = Vec::new();
            while response.len() < payload.len() {
                response.extend(peer.next().await.payload);
            }
            assert_eq!(response, payload);
        };
        let ((), result) = tokio::join!(read, stream.flush());
        result.unwrap();
        assert!(
            timeout(
                Duration::from_millis(20),
                stream.write_all(&vec![38; 4 << 20])
            )
            .await
            .is_err(),
            "writes accepted an unbounded backlog"
        );
        drop(connection);
        assert!(stream.flush().await.is_err());
    })
    .await
    .unwrap();
}
