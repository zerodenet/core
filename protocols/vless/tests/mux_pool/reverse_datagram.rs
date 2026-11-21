//! Library framing/custody checks; native provider qualification stays in Proxy.
use super::*;
use std::future::Future;
use zero_transport::pooled_driver::datagram::PacketIo;

#[tokio::test]
async fn reverse_udp_future_keeps_its_real_sid_until_admission_or_cancellation() {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let (socket, mut peer) = tokio::io::duplex(1);
        let (connection, mut driver) = MuxPoolConn::prepare(
            socket,
            &[0; 16],
            1,
            None,
            MuxResponseBacklogPolicy::default(),
            false,
        );
        let connection = Arc::new(connection);
        let cold_sid = connection.try_reserve_stream_id().unwrap();
        let cold = super::super::reverse_datagram(
            connection.clone(),
            cold_sid,
            crate::mux::origin::Origin::default(),
            None,
            None,
        );
        assert!(connection.streams.lock().unwrap().contains_key(&cold_sid));
        assert_eq!(connection.active_connections(), 1);
        drop(cold);
        assert!(!connection.streams.lock().unwrap().contains_key(&cold_sid));
        assert_eq!(connection.active_connections(), 0);

        let first_sid = connection.try_reserve_stream_id().unwrap();
        let mut first = super::super::reverse_datagram(
            connection.clone(),
            first_sid,
            crate::mux::origin::Origin::default(),
            None,
            None,
        )
        .await
        .unwrap();
        let packet = zero_core::packet::Datagram {
            association: zero_core::component::AssociationId(1),
            address: zero_core::request::Destination::new(Address::Ipv4([203, 0, 113, 1]), 5353),
            payload: b"actual UDP".to_vec().into(),
        };
        first.send(&packet).unwrap();
        drop(first);
        // The real driver cannot flush its first frame through this one-byte
        // duplex while the original peer withholds reads. Its queued END keeps
        // the actual control permit, so the next admission really suspends.
        std::future::poll_fn(|cx| {
            assert!(driver.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let cancelled_sid = connection.try_reserve_stream_id().unwrap();
        let mut pending = Box::pin(super::super::reverse_datagram(
            connection.clone(),
            cancelled_sid,
            crate::mux::origin::Origin::default(),
            None,
            None,
        ));
        std::future::poll_fn(|cx| {
            assert!(pending.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert!(connection
            .streams
            .lock()
            .unwrap()
            .contains_key(&cancelled_sid));
        drop(pending);
        assert!(!connection
            .streams
            .lock()
            .unwrap()
            .contains_key(&cancelled_sid));
        assert_eq!(connection.active_connections(), 0);

        let sid = connection.try_reserve_stream_id().unwrap();
        let fresh = super::super::reverse_datagram(
            connection.clone(),
            sid,
            crate::mux::origin::Origin::default(),
            None,
            None,
        );
        let physical = tokio::spawn(driver);
        let frame = crate::mux::read_mux_frame_tokio(&mut peer).await.unwrap();
        assert_eq!(frame.session_id, first_sid);
        assert_eq!(frame.status, crate::mux::STATUS_NEW);
        assert_eq!(frame.payload, b"actual UDP");
        let end = crate::mux::read_mux_frame_tokio(&mut peer).await.unwrap();
        assert_eq!(end.session_id, first_sid);
        assert_eq!(end.status, crate::mux::STATUS_END);
        let mut fresh = fresh.await.unwrap();
        assert!(connection.streams.lock().unwrap().contains_key(&sid));
        fresh.send(&packet).unwrap();
        let frame = crate::mux::read_mux_frame_tokio(&mut peer).await.unwrap();
        assert_eq!(frame.session_id, sid);
        assert_eq!(frame.payload, b"actual UDP");
        drop(fresh);
        let end = crate::mux::read_mux_frame_tokio(&mut peer).await.unwrap();
        assert_eq!(end.session_id, sid);
        assert_eq!(end.status, crate::mux::STATUS_END);
        drop(peer);
        physical.await.unwrap();
        assert!(connection.closed());
        assert_eq!(connection.active_connections(), 0);
    })
    .await
    .unwrap();
}
