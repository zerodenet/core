use super::super::*;
#[tokio::test]
async fn initial_udp_data_flag_distinguishes_empty_packet_from_empty_association_open() {
    for data in [false, true] {
        let sid = 7;
        let target = Address::Ipv4([127, 0, 0, 1]);
        let wire = if data {
            codec::encode_new_udp_data_frame_optional(sid, &target, 8080, None, &[]).unwrap()
        } else {
            codec::encode_new_stream(sid, NETWORK_UDP, 8080, &target).unwrap()
        };
        let frame = codec::read_frame_tokio(&mut wire.as_slice()).await.unwrap();
        assert_eq!(frame.options & OPTION_DATA != 0, data);
        assert!(frame.payload.is_empty());
        let action: VlessInboundMuxAction = MuxServer::frame_event(frame).unwrap().into();
        let mut states = VlessInboundMuxStreams::new();
        let mut mux = VlessInboundMuxSession::new();
        let (transport, _) = tokio::io::duplex(64);
        let mut transport = zero_platform_tokio::TcpRelayStream::new(transport);
        let opened = states
            .apply_inbound_action(&mut mux, &mut transport, action)
            .await
            .unwrap()
            .unwrap();
        let (writer, _) = VlessInboundMuxWriter::channel(MuxResponseBacklogPolicy::default());
        let route = opened.into_route_with_auth(None, writer);
        let VlessInboundMuxOpenedRouteState::Udp { mut relay } = route.state else {
            panic!("actual UDP route")
        };
        let termination = relay.termination_probe.clone();
        if data {
            let request = relay
                .read_inbound_dispatch()
                .await
                .unwrap()
                .expect("real initial empty packet");
            let (_, actual, port, payload, _) = request.into_parts();
            assert_eq!(actual, target);
            assert_eq!(port, 8080);
            assert!(payload.is_empty());
        } else {
            assert!(matches!(
                relay.up_rx.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ));
        }
        assert!(states.contains_stream(sid));
        states
            .apply_inbound_action(
                &mut mux,
                &mut transport,
                VlessInboundMuxAction::End { session_id: sid },
            )
            .await
            .unwrap();
        assert!(!states.contains_stream(sid));
        assert_eq!(
            termination.reason(),
            zero_core::InboundMuxUdpTermination::ExplicitEnd
        );
        assert!(relay.read_inbound_dispatch().await.unwrap().is_none());
    }
}
