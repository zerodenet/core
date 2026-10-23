//! Actual protocol authentication over the same canonical H3/user/native graph.
//! Pool/dispatcher/mask/hop production ownership is not inferred from this test.
use super::*;
use zero_traits::UdpDatagramFraming;

#[tokio::test]
async fn actual_hy2_auth_exchange_and_sealed_udp_keep_the_original_canonical_users_until_drop() {
    tokio::time::timeout(Duration::from_secs(12), async {
        let peer = Peer::start_hysteria2().await;
        let (fixture, owner, mut receiver, connection, tasks) = connect_owner(&peer).await;
        let completion = owner.users_completion().unwrap();
        // A distinct real datagram user receives its own existing finite lease;
        // it is never a raw Quinn clone or an untracked constructor capability.
        let datagrams = connection.try_clone().unwrap();
        let mut pending = owner.prepare_http3(connection).unwrap();
        let mut sender = fixture.drive(pending.ready()).await.unwrap();
        let settings = ::hysteria2::settings::Settings {
            upload: 4_000_000,
            download: 2_000_000,
            ..Default::default()
        };
        let negotiated = fixture
            .drive(::hysteria2::transport::authenticate_http3_request(
                &mut sender,
                "actual-password",
                settings,
            ))
            .await
            .unwrap();
        assert!(negotiated.udp_enabled);
        assert_eq!(
            negotiated.receive_bandwidth,
            ::hysteria2::handshake::ReceiveBandwidth::Limit(3_000_000)
        );
        assert_eq!(settings.client_send_rate(negotiated.receive_bandwidth), 3_000_000);
        let target = zero_core::Address::Domain("actual.hy2.test".into());
        let bytes = ::hysteria2::Hysteria2Outbound
            .encode_udp_datagram(&::hysteria2::udp::Hysteria2UdpPacketTarget {
                session_id: 17,
                packet_id: 0,
                target: &target,
                port: 5353,
                payload: b"actual HY2 datagram",
            })
            .unwrap();
        datagrams.send_datagram(bytes.into()).unwrap();
        let response = fixture.drive(datagrams.read_datagram()).await.unwrap();
        let response = ::hysteria2::Hysteria2Outbound
            .decode_udp_datagram(&(), &response)
            .unwrap();
        assert_eq!(response.session_id(), 17);
        assert_eq!(response.target(), &target);
        assert_eq!(response.port(), 5353);
        assert_eq!(response.payload(), b"actual HY2 datagram");

        owner.begin_shutdown();
        receive_all(&fixture, &owner, &mut receiver).await;
        drop(owner);
        drop(sender);
        assert!(completion.try_closed().is_none(), "actual datagram connection user survives all original driver joins and authentication sender release");
        drop(datagrams);
        let receipt = completion.try_closed().unwrap();
        assert!(!fixture.resources().io_evidence().physical_confirmed());
        fixture.confirm_users(receipt, &tasks).unwrap();
        assert!(fixture.shutdown().await.is_complete());
        let proof = fixture.resources().io_evidence();
        assert_eq!(proof.metadata[0].confirmed_users(), 1);
        assert_eq!(proof.metadata[0].main.confirmed_closed, 1);
        assert!(proof.physical_confirmed());
        peer.finish().await;
    })
    .await
    .unwrap();
}
