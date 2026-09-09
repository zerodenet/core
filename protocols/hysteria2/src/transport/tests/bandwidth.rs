use super::{auth::authenticate_http3_with_settings, test_fixtures};
use crate::{
    handshake::ReceiveBandwidth,
    settings::{Congestion, Settings},
};
use std::time::Instant;
use tokio::time::{timeout, Duration};

#[tokio::test]
async fn configured_bandwidth_preserves_negotiated_loss_compensation_on_both_peers() {
    timeout(Duration::from_secs(10), async {
        for disabled in [false, true] {
            let client_settings = Settings {
                upload: 1_000_000,
                download: 3_000_000,
                disable_loss_compensation: disabled,
                ..Default::default()
            };
            let server_settings = Settings {
                upload: 2_000_000,
                download: 4_000_000,
                disable_loss_compensation: disabled,
                ..Default::default()
            };
            // Install the same settings on the actual QUIC carrier and HTTP/3
            // profiles. Testing only headers misses an accidental carrier cap.
            let (client, server) =
                test_fixtures::pair_with_settings(client_settings, server_settings).await;
            let profile = test_fixtures::profile().with_settings(server_settings);
            let (client, server) = tokio::join!(
                authenticate_http3_with_settings(client, "test-password", client_settings),
                profile.accept_authenticated_connection(server),
            );
            let (client, server) = (client.unwrap(), server.unwrap());
            assert_eq!(
                client.negotiated().receive_bandwidth,
                ReceiveBandwidth::Limit(4_000_000)
            );
            let server_connection = server.datagram_source();
            for (connection, target) in [
                (client.connection(), 1_000_000),
                (&*server_connection, 2_000_000),
            ] {
                let mut controller = connection.congestion_state();
                assert_eq!(controller.pacing_rate(), Some(target));
                // Inject deterministic feedback into the installed controller's
                // snapshot, not the live network. Exceed the 50-packet threshold
                // and reach the official 0.8 ACK floor despite handshake ACKs.
                controller.on_packets_lost(Instant::now(), 10_000);
                let compensated = if disabled { target } else { target * 5 / 4 };
                assert_eq!(controller.pacing_rate(), Some(compensated));
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn auto_and_zero_bandwidth_keep_configured_adaptive_controller_uncapped() {
    timeout(Duration::from_secs(10), async {
        // An explicit server auto response overrides nonzero client rates.
        // With zero client RX, the server remains adaptive despite its own TX.
        for (ignore, client_receive, client_send, server_receive) in
            [(true, 2_000_000, 1_000_000, 3_000_000), (false, 0, 0, 0)]
        {
            let client_settings = Settings {
                upload: client_send,
                download: client_receive,
                congestion: Congestion::Reno,
                ..Default::default()
            };
            let server_settings = Settings {
                upload: 1_000_000,
                download: server_receive,
                ignore_client_bandwidth: ignore,
                congestion: Congestion::Reno,
                ..Default::default()
            };
            let (client, server) =
                test_fixtures::pair_with_settings(client_settings, server_settings).await;
            let profile = test_fixtures::profile().with_settings(server_settings);
            let (client, server) = tokio::join!(
                authenticate_http3_with_settings(client, "test-password", client_settings),
                profile.accept_authenticated_connection(server),
            );
            let (client, server) = (client.unwrap(), server.unwrap());
            assert_eq!(
                client.negotiated().receive_bandwidth,
                if ignore {
                    ReceiveBandwidth::Auto
                } else {
                    ReceiveBandwidth::Limit(0)
                }
            );
            let server_connection = server.datagram_source();
            for connection in [client.connection(), &*server_connection] {
                // Reno uses the window/RTT pacer, without a fixed-rate override.
                assert_eq!(connection.congestion_state().pacing_rate(), None);
            }
        }
    })
    .await
    .unwrap();
}
