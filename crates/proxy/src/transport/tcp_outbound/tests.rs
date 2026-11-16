//! Local wire representation regressions, not native-provider qualification.
use super::*;
use crate::transport::{ClientStream, TcpRelayStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_platform_tokio::{contract::stream::BoxStream, ApplicationSettings};
use zero_traits::{AsyncSocket, TransportBypassControl};

#[tokio::test]
async fn deferred_socket_boundary_retains_original_application_and_bypass_state() {
    for relay in [false, true] {
        let (io, mut peer) = tokio::io::duplex(128);
        let control = TransportBypassControl::default();
        let settings = ApplicationSettings {
            protocol: b"original".to_vec(),
            peer: b"negotiated".to_vec(),
        };
        let original = TcpRelayStream::with_transport_bypass_control(io, control.clone())
            .with_application_settings(Some(settings.clone()));
        let established = EstablishedTcpOutbound::proxied_compatibility(
            "node".into(),
            "host".into(),
            443,
            original,
        );
        let established = if relay {
            established
                .with_relay_route("last".into(), None, vec![])
                .unwrap()
        } else {
            established
        };
        let mut same = established.into_compatibility_stream().unwrap();
        assert_eq!(same.application_settings(), Some(&settings));
        let retained = AsyncSocket::transport_bypass_control(&same).unwrap();
        retained.request_read_bypass();
        retained.request_write_bypass();
        assert!(control.read_bypass_requested() && control.write_bypass_requested());
        let ((), ()) = tokio::join!(
            async {
                AsyncWriteExt::write_all(&mut same, b"original")
                    .await
                    .unwrap();
                AsyncWriteExt::shutdown(&mut same).await.unwrap();
                let mut reply = Vec::new();
                same.read_to_end(&mut reply).await.unwrap();
                assert_eq!(reply, b"after EOF");
            },
            async {
                let mut request = Vec::new();
                peer.read_to_end(&mut request).await.unwrap();
                assert_eq!(request, b"original");
                peer.write_all(b"after EOF").await.unwrap();
                peer.shutdown().await.unwrap();
            }
        );
    }
}

#[test]
fn standard_prefix_keeps_actual_endpoint_facts_only_at_the_deferred_boundary() {
    let (io, _peer) = tokio::io::duplex(128);
    let local = "127.0.0.1:31301".parse().unwrap();
    let remote = "127.0.0.1:31302".parse().unwrap();
    let same = EstablishedTcpOutbound::proxied("native", "host", 443, Box::pin(io) as BoxStream)
        .with_endpoints(Some(local), Some(remote))
        .into_compatibility_stream()
        .unwrap();
    assert_eq!(same.local_addr().unwrap(), local);
    assert_eq!(same.peer_addr().unwrap(), remote);
}
