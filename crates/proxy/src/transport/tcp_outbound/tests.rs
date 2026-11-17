//! Local wire representation regressions, not native-provider qualification.
use super::*;
use crate::transport::TcpRelayStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_platform_tokio::{contract::stream::BoxStream, ApplicationSettings};
use zero_traits::{AsyncSocket, TransportBypassControl};

#[tokio::test]
async fn standard_view_keeps_original_wire_half_close_and_transport_state() {
    for relay in [false, true] {
        let (io, mut peer) = tokio::io::duplex(128);
        let control = TransportBypassControl::default();
        let settings = ApplicationSettings {
            protocol: b"original".to_vec(),
            peer: b"negotiated".to_vec(),
        };
        let original = TcpRelayStream::with_transport_bypass_control(io, control.clone())
            .with_application_settings(Some(settings.clone()));
        assert_eq!(original.application_settings(), Some(&settings));
        let retained = AsyncSocket::transport_bypass_control(&original).unwrap();
        let io = Box::pin(original) as BoxStream;
        let established = if relay {
            EstablishedTcpOutbound::relay("last", None, vec![], io)
        } else {
            EstablishedTcpOutbound::proxied("node", "host", 443, io)
        };
        let mut same = established.into_stream().unwrap();
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
fn standard_outbound_retains_actual_endpoint_observations_without_rebuilding_a_socket_facade() {
    use super::model::{EstablishedStream, EstablishedTcpOutboundKind};
    let (io, _peer) = tokio::io::duplex(128);
    let local = "127.0.0.1:31301".parse().unwrap();
    let remote = "127.0.0.1:31302".parse().unwrap();
    let outbound =
        EstablishedTcpOutbound::proxied("native", "host", 443, Box::pin(io) as BoxStream)
            .with_endpoints(Some(local), Some(remote));
    let EstablishedTcpOutboundKind::Proxied {
        upstream: EstablishedStream { endpoints, .. },
        ..
    } = outbound.kind
    else {
        panic!("same standard result required")
    };
    assert_eq!(endpoints, (Some(local), Some(remote)));
}
