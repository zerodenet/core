use super::*;
use std::{future::Future, num::NonZeroUsize, task::Poll};
use zero_platform_tokio::{controlled_datagram::AcquisitionOutcome, EgressInterfaceControl};

struct Codec;
impl DatagramCodec<Address> for Codec {
    type Error = zero_core::Error;
    fn encode(&self, target: &Address, port: u16, payload: &[u8]) -> Result<Vec<u8>, Self::Error> {
        let Address::Ipv4(address) = target else {
            panic!("fixture uses IPv4 address frames")
        };
        let mut wire = address.to_vec();
        wire.extend_from_slice(&port.to_be_bytes());
        wire.extend_from_slice(payload);
        Ok(wire)
    }
    fn decode(&self, wire: &[u8]) -> Option<(Address, u16, Vec<u8>)> {
        if wire.len() < 6 {
            return None;
        }
        Some((
            Address::Ipv4(wire[..4].try_into().ok()?),
            u16::from_be_bytes(wire[4..6].try_into().ok()?),
            wire[6..].to_vec(),
        ))
    }
}
fn owner() -> ControlledDatagramOwner {
    ControlledDatagramOwner::new(NonZeroUsize::new(1).unwrap())
}

#[tokio::test]
async fn owned_packet_wire_preserves_actual_framing_zero_length_and_decoded_source_then_explicit_original_close(
) {
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let egress = EgressInterfaceControl::default();
    let factory = crate::OutboundDatagramSocketFactory::new(egress.clone());
    let mut owner = owner();
    let wire = OwnedUdpSocketPacketPath::establish(
        peer.local_addr().unwrap(),
        Arc::new(Codec),
        &factory,
        &mut owner,
        &Default::default(),
        egress.generation(),
    )
    .unwrap();
    assert!(owner.retains(wire.reservation()));
    let target = Address::Ipv4([192, 0, 2, 2]);
    wire.send_to(&target, 443, b"").await.unwrap();
    let mut bytes = [0; 64];
    let (length, sender) = peer.recv_from(&mut bytes).await.unwrap();
    assert_eq!(&bytes[..length], &[192, 0, 2, 2, 1, 187]);
    assert_eq!(sender.port(), wire.local_addr().unwrap().port());
    assert!(sender.ip().is_loopback());
    let mut output = [0; 64];
    let mut receiving = Box::pin(wire.recv_from_with_source(&mut output));
    std::future::poll_fn(|cx| {
        assert!(receiving.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    peer.send_to(
        &Codec
            .encode(&Address::Ipv4([203, 0, 113, 2]), 53, b"reply")
            .unwrap(),
        sender,
    )
    .await
    .unwrap();
    let (size, source, port) = receiving.await.unwrap();
    assert_eq!(
        (&output[..size], source, port),
        (&b"reply"[..], Address::Ipv4([203, 0, 113, 2]), 53)
    );
    let report = owner.close_all();
    assert!(report.is_complete());
    assert_eq!(report.confirmed_closed, 1);
    assert_eq!(owner.occupied(), 0);
    assert!(wire.send_to(&target, 443, b"closed").await.is_err());
}

#[tokio::test]
async fn dropping_the_returned_wire_retains_the_original_reservation_until_the_actual_owner_closes_it(
) {
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let egress = EgressInterfaceControl::default();
    let factory = crate::OutboundDatagramSocketFactory::new(egress.clone());
    let mut owner = owner();
    let wire = OwnedUdpSocketPacketPath::establish(
        peer.local_addr().unwrap(),
        Arc::new(Codec),
        &factory,
        &mut owner,
        &Default::default(),
        egress.generation(),
    )
    .unwrap();
    let original = wire.reservation();
    drop(wire);
    assert!(owner.retains(original));
    assert_eq!(owner.occupied(), 1);
    assert!(OwnedUdpSocketPacketPath::establish(
        peer.local_addr().unwrap(),
        Arc::new(Codec),
        &factory,
        &mut owner,
        &Default::default(),
        egress.generation()
    )
    .is_err());
    let report = owner.close_all();
    assert_eq!(report.confirmed_closed, 1);
    assert!(report.is_complete());
}

#[tokio::test]
async fn strict_family_and_actual_stale_egress_reject_before_creating_an_original_socket() {
    let egress = EgressInterfaceControl::default();
    let factory = crate::OutboundDatagramSocketFactory::new(egress.clone());
    let mut owner = owner();
    let policy = zero_traits::DialPolicy {
        address_family: zero_traits::AddressFamily::OnlyIpv4,
        ..Default::default()
    };
    let error = factory
        .bind_owned(
            &mut owner,
            "[::1]:53".parse().unwrap(),
            &policy,
            egress.generation(),
        )
        .unwrap_err();
    assert!(matches!(error.outcome, AcquisitionOutcome::KnownNoNative));
    assert!(owner.take_report().is_complete());
    assert_eq!(owner.occupied(), 0);
    let actual_before = egress.generation();
    egress.mark_unavailable_for(false, "actual-host-control-change");
    let error = factory
        .bind_owned(
            &mut owner,
            "127.0.0.1:53".parse().unwrap(),
            &Default::default(),
            actual_before,
        )
        .unwrap_err();
    assert!(matches!(error.outcome, AcquisitionOutcome::KnownNoNative));
    assert_eq!(owner.occupied(), 0);
}

#[test]
fn owned_plain_wire_cannot_drop_required_udp_transforms() {
    let egress = EgressInterfaceControl::default();
    let factory = crate::OutboundDatagramSocketFactory::new(egress.clone()).with_final_mask(
        crate::finalmask::Profile::from_udp(vec![crate::finalmask::udp::Mask::Dtls]).unwrap(),
    );
    let mut owner = owner();
    let error = factory
        .bind_owned(
            &mut owner,
            "127.0.0.1:53".parse().unwrap(),
            &Default::default(),
            egress.generation(),
        )
        .unwrap_err();
    assert_eq!(error.source.kind(), std::io::ErrorKind::Unsupported);
    assert!(matches!(error.outcome, AcquisitionOutcome::KnownNoNative));
    assert_eq!(owner.occupied(), 0);
}
