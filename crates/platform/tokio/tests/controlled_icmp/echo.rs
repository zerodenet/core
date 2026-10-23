use super::*;
use std::net::{IpAddr, Ipv6Addr};
async fn echo(view: &mut ControlledIcmpSocket, target: IpAddr, sequence: u16) {
    let local = view.connect(target).await.unwrap();
    let mut message = vec![if target.is_ipv4() { 8 } else { 128 }, 0, 0, 0, 0x45, 0x67];
    message.extend_from_slice(&sequence.to_be_bytes());
    message.extend_from_slice(b"controlled-icmp-loopback");
    let sum = if let (IpAddr::V6(local), IpAddr::V6(target)) = (local, target) {
        let mut pseudo = Vec::new();
        pseudo.extend_from_slice(&local.octets());
        pseudo.extend_from_slice(&target.octets());
        pseudo.extend_from_slice(&(message.len() as u32).to_be_bytes());
        pseudo.extend_from_slice(&[0, 0, 0, 58]);
        pseudo.extend_from_slice(&message);
        checksum(&pseudo)
    } else {
        checksum(&message)
    };
    message[2..4].copy_from_slice(&sum.to_be_bytes());
    assert_eq!(view.send(&message).await.unwrap(), message.len());
    let identifier = view.reply_identifier(0x4567).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let mut bytes = [0; 1500];
        loop {
            let (size, source) = view.recv_from(&mut bytes).await.unwrap();
            if size == message.len()
                && source == target
                && bytes[0] == if target.is_ipv4() { 0 } else { 129 }
                && bytes[4..6] == identifier.to_be_bytes()
                && bytes[6..].get(..message.len() - 6) == Some(&message[6..])
            {
                break;
            }
        }
    })
    .await
    .unwrap();
}
fn checksum(bytes: &[u8]) -> u16 {
    let mut sum = 0u32;
    for chunk in bytes.chunks(2) {
        sum += u32::from(u16::from_be_bytes([chunk[0], *chunk.get(1).unwrap_or(&0)]));
    }
    while sum >> 16 != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}
#[tokio::test]
#[ignore = "actual native loopback ICMP access required"]
async fn sixty_five_lifetime_probes_reuse_capacity_only_after_actual_close() {
    let _serial = LIVE.lock().await;
    let mut owner = owner(1);
    for sequence in 0..65 {
        let mut view = owner.bind(local(), None).unwrap();
        assert_eq!(owner.occupied(), 1);
        assert!(owner.bind(local(), None).is_err());
        echo(&mut view, Ipv4Addr::LOCALHOST.into(), sequence).await;
        assert!(owner.close(&view).unwrap().matches(&view));
        assert_eq!(owner.take_report().confirmed_closed, 1);
        assert_eq!(owner.occupied(), 0);
    }
}
#[tokio::test]
#[ignore = "actual native IPv6 loopback ICMP access required"]
async fn actual_ipv6_echo_preserves_native_family_and_reply_identifier() {
    let _serial = LIVE.lock().await;
    let mut owner = owner(1);
    let mut view = owner.bind(Ipv6Addr::UNSPECIFIED.into(), None).unwrap();
    assert!(view.connect(Ipv4Addr::LOCALHOST.into()).await.is_err());
    echo(&mut view, Ipv6Addr::LOCALHOST.into(), 1).await;
    owner.close(&view).unwrap();
    assert_eq!(owner.take_report().confirmed_closed, 1);
}
#[tokio::test]
#[ignore = "run only in a child without RAW capability and with Linux ping socket permission"]
async fn actual_raw_permission_denial_falls_back_to_real_icmp_datagram() {
    let _serial = LIVE.lock().await;
    let mut owner = owner(1);
    let mut view = owner.bind(local(), None).unwrap();
    assert_eq!(view.mode(), IcmpMode::PingDatagram);
    let original = view
        .raw_permission_source()
        .expect("actual RAW permission failure");
    assert_eq!(original.kind(), std_io::ErrorKind::PermissionDenied);
    assert!(original.raw_os_error().is_some());
    echo(&mut view, Ipv4Addr::LOCALHOST.into(), 1).await;
    owner.close(&view).unwrap();
    let report = owner.take_report();
    assert_eq!(report.confirmed_closed, 1);
    assert!(
        report.first_acquisition_failure.is_none(),
        "successful allowed fallback is not a terminal acquisition failure"
    );
}
