//! A real fixture caller owns work/ACK; absence of Native authority stays real.
use super::*;
#[tokio::test]
async fn routing_echo_keeps_owned_memory_until_real_child_join_and_ack_without_native_authority() {
    let mut fixture = Fixture::new(direct());
    let mut request = vec![0_u8; 28];
    request[0] = 0x45;
    request[2..4].copy_from_slice(&28_u16.to_be_bytes());
    request[8] = 64;
    request[9] = packet::IPPROTO_ICMP;
    request[12..16].copy_from_slice(&[10, 66, 0, 2]);
    request[16..20].copy_from_slice(&[127, 0, 0, 1]);
    request[20] = 8;
    let checksum = packet::checksum(&request[20..]);
    request[22..24].copy_from_slice(&checksum.to_be_bytes());
    let checksum = packet::checksum(&request[..20]);
    request[10..12].copy_from_slice(&checksum.to_be_bytes());
    assert!(fixture.forward(&request, 1500, false).await);
    let receipt = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        fixture.scope.next_receipt(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(receipt.joined().result().is_ok());
    let mut children = fixture.echo_tasks.lock().await.unwrap();
    children.join_next().await.unwrap().unwrap();
    assert_eq!(
        children.len(),
        1,
        "body/join still owes its actual Host ACK"
    );
    assert!(!receipt.joined().registration_released());
    fixture.scope.acknowledge(&receipt).unwrap();
    assert_eq!(children.len(), 0);
    drop(children);
    let response = fixture.received.recv().await.unwrap();
    assert!(
        packet::parse_icmp_error(&response).is_some(),
        "no Native grant returns a real local error, never opens an ICMP socket"
    );
    drop(response);
    assert!(fixture.echo_tasks.quiescent());
    fixture.scope.begin_shutdown();
    assert!(fixture.scope.next_receipt().await.is_none());
}
