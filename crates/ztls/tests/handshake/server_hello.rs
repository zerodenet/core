use super::assemble;
fn record(payload: &[u8]) -> Vec<u8> {
    let mut record = vec![22, 3, 3];
    record.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    record.extend_from_slice(payload);
    record
}
#[test]
fn server_hello_reassembles_fragmented_header_and_body() {
    let message = [2, 0, 0, 4, 1, 2, 3, 4];
    for split in 1..message.len() {
        let mut pending = Vec::new();
        assert!(assemble(&mut pending, &record(&message[..split]))
            .unwrap()
            .is_none());
        assert_eq!(
            assemble(&mut pending, &record(&message[split..]))
                .unwrap()
                .unwrap(),
            record(&message)
        );
        assert!(pending.is_empty());
    }
}
#[test]
fn compatibility_ccs_requires_handshake_boundary() {
    let mut pending = Vec::new();
    assert!(assemble(&mut pending, &[20, 3, 3, 0, 1, 1])
        .unwrap()
        .is_none());
    assemble(&mut pending, &record(&[2, 0])).unwrap();
    assert!(assemble(&mut pending, &[20, 3, 3, 0, 1, 1]).is_err());
}
#[test]
fn reject_oversized_or_coalesced_server_hello() {
    assert!(assemble(&mut Vec::new(), &record(&[2, 1, 0, 0])).is_err());
    assert!(assemble(&mut Vec::new(), &record(&[2, 0, 0, 1, 0, 1])).is_err());
    assert!(assemble(&mut Vec::new(), &record(&vec![2; 16385])).is_err());
}
