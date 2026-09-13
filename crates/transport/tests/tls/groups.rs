use super::*;

#[test]
fn added_groups_agree_and_reject_malformed_peer_shares() {
    for (id, public_len, secret_len) in [(25, 133, 66), (4587, 1249, 64), (4589, 1665, 80)] {
        let group = lookup(id).unwrap();
        let client = group.start().unwrap();
        assert_eq!(client.pub_key().len(), public_len);
        let server = group.start_and_complete(client.pub_key()).unwrap();
        let secret = client.complete(&server.pub_key).unwrap();
        assert_eq!(secret.secret_bytes().len(), secret_len);
        assert_eq!(secret.secret_bytes(), server.secret.secret_bytes());
        assert!(group.start_and_complete(&vec![0; public_len]).is_err());
        assert!(group.start_and_complete(&vec![4; public_len - 1]).is_err());
        assert!(group.start().unwrap().complete(&[]).is_err());
    }
}

#[test]
fn p384_hybrid_can_negotiate_its_offered_classical_component() {
    let hybrid = P384MlKem1024.start().unwrap();
    let (id, share) = hybrid.hybrid_component().unwrap();
    assert_eq!(id, NamedGroup::secp384r1);
    let classical = lookup(24).unwrap().start_and_complete(share).unwrap();
    let secret = hybrid
        .complete_hybrid_component(&classical.pub_key)
        .unwrap();
    assert_eq!(secret.secret_bytes(), classical.secret.secret_bytes());
    assert!(!P384MlKem1024.usable_for_version(ProtocolVersion::TLSv1_2));
}
