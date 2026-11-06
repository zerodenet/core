use super::*;
fn declaration(datagram: usize) -> ResourceDeclaration {
    ResourceDeclaration::PacketDatagramReads {
        datagram,
        workspaces: [NonZeroUsize::new(65_536).unwrap(); 2],
    }
}
#[test]
fn cold_read_descriptions_reject_wrong_descriptor_and_duplicate_same_original() {
    assert!(packet_datagram_read_spec(&[declaration(1)], 0).is_err());
    let listener = ResourceDeclaration::DatagramListener {
        accepted: None,
        destination: Destination::new(zero_core::Address::Ipv4([127, 0, 0, 1]), 0),
        max_bind_attempts: NonZeroUsize::new(1).unwrap(),
        max_pending_sends: NonZeroUsize::new(1).unwrap(),
    };
    assert!(
        packet_datagram_read_spec(&[listener.clone(), declaration(0), declaration(0)], 0).is_err()
    );
    assert!(packet_datagram_read_spec(&[listener.clone(), declaration(1)], 0).is_err());
    assert_eq!(
        packet_datagram_read_spec(&[listener, declaration(0)], 0).unwrap(),
        Some([NonZeroUsize::new(65_536).unwrap(); 2])
    );
}
