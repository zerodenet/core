use mieru::{crypto::MieruCipher, metadata::*, segment::*};

#[test]
fn fragmented_padding_preserves_nonce_and_next_frame() {
    for session in [false, true] {
        let mut send = MieruCipher::with_nonce(&[3; 32], [5; 24]);
        let mut wire = if session {
            let mut meta = SessionMetadata::new(OPEN_SESSION_REQUEST);
            meta.session_id = 1;
            meta.payload_length = 5;
            meta.suffix_length = 19;
            build_session_segment(&meta, b"hello", &mut send, true).unwrap()
        } else {
            let mut meta = DataMetadata::new(DATA_CLIENT_TO_SERVER);
            meta.session_id = 1;
            meta.payload_length = 5;
            meta.prefix_length = 11;
            meta.suffix_length = 19;
            build_data_segment(&meta, b"hello", &mut send, true).unwrap()
        };
        let first_len = wire.len();
        let mut recv = MieruCipher::with_nonce(&[3; 32], [0; 24]);
        for n in 0..first_len {
            let nonce = *recv.current_nonce();
            assert!(
                parse_segment(&wire[..n], &mut recv, true, session).is_err(),
                "accepted prefix {n}/{first_len}"
            );
            assert_eq!(*recv.current_nonce(), nonce);
        }
        let mut next = DataMetadata::new(DATA_CLIENT_TO_SERVER);
        next.payload_length = 4;
        wire.extend(build_data_segment(&next, b"next", &mut send, false).unwrap());
        let (frame, consumed) = parse_segment(&wire, &mut recv, true, session).unwrap();
        assert_eq!(frame.payload, b"hello");
        assert_eq!(consumed, first_len);
        let (frame, n) = parse_segment(&wire[consumed..], &mut recv, false, false).unwrap();
        assert_eq!(frame.payload, b"next");
        assert_eq!(consumed + n, wire.len());
    }
}
