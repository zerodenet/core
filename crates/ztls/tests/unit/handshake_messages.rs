use super::find_handshake_message;

fn handshake_message(message_type: u8, body: &[u8]) -> Vec<u8> {
    let mut message = vec![message_type];
    message.extend_from_slice(&(body.len() as u32).to_be_bytes()[1..]);
    message.extend_from_slice(body);
    message
}

#[test]
fn incomplete_message_is_found_after_the_remaining_fragment_arrives() {
    let encrypted_extensions = handshake_message(8, &[0, 0]);
    let certificate = handshake_message(11, &[0; 32]);
    let split = certificate.len() / 2;

    let mut accumulated = encrypted_extensions.clone();
    accumulated.extend_from_slice(&certificate[..split]);
    assert_eq!(find_handshake_message(&accumulated, 11), None);

    accumulated.extend_from_slice(&certificate[split..]);
    assert_eq!(
        find_handshake_message(&accumulated, 11),
        Some(encrypted_extensions.len())
    );
}

#[test]
fn extra_messages_do_not_hide_required_handshake_messages() {
    let mut accumulated = handshake_message(8, &[]);
    accumulated.extend_from_slice(&handshake_message(13, &[0]));
    let certificate_offset = accumulated.len();
    accumulated.extend_from_slice(&handshake_message(11, &[0; 8]));
    accumulated.extend_from_slice(&handshake_message(15, &[0; 8]));
    let finished_offset = accumulated.len();
    accumulated.extend_from_slice(&handshake_message(20, &[0; 32]));

    assert_eq!(
        find_handshake_message(&accumulated, 11),
        Some(certificate_offset)
    );
    assert_eq!(
        find_handshake_message(&accumulated, 20),
        Some(finished_offset)
    );
}
