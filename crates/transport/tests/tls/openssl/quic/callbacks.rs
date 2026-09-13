use super::{complete_message_len, crypto_recv, crypto_release, CallbackState, MAX_CRYPTO_BUFFER};

#[test]
fn waits_for_a_complete_tls_handshake_message() {
    assert_eq!(complete_message_len(&[1, 0, 0]), None);
    assert_eq!(complete_message_len(&[1, 0, 0, 2, 7]), None);
    assert_eq!(complete_message_len(&[1, 0, 0, 2, 7, 8]), Some(6));
}

#[test]
fn delivered_record_stays_valid_until_release_despite_more_crypto_input() {
    let mut state = CallbackState::default();
    let message = [1, 0, 0, 2, 7, 8];
    state.push_input(&message).unwrap();
    let mut pointer = std::ptr::null();
    let mut length = 0;
    let arg = (&mut state as *mut CallbackState).cast();
    unsafe {
        assert_eq!(
            crypto_recv(std::ptr::null_mut(), &mut pointer, &mut length, arg),
            1
        );
    }
    state
        .push_input(&vec![9; MAX_CRYPTO_BUFFER - message.len()])
        .unwrap();
    assert!(state.push_input(&[9]).is_err());
    unsafe {
        assert_eq!(std::slice::from_raw_parts(pointer, length), message);
        assert_eq!(crypto_release(std::ptr::null_mut(), length, arg), 1);
    }
    assert!(state.active_read.is_none());
    state.push_input(&message).unwrap();
}
