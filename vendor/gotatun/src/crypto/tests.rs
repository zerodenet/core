// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::aead::{Aad, CHACHA20_POLY1305, LessSafeKey, Nonce, UnboundKey};
use chacha20poly1305::ChaCha20Poly1305;
use zeroize::ZeroizeOnDrop;

#[test]
fn session_aead_owns_a_key_that_zeroizes_on_drop() {
    fn assert_zeroize_on_drop<T: ZeroizeOnDrop>() {}
    assert_zeroize_on_drop::<ChaCha20Poly1305>();

    let key = LessSafeKey::new(UnboundKey::new(&CHACHA20_POLY1305, &[7; 32]).unwrap());
    let mut payload = b"session payload".to_vec();
    let tag = key
        .seal_in_place_separate_tag(
            Nonce::assume_unique_for_key([1; 12]),
            Aad::from(b"aad"),
            &mut payload,
        )
        .unwrap();
    payload.extend_from_slice(tag.as_ref());

    let decrypted = key
        .open_in_place(
            Nonce::assume_unique_for_key([1; 12]),
            Aad::from(b"aad"),
            &mut payload,
        )
        .unwrap();
    assert_eq!(decrypted, b"session payload");
}

#[test]
fn session_aead_rejects_modified_tag() {
    let key = LessSafeKey::new(UnboundKey::new(&CHACHA20_POLY1305, &[7; 32]).unwrap());
    let mut payload = b"payload".to_vec();
    let tag = key
        .seal_in_place_separate_tag(
            Nonce::assume_unique_for_key([2; 12]),
            Aad::from(b"aad"),
            &mut payload,
        )
        .unwrap();
    payload.extend_from_slice(tag.as_ref());
    *payload.last_mut().unwrap() ^= 1;

    assert!(
        key.open_in_place(
            Nonce::assume_unique_for_key([2; 12]),
            Aad::from(b"aad"),
            &mut payload
        )
        .is_err()
    );
}
