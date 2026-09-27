// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use super::{HandshakeState, NoiseParams};
use crate::x25519::{PublicKey, StaticSecret};
use zeroize::Zeroizing;

#[test]
fn handshake_debug_redacts_secret_fields() {
    let private = StaticSecret::from([3; 32]);
    let public = PublicKey::from(&private);
    let peer = PublicKey::from(&StaticSecret::from([4; 32]));
    let params = NoiseParams::new(private, public, peer, Some([42; 32]));
    let debug = format!("{params:?}");
    assert!(debug.contains("preshared_key: \"<redacted>\""));
    assert!(debug.contains("sending_mac1_key: \"<redacted>\""));
    assert!(!debug.contains("[42, 42, 42"));

    let state = HandshakeState::InitReceived {
        hash: [1; 32],
        chaining_key: Zeroizing::new([2; 32]),
        peer_ephemeral_public: peer,
        peer_index: 1,
    };
    assert_eq!(format!("{state:?}"), "InitReceived(<redacted>)");
}
