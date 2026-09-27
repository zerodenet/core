// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
//   Copyright (c) Mullvad VPN AB. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0

//! ChaCha20-Poly1305 AEAD backend. Zero's opt-in `zeroized-rustcrypto`
//! backend stores the session key in an AEAD type that erases it on drop.
//!
//! `zeroized-rustcrypto` takes precedence over `aws-lc-rs`, which takes
//! precedence over `ring`. Disable default features to avoid compiling an
//! unused backend.

#[cfg(not(any(
    feature = "ring",
    feature = "aws-lc-rs",
    feature = "zeroized-rustcrypto"
)))]
compile_error!("gotatun requires an AEAD backend feature");

#[cfg(feature = "zeroized-rustcrypto")]
pub mod error {
    #[derive(Debug)]
    pub struct Unspecified;
}

#[cfg(feature = "zeroized-rustcrypto")]
pub mod aead {
    use chacha20poly1305::{
        ChaCha20Poly1305, Nonce as CryptoNonce, Tag,
        aead::{AeadInPlace, KeyInit},
    };

    use super::error::Unspecified;

    pub struct Algorithm;
    pub const CHACHA20_POLY1305: Algorithm = Algorithm;

    pub struct UnboundKey(ChaCha20Poly1305);

    impl UnboundKey {
        pub fn new(_: &Algorithm, key: &[u8]) -> Result<Self, Unspecified> {
            ChaCha20Poly1305::new_from_slice(key)
                .map(Self)
                .map_err(|_| Unspecified)
        }
    }

    pub struct LessSafeKey(UnboundKey);

    impl LessSafeKey {
        pub fn new(key: UnboundKey) -> Self {
            Self(key)
        }

        pub fn seal_in_place_separate_tag(
            &self,
            nonce: Nonce,
            aad: Aad<'_>,
            data: &mut [u8],
        ) -> Result<Tag, Unspecified> {
            self.0
                .0
                .encrypt_in_place_detached(CryptoNonce::from_slice(&nonce.0), aad.0, data)
                .map_err(|_| Unspecified)
        }

        pub fn open_in_place<'a>(
            &self,
            nonce: Nonce,
            aad: Aad<'_>,
            data_and_tag: &'a mut [u8],
        ) -> Result<&'a mut [u8], Unspecified> {
            let ciphertext_len = data_and_tag.len().checked_sub(16).ok_or(Unspecified)?;
            let (data, tag) = data_and_tag.split_at_mut(ciphertext_len);
            self.0
                .0
                .decrypt_in_place_detached(
                    CryptoNonce::from_slice(&nonce.0),
                    aad.0,
                    data,
                    Tag::from_slice(tag),
                )
                .map_err(|_| Unspecified)?;
            Ok(data)
        }
    }

    pub struct Nonce([u8; 12]);

    impl Nonce {
        pub fn assume_unique_for_key(bytes: [u8; 12]) -> Self {
            Self(bytes)
        }
    }

    pub struct Aad<'a>(&'a [u8]);

    impl<'a> From<&'a [u8]> for Aad<'a> {
        fn from(bytes: &'a [u8]) -> Self {
            Self(bytes)
        }
    }

    impl<'a, const N: usize> From<&'a [u8; N]> for Aad<'a> {
        fn from(bytes: &'a [u8; N]) -> Self {
            Self(bytes)
        }
    }
}

#[cfg(all(feature = "aws-lc-rs", not(feature = "zeroized-rustcrypto")))]
pub use aws_lc_rs::{aead, error};

#[cfg(all(
    feature = "ring",
    not(any(feature = "aws-lc-rs", feature = "zeroized-rustcrypto"))
))]
pub use ring::{aead, error};

#[cfg(all(test, feature = "zeroized-rustcrypto"))]
#[path = "crypto/tests.rs"]
mod tests;
