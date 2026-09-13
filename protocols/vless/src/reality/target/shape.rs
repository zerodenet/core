// SPDX-License-Identifier: MPL-2.0
// REALITY behavior follows XTLS/REALITY 9234c772ba8f, as pinned by Xray-core v26.3.27.
//! The target's plaintext ServerHello and encrypted record layout.
use super::super::hello::{extensions, invalid, shares};
use std::io;
#[derive(Debug, Clone)]
pub(crate) struct Shape {
    pub hello: Vec<u8>,
    pub lengths: Vec<usize>,
    post_handshake_lengths: Vec<usize>,
    max_ccs_records: usize,
}
impl Shape {
    pub fn inspect(bytes: &[u8]) -> io::Result<Option<Self>> {
        let mut offset = 0;
        let mut hello = Vec::new();
        let mut lengths = Vec::new();
        loop {
            if bytes.len() < offset + 5 {
                return Ok(None);
            }
            let header = &bytes[offset..offset + 5];
            let size = 5 + u16::from_be_bytes([header[3], header[4]]) as usize;
            if header[1..3] != [3, 3] || size > 8192 {
                return Err(invalid());
            }
            match lengths.len() {
                0 => {
                    if header[0] != 22 {
                        return Err(invalid());
                    }
                    if bytes.len() < offset + size {
                        return Ok(None);
                    }
                    let record = &bytes[offset..offset + size];
                    if record.get(5) != Some(&2) || record.get(9..11) != Some(&[3, 3]) {
                        return Err(invalid());
                    }
                    let ext = extensions(record, false)?;
                    if !ext
                        .iter()
                        .any(|(kind, data)| *kind == 43 && *data == [3, 4])
                    {
                        return Err(invalid());
                    }
                    let shares = shares(record, false)?;
                    if shares.len() != 1
                        || !matches!((shares[0].0, shares[0].1.len()), (29, 32) | (4588, 1120))
                    {
                        return Err(invalid());
                    }
                    if ztls::cipher::CipherSuite::from_id(ztls::util::extract_server_cipher_suite(
                        record,
                    )?)
                    .is_none()
                    {
                        return Err(invalid());
                    }
                    hello = record.to_vec();
                }
                1 => {
                    if header[0] != 20 || size != 6 {
                        return Err(invalid());
                    }
                    if bytes.len() < offset + size {
                        return Ok(None);
                    }
                    if bytes[offset + 5] != 1 {
                        return Err(invalid());
                    }
                }
                _ => {
                    if header[0] != 23 || size < 22 {
                        return Err(invalid());
                    }
                    // Official packed mode starts when the first encrypted record exceeds 512 bytes.
                    if lengths.len() == 2 && size > 512 {
                        lengths.push(size);
                        return Ok(Some(Self {
                            hello,
                            lengths,
                            post_handshake_lengths: Vec::new(),
                            max_ccs_records: usize::MAX,
                        }));
                    }
                }
            }
            if bytes.len() < offset + size {
                return Ok(None);
            }
            lengths.push(size);
            offset += size;
            if lengths.len() == 6 {
                return Ok(Some(Self {
                    hello,
                    lengths,
                    post_handshake_lengths: Vec::new(),
                    max_ccs_records: usize::MAX,
                }));
            }
        }
    }
    pub(in crate::reality) fn with_detection(
        mut self,
        post_handshake_lengths: Vec<usize>,
        max_ccs_records: usize,
    ) -> Self {
        self.post_handshake_lengths = post_handshake_lengths;
        self.max_ccs_records = max_ccs_records;
        self
    }
    pub(in crate::reality) fn max_ccs_records(&self) -> usize {
        self.max_ccs_records
    }
    pub fn suite(&self) -> io::Result<ztls::cipher::CipherSuite> {
        ztls::cipher::CipherSuite::from_id(ztls::util::extract_server_cipher_suite(&self.hello)?)
            .ok_or_else(invalid)
    }
    pub fn exchange(&self, client: &[u8]) -> io::Result<(Vec<u8>, Vec<u8>)> {
        use x25519_dalek::{PublicKey, StaticSecret};
        let (group, data) = shares(&self.hello, false)?
            .into_iter()
            .next()
            .ok_or_else(invalid)?;
        let peer = shares(client, true)?
            .into_iter()
            .find(|(g, _)| *g == group)
            .ok_or_else(invalid)?
            .1;
        let expected = if group == 4588 { 1216 } else { 32 };
        if peer.len() != expected {
            return Err(invalid());
        }
        let x25519: [u8; 32] = peer[peer.len() - 32..].try_into().unwrap();
        let private = crate::mlkem::random::<32>();
        let public = PublicKey::from(&StaticSecret::from(private));
        let shared = super::super::reality_auth::perform_ecdh(&private, &x25519)?;
        let (mut wire, mut secret) = if group == 4588 {
            let (ciphertext, key) = crate::mlkem::encapsulate(&peer[..1184])?;
            (ciphertext, key.to_vec())
        } else {
            (Vec::new(), Vec::new())
        };
        wire.extend_from_slice(public.as_bytes());
        secret.extend_from_slice(&shared);
        let position = data.as_ptr() as usize - self.hello.as_ptr() as usize;
        let mut hello = self.hello[5..].to_vec();
        hello[position - 5..position - 5 + wire.len()].copy_from_slice(&wire);
        Ok((hello, secret))
    }
    pub fn encrypt(
        &self,
        encryptor: &mut ztls::record::RecordEncryptor<'_>,
        messages: [&[u8]; 4],
        out: &mut Vec<u8>,
    ) -> io::Result<()> {
        out.extend_from_slice(&[20, 3, 3, 0, 1, 1]);
        if self.lengths.len() == 3 {
            let plaintext = messages.concat();
            if plaintext.len() + 22 > self.lengths[2] {
                return Err(io::Error::other(
                    "REALITY target encrypted flight is too small",
                ));
            }
            encryptor.encrypt_handshake_with_padding(&plaintext, out, self.lengths[2])
        } else {
            for (index, message) in messages.into_iter().enumerate() {
                let length = self.lengths[index + 2];
                if message.len() + 22 > length {
                    return Err(io::Error::other(
                        "REALITY target handshake record is too small",
                    ));
                }
                encryptor.encrypt_handshake_with_padding(message, out, length)?;
            }
            Ok(())
        }
    }

    pub(in crate::reality) fn encrypt_post_handshake(
        &self,
        encryptor: &mut ztls::record::RecordEncryptor<'_>,
        out: &mut Vec<u8>,
    ) -> io::Result<()> {
        for &length in &self.post_handshake_lengths {
            encryptor.encrypt_app_data_with_padding(&[], out, length)?;
        }
        Ok(())
    }
}
