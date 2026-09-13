// SPDX-License-Identifier: MPL-2.0
// VLESS Encryption wire behavior adapted from XTLS/Xray-core v26.3.27
// (d2758a023cd7f4174a5a5fa4ff66e487d4342ba0), proxy/vless/encryption.
use super::crypto::{mask, record_length, Ctr};
use std::io;

#[derive(Default)]
pub(super) struct HeaderMask {
    header: [u8; 5],
    filled: usize,
    skip: usize,
}
impl HeaderMask {
    pub fn apply(
        &mut self,
        cipher: &mut Option<Ctr>,
        mut bytes: &mut [u8],
        incoming: bool,
    ) -> io::Result<()> {
        if cipher.is_none() {
            return Ok(());
        }
        while !bytes.is_empty() {
            if self.skip > 0 {
                let n = self.skip.min(bytes.len());
                self.skip -= n;
                bytes = &mut bytes[n..];
                continue;
            }
            let n = (5 - self.filled).min(bytes.len());
            if incoming {
                mask(cipher, &mut bytes[..n]);
            }
            self.header[self.filled..self.filled + n].copy_from_slice(&bytes[..n]);
            if !incoming {
                mask(cipher, &mut bytes[..n]);
            }
            self.filled += n;
            bytes = &mut bytes[n..];
            if self.filled == 5 {
                self.skip = record_length(&self.header)?;
                self.filled = 0;
            }
        }
        Ok(())
    }
}
