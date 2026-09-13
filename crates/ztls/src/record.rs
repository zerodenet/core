// TLS 1.3 record layer encryption/decryption.
//
// Handles framing plaintext into TLS records, including:
// - Adding/stripping ContentType trailer byte
// - Fragmenting large data into multiple records (max 16KB plaintext each)
// - Building TLS record headers
// - Managing sequence numbers

use std::io::{self, Error};

use crate::aead::AeadKey;
use crate::common::{
    strip_content_type_slice, CONTENT_TYPE_ALERT, CONTENT_TYPE_APPLICATION_DATA,
    CONTENT_TYPE_HANDSHAKE, MAX_TLS_CIPHERTEXT_LEN, MAX_TLS_PLAINTEXT_LEN, TLS_RECORD_HEADER_SIZE,
};

/// Encrypts plaintext into TLS 1.3 records.
///
/// Manages the write-side sequence number and handles record framing.
pub struct RecordEncryptor<'a> {
    key: &'a AeadKey,
    iv: &'a [u8],
    seq: &'a mut u64,
}

impl<'a> RecordEncryptor<'a> {
    #[inline]
    pub fn new(key: &'a AeadKey, iv: &'a [u8], seq: &'a mut u64) -> Self {
        Self { key, iv, seq }
    }

    /// Encrypt application data into TLS 1.3 records.
    ///
    /// For data <= 16KB: encrypts in-place in the plaintext buffer (zero-copy).
    /// For data > 16KB: fragments into multiple records.
    ///
    /// Clears the plaintext buffer after encryption.
    #[inline]
    pub fn encrypt_app_data(
        &mut self,
        plaintext: &mut Vec<u8>,
        out: &mut Vec<u8>,
    ) -> io::Result<()> {
        if plaintext.is_empty() {
            return Ok(());
        }

        if plaintext.len() <= MAX_TLS_PLAINTEXT_LEN {
            // Fast path: single record, encrypt in-place
            self.encrypt_record_in_place(plaintext, out, CONTENT_TYPE_APPLICATION_DATA)?;
        } else {
            // Slow path: fragment into multiple records
            self.encrypt_fragmented(plaintext, out, CONTENT_TYPE_APPLICATION_DATA)?;
        }

        plaintext.clear();
        Ok(())
    }

    /// Encrypt one application-data record padded to an observed wire size.
    /// Empty application content is permitted so REALITY can reproduce a
    /// target's post-handshake record layout without exposing target bytes.
    pub fn encrypt_app_data_with_padding(
        &mut self,
        data: &[u8],
        out: &mut Vec<u8>,
        target_record_size: usize,
    ) -> io::Result<()> {
        if !(TLS_RECORD_HEADER_SIZE + 17..=TLS_RECORD_HEADER_SIZE + MAX_TLS_CIPHERTEXT_LEN)
            .contains(&target_record_size)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid padded TLS application record size",
            ));
        }
        let mut buffer = data.to_vec();
        self.encrypt_record_with_padding(
            &mut buffer,
            out,
            CONTENT_TYPE_APPLICATION_DATA,
            target_record_size,
        )
    }

    /// Encrypt handshake data into TLS 1.3 records.
    #[inline]
    pub fn encrypt_handshake(&mut self, data: &[u8], out: &mut Vec<u8>) -> io::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        if data.len() <= MAX_TLS_PLAINTEXT_LEN {
            let mut buf = data.to_vec();
            self.encrypt_record_in_place(&mut buf, out, CONTENT_TYPE_HANDSHAKE)?;
        } else {
            for chunk in data.chunks(MAX_TLS_PLAINTEXT_LEN) {
                let mut buf = chunk.to_vec();
                self.encrypt_record_in_place(&mut buf, out, CONTENT_TYPE_HANDSHAKE)?;
            }
        }

        Ok(())
    }

    /// Encrypt handshake data with padding to match a target record size.
    ///
    /// Uses TLS 1.3 inner padding (zeros after content type byte) to pad
    /// the encrypted record to match the target size from the destination server.
    ///
    /// If target_size is 0 or smaller than our minimum, no padding is added.
    #[inline]
    pub fn encrypt_handshake_with_padding(
        &mut self,
        data: &[u8],
        out: &mut Vec<u8>,
        target_record_size: usize,
    ) -> io::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        // For very large data, fragment without padding
        if data.len() > MAX_TLS_PLAINTEXT_LEN {
            // Fragment into multiple records - only pad the last one if needed
            let chunks: Vec<_> = data.chunks(MAX_TLS_PLAINTEXT_LEN).collect();
            for (i, chunk) in chunks.iter().enumerate() {
                let mut buf = chunk.to_vec();
                if i == chunks.len() - 1 && target_record_size > 0 {
                    // Last chunk - apply padding
                    self.encrypt_record_with_padding(
                        &mut buf,
                        out,
                        CONTENT_TYPE_HANDSHAKE,
                        target_record_size,
                    )?;
                } else {
                    self.encrypt_record_in_place(&mut buf, out, CONTENT_TYPE_HANDSHAKE)?;
                }
            }
            return Ok(());
        }

        let mut buf = data.to_vec();
        if target_record_size > 0 {
            self.encrypt_record_with_padding(
                &mut buf,
                out,
                CONTENT_TYPE_HANDSHAKE,
                target_record_size,
            )?;
        } else {
            self.encrypt_record_in_place(&mut buf, out, CONTENT_TYPE_HANDSHAKE)?;
        }

        Ok(())
    }

    /// Encrypt a close_notify alert.
    #[inline]
    pub fn encrypt_close_notify(&mut self, out: &mut Vec<u8>) -> io::Result<()> {
        let mut buf = vec![0x01, 0x00]; // level=warning, desc=close_notify
        self.encrypt_record_in_place(&mut buf, out, CONTENT_TYPE_ALERT)
    }

    /// Encrypt a single record in-place.
    ///
    /// 1. Appends content type byte to buffer
    /// 2. Encrypts in-place (appends 16-byte tag)
    /// 3. Writes TLS record header + ciphertext to output
    /// 4. Increments sequence number
    #[inline]
    fn encrypt_record_in_place(
        &mut self,
        buf: &mut Vec<u8>,
        out: &mut Vec<u8>,
        content_type: u8,
    ) -> io::Result<()> {
        // Append inner content type
        buf.push(content_type);

        // Build header (need ciphertext length = plaintext + content_type + tag)
        let ciphertext_len = buf.len() + 16;

        debug_assert!(
            ciphertext_len <= MAX_TLS_CIPHERTEXT_LEN,
            "BUG: ciphertext_len {} exceeds MAX_TLS_CIPHERTEXT_LEN {}",
            ciphertext_len,
            MAX_TLS_CIPHERTEXT_LEN
        );

        let header = make_record_header(ciphertext_len);

        // Encrypt in-place
        self.key.seal_in_place(buf, self.iv, *self.seq, &header)?;
        *self.seq = self
            .seq
            .checked_add(1)
            .ok_or_else(|| Error::other("TLS sequence number exhausted"))?;

        // Write to output
        out.reserve(TLS_RECORD_HEADER_SIZE + buf.len());
        out.extend_from_slice(&header);
        out.extend_from_slice(buf);

        Ok(())
    }

    /// Encrypt a single record with padding to match a target size.
    ///
    /// TLS 1.3 inner plaintext format: [content] [content_type] [zero_padding...]
    /// The padding is added after the content type byte and before encryption.
    #[inline]
    fn encrypt_record_with_padding(
        &mut self,
        buf: &mut Vec<u8>,
        out: &mut Vec<u8>,
        content_type: u8,
        target_record_size: usize,
    ) -> io::Result<()> {
        // Append inner content type
        buf.push(content_type);

        // Calculate padding needed to match target record size
        // target_record_size = header(5) + inner_plaintext_with_padding + tag(16)
        // So: inner_plaintext_with_padding = target_record_size - 5 - 16 = target_record_size - 21
        // Padding = inner_plaintext_with_padding - buf.len()
        let current_inner_len = buf.len();
        let target_inner_len = target_record_size.saturating_sub(TLS_RECORD_HEADER_SIZE + 16);

        if target_inner_len > current_inner_len && target_inner_len <= MAX_TLS_PLAINTEXT_LEN + 1 {
            let padding = target_inner_len - current_inner_len;
            buf.resize(buf.len() + padding, 0);
            tracing::trace!(
                "REALITY: Added {} bytes of TLS 1.3 inner padding (target={}, current={})",
                padding,
                target_record_size,
                TLS_RECORD_HEADER_SIZE + current_inner_len + 16
            );
        }

        // Build header with actual ciphertext length
        let ciphertext_len = buf.len() + 16;

        debug_assert!(
            ciphertext_len <= MAX_TLS_CIPHERTEXT_LEN,
            "BUG: ciphertext_len {} exceeds MAX_TLS_CIPHERTEXT_LEN {}",
            ciphertext_len,
            MAX_TLS_CIPHERTEXT_LEN
        );

        let header = make_record_header(ciphertext_len);

        // Encrypt in-place
        self.key.seal_in_place(buf, self.iv, *self.seq, &header)?;
        *self.seq = self
            .seq
            .checked_add(1)
            .ok_or_else(|| Error::other("TLS sequence number exhausted"))?;

        // Write to output
        out.reserve(TLS_RECORD_HEADER_SIZE + buf.len());
        out.extend_from_slice(&header);
        out.extend_from_slice(buf);

        Ok(())
    }

    /// Encrypt data larger than 16KB by fragmenting into multiple records.
    #[inline]
    fn encrypt_fragmented(
        &mut self,
        data: &[u8],
        out: &mut Vec<u8>,
        content_type: u8,
    ) -> io::Result<()> {
        for chunk in data.chunks(MAX_TLS_PLAINTEXT_LEN) {
            let mut buf = chunk.to_vec();
            self.encrypt_record_in_place(&mut buf, out, content_type)?;
        }
        Ok(())
    }
}

/// Decrypts TLS 1.3 records into plaintext.
///
/// Manages the read-side sequence number and handles content type extraction.
pub struct RecordDecryptor<'a> {
    key: &'a AeadKey,
    iv: &'a [u8],
    seq: &'a mut u64,
}

impl<'a> RecordDecryptor<'a> {
    #[inline]
    pub fn new(key: &'a AeadKey, iv: &'a [u8], seq: &'a mut u64) -> Self {
        Self { key, iv, seq }
    }

    /// Decrypt a TLS 1.3 record in-place, returning (content_type, plaintext_slice).
    ///
    /// Zero-allocation decryption: decrypts directly in the provided buffer
    /// and returns a slice to the plaintext within that buffer.
    ///
    /// # Arguments
    /// * `ciphertext` - Mutable slice containing ciphertext + auth tag (will be decrypted in-place)
    /// * `record_len` - Length from TLS record header (ciphertext + tag length)
    ///
    /// # Returns
    /// Tuple of (content_type, plaintext_slice) where plaintext_slice borrows from ciphertext
    #[inline]
    pub fn decrypt_record_in_place<'b>(
        &mut self,
        ciphertext: &'b mut [u8],
        record_len: u16,
    ) -> io::Result<(u8, &'b [u8])> {
        let aad = make_record_header(record_len as usize);

        let plaintext = self
            .key
            .open_in_place_slice(ciphertext, self.iv, *self.seq, &aad)?;
        *self.seq = self
            .seq
            .checked_add(1)
            .ok_or_else(|| Error::other("TLS sequence number exhausted"))?;

        // Strip content type (returns content_type and valid length)
        let (content_type, valid_len) = strip_content_type_slice(plaintext)?;

        Ok((content_type, &plaintext[..valid_len]))
    }
}

/// Build a TLS record header for the given ciphertext length.
#[inline]
fn make_record_header(ciphertext_len: usize) -> [u8; TLS_RECORD_HEADER_SIZE] {
    [
        CONTENT_TYPE_APPLICATION_DATA, // Outer type is always ApplicationData in TLS 1.3
        0x03,
        0x03, // TLS 1.2 version (for compatibility)
        (ciphertext_len >> 8) as u8,
        (ciphertext_len & 0xff) as u8,
    ]
}
