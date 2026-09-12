use super::*;
#[cfg(feature = "crypto")]
pub fn encrypt_tcp_2022_single_chunk(
    cipher: CipherKind,
    key: &[u8],
    nonce_counter: &mut u128,
    plaintext: &[u8],
) -> Result<Vec<u8>, Error> {
    check_tcp_nonce(*nonce_counter)?;
    let nonce = tcp_nonce(*nonce_counter);
    *nonce_counter = nonce_counter
        .checked_add(1)
        .ok_or(Error::Protocol("ss: TCP nonce exhausted"))?;
    aead_encrypt(cipher, key, &nonce, plaintext)
}

/// Decrypt a single 2022 AEAD chunk (one nonce increment). Inverse of
/// [`encrypt_tcp_2022_single_chunk`].
#[cfg(feature = "crypto")]
pub fn decrypt_tcp_2022_single_chunk(
    cipher: CipherKind,
    key: &[u8],
    nonce_counter: &mut u128,
    ciphertext: &[u8],
) -> Result<Vec<u8>, Error> {
    check_tcp_nonce(*nonce_counter)?;
    let nonce = tcp_nonce(*nonce_counter);
    *nonce_counter = nonce_counter
        .checked_add(1)
        .ok_or(Error::Protocol("ss: TCP nonce exhausted"))?;
    aead_decrypt(cipher, key, &nonce, ciphertext)
}

#[cfg(feature = "crypto")]
fn check_tcp_nonce(counter: u128) -> Result<(), Error> {
    if counter >= (1u128 << 96) {
        Err(Error::Protocol("ss: TCP nonce exhausted"))
    } else {
        Ok(())
    }
}

pub fn tcp_nonce(counter: u128) -> [u8; 12] {
    let mut nonce = [0u8; 12];
    nonce.copy_from_slice(&counter.to_le_bytes()[..12]);
    nonce
}

#[cfg(feature = "crypto")]
pub fn encrypt_tcp_chunk(
    cipher: CipherKind,
    key: &[u8],
    nonce_counter: &mut u128,
    payload: &[u8],
) -> Result<Vec<u8>, Error> {
    if payload.len() > max_tcp_payload_len(cipher) {
        return Err(Error::Protocol("ss: tcp chunk too large"));
    }

    check_tcp_nonce(*nonce_counter)?;
    let length_nonce = tcp_nonce(*nonce_counter);
    *nonce_counter = nonce_counter
        .checked_add(1)
        .ok_or(Error::Protocol("ss: TCP nonce exhausted"))?;
    let encrypted_length = aead_encrypt(
        cipher,
        key,
        &length_nonce,
        &(payload.len() as u16).to_be_bytes(),
    )?;

    check_tcp_nonce(*nonce_counter)?;
    let payload_nonce = tcp_nonce(*nonce_counter);
    *nonce_counter = nonce_counter
        .checked_add(1)
        .ok_or(Error::Protocol("ss: TCP nonce exhausted"))?;
    let encrypted_payload = aead_encrypt(cipher, key, &payload_nonce, payload)?;

    let mut chunk = Vec::with_capacity(encrypted_length.len() + encrypted_payload.len());
    chunk.extend_from_slice(&encrypted_length);
    chunk.extend_from_slice(&encrypted_payload);
    Ok(chunk)
}

#[cfg(feature = "crypto")]
pub fn decrypt_tcp_chunk_length(
    cipher: CipherKind,
    key: &[u8],
    nonce_counter: &mut u128,
    encrypted_length: &[u8],
) -> Result<usize, Error> {
    if encrypted_length.len() != TCP_CHUNK_SIZE_LEN + cipher.tag_len() {
        return Err(Error::Protocol("ss: invalid encrypted length size"));
    }

    check_tcp_nonce(*nonce_counter)?;
    let nonce = tcp_nonce(*nonce_counter);
    *nonce_counter = nonce_counter
        .checked_add(1)
        .ok_or(Error::Protocol("ss: TCP nonce exhausted"))?;
    let plain = aead_decrypt(cipher, key, &nonce, encrypted_length)?;
    if plain.len() != TCP_CHUNK_SIZE_LEN {
        return Err(Error::Protocol("ss: invalid decrypted length size"));
    }

    let payload_len = u16::from_be_bytes([plain[0], plain[1]]) as usize;
    if payload_len > max_tcp_payload_len(cipher) {
        return Err(Error::Protocol("ss: tcp chunk too large"));
    }
    Ok(payload_len)
}

/// Maximum payload length per chunk. Legacy AEAD caps at 0x3FFF; SIP022 2022
/// removes that cap and allows up to 0xFFFF (spec 3.1.2).
pub const fn max_tcp_payload_len(cipher: CipherKind) -> usize {
    if cipher.is_blake3() {
        0xFFFF
    } else {
        MAX_TCP_PAYLOAD_SIZE
    }
}

#[cfg(feature = "crypto")]
pub fn decrypt_tcp_chunk_payload(
    cipher: CipherKind,
    key: &[u8],
    nonce_counter: &mut u128,
    expected_len: usize,
    encrypted_payload: &[u8],
) -> Result<Vec<u8>, Error> {
    if encrypted_payload.len() != expected_len + cipher.tag_len() {
        return Err(Error::Protocol("ss: invalid encrypted payload size"));
    }

    check_tcp_nonce(*nonce_counter)?;
    let nonce = tcp_nonce(*nonce_counter);
    *nonce_counter = nonce_counter
        .checked_add(1)
        .ok_or(Error::Protocol("ss: TCP nonce exhausted"))?;
    let plain = aead_decrypt(cipher, key, &nonce, encrypted_payload)?;
    if plain.len() != expected_len {
        return Err(Error::Protocol("ss: invalid decrypted payload size"));
    }
    Ok(plain)
}

#[cfg(feature = "crypto")]
pub async fn read_tcp_chunk<S: AsyncSocket>(
    stream: &mut S,
    cipher: CipherKind,
    key: &[u8],
    nonce_counter: &mut u128,
) -> Result<Vec<u8>, Error> {
    let mut encrypted_length = vec![0u8; TCP_CHUNK_SIZE_LEN + cipher.tag_len()];
    read_exact(stream, &mut encrypted_length).await?;
    let payload_len = decrypt_tcp_chunk_length(cipher, key, nonce_counter, &encrypted_length)?;

    let mut encrypted_payload = vec![0u8; payload_len + cipher.tag_len()];
    read_exact(stream, &mut encrypted_payload).await?;
    decrypt_tcp_chunk_payload(cipher, key, nonce_counter, payload_len, &encrypted_payload)
}

/// Encrypt a TCP chunk and write it to the stream.
///
/// Wraps [`encrypt_tcp_chunk`] + [`AsyncSocket::write_all`]. Each call
/// consumes `payload.len()` plain bytes (up to [`MAX_TCP_PAYLOAD_SIZE`])
/// and writes the encrypted AEAD chunk (encrypted length + encrypted payload)
/// to the stream.
#[cfg(feature = "crypto")]
pub async fn write_tcp_chunk<S: AsyncSocket>(
    stream: &mut S,
    cipher: CipherKind,
    key: &[u8],
    nonce_counter: &mut u128,
    payload: &[u8],
) -> Result<(), Error> {
    let chunk = encrypt_tcp_chunk(cipher, key, nonce_counter, payload)?;
    stream
        .write_all(&chunk)
        .await
        .map_err(|_| Error::Io("ss: write failed"))
}
