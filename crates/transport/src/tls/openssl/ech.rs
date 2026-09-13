use std::io;

use base64::Engine;
use foreign_types::ForeignType;
use openssl::{
    pkey::{Id, PKey},
    ssl::{SslContextBuilder, SslOptions},
};

use super::ffi;

pub(super) fn apply(builder: &mut SslContextBuilder, encoded: &str) -> io::Result<()> {
    if encoded.is_empty() {
        return Ok(());
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|_| invalid("ECH server keys must be standard base64"))?;
    let _ = builder.set_options(SslOptions::from_bits_retain(1_u64 << 38));
    let store = unsafe { ffi::OSSL_ECHSTORE_new(std::ptr::null_mut(), std::ptr::null()) };
    if store.is_null() {
        return Err(io::Error::other(openssl::error::ErrorStack::get()));
    }
    let result = (|| {
        let mut position = 0usize;
        while position < bytes.len() {
            let key_length = take_u16(&bytes, &mut position)?;
            let private = take(&bytes, &mut position, key_length)?;
            let config_length = take_u16(&bytes, &mut position)?;
            let config = take(&bytes, &mut position, config_length)?;
            if private.len() != 32 || config.get(5..7) != Some(&[0x00, 0x20]) {
                return Err(invalid("ECH server keys must contain X25519 key sets"));
            }
            let key =
                PKey::private_key_from_raw_bytes(private, Id::X25519).map_err(io::Error::other)?;
            let mut list = Vec::with_capacity(config.len() + 2);
            list.extend_from_slice(&(config.len() as u16).to_be_bytes());
            list.extend_from_slice(config);
            let pem = pem_ech_config(&list);
            let bio = unsafe {
                openssl_sys::BIO_new_mem_buf(pem.as_ptr().cast(), pem.len().try_into().unwrap())
            };
            if bio.is_null() {
                return Err(io::Error::other(openssl::error::ErrorStack::get()));
            }
            let loaded =
                unsafe { ffi::OSSL_ECHSTORE_set1_key_and_read_pem(store, key.as_ptr(), bio, 0) };
            unsafe { openssl_sys::BIO_free_all(bio) };
            if loaded != 1 {
                return Err(io::Error::other(openssl::error::ErrorStack::get()));
            }
        }
        let attached = unsafe { ffi::SSL_CTX_set1_echstore(builder.as_ptr(), store) };
        if attached != 1 {
            return Err(io::Error::other(openssl::error::ErrorStack::get()));
        }
        Ok(())
    })();
    unsafe { ffi::OSSL_ECHSTORE_free(store) };
    result
}

fn pem_ech_config(config: &[u8]) -> Vec<u8> {
    let encoded = base64::engine::general_purpose::STANDARD.encode(config);
    let mut pem = b"-----BEGIN ECHCONFIG-----\n".to_vec();
    for chunk in encoded.as_bytes().chunks(64) {
        pem.extend_from_slice(chunk);
        pem.push(b'\n');
    }
    pem.extend_from_slice(b"-----END ECHCONFIG-----\n");
    pem
}

fn take_u16(bytes: &[u8], position: &mut usize) -> io::Result<usize> {
    let value = take(bytes, position, 2)?;
    Ok(usize::from(u16::from_be_bytes([value[0], value[1]])))
}

fn take<'a>(bytes: &'a [u8], position: &mut usize, length: usize) -> io::Result<&'a [u8]> {
    let end = position
        .checked_add(length)
        .ok_or_else(|| invalid("ECH server key length overflow"))?;
    let value = bytes
        .get(*position..end)
        .ok_or_else(|| invalid("ECH server keys are truncated"))?;
    *position = end;
    Ok(value)
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}
