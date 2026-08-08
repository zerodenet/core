use alloc::vec::Vec;

use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_128_GCM};
use ring::digest;
use ring::hkdf;
use ring::rand::SecureRandom;
use zero_core::{Address, Error};
use zero_traits::AsyncSocket;

use super::is_zero_aead_flow;
use crate::shared::{read_exact, write_address, ATYP_DOMAIN, ATYP_IPV4, ATYP_IPV6};

const AEAD_KEY_LEN: usize = 16;
const AEAD_NONCE_LEN: usize = 12;
const AEAD_TAG_LEN: usize = 16;

fn derive_flow_key(uuid: &[u8; 16], salt: &[u8]) -> Result<[u8; AEAD_KEY_LEN], Error> {
    let salt = hkdf::Salt::new(hkdf::HKDF_SHA256, salt);
    let prk = salt.extract(uuid);
    let info = b"vless flow aead key";
    let mut key = [0u8; AEAD_KEY_LEN];
    prk.expand(&[info], HkdfKeyLen(AEAD_KEY_LEN))
        .and_then(|okm| okm.fill(&mut key))
        .map_err(|_| Error::Protocol("flow key derivation failed"))?;
    Ok(key)
}

struct HkdfKeyLen(usize);

impl hkdf::KeyType for HkdfKeyLen {
    fn len(&self) -> usize {
        self.0
    }
}

/// Builds Zero's private AEAD v1 request command block.
pub fn flow_build_request(
    uuid: &[u8; 16],
    flow: Option<&str>,
    command: u8,
    port: u16,
    address: &Address,
) -> Result<(u8, Vec<u8>), Error> {
    if !is_zero_aead_flow(flow) {
        let mut buf = Vec::with_capacity(4);
        buf.push(command);
        buf.extend_from_slice(&port.to_be_bytes());
        write_address(&mut buf, address)?;
        return Ok((0x00, buf));
    }

    let mut plain = Vec::new();
    plain.push(command);
    plain.extend_from_slice(&port.to_be_bytes());
    write_address(&mut plain, address)?;

    let rng = ring::rand::SystemRandom::new();
    let mut salt = [0u8; 8];
    rng.fill(&mut salt)
        .map_err(|_| Error::Protocol("random generation failed"))?;

    let key_bytes = derive_flow_key(uuid, &salt)?;
    let unbound = UnboundKey::new(&AES_128_GCM, &key_bytes)
        .map_err(|_| Error::Protocol("flow key init failed"))?;
    let key = LessSafeKey::new(unbound);

    let mut pad_buf = [0u8; 1];
    rng.fill(&mut pad_buf)
        .map_err(|_| Error::Protocol("random generation failed"))?;
    let pad_len = (pad_buf[0] & 0x1f) as usize;
    let mut padded = Vec::with_capacity(1 + pad_len + 2 + plain.len() + AEAD_TAG_LEN);
    padded.push(pad_len as u8);
    padded.resize(1 + pad_len, 0);
    padded.extend_from_slice(&(plain.len() as u16).to_be_bytes());
    padded.extend_from_slice(&plain);

    let nonce = flow_nonce(&salt);
    key.seal_in_place_append_tag(nonce, Aad::empty(), &mut padded)
        .map_err(|_| Error::Protocol("flow encryption failed"))?;

    let mut payload = Vec::with_capacity(salt.len() + padded.len());
    payload.extend_from_slice(&salt);
    payload.extend_from_slice(&padded);
    Ok((0x01, payload))
}

pub(crate) async fn flow_read_request<S>(
    stream: &mut S,
    flow: Option<&str>,
    uuid: &[u8; 16],
) -> Result<(u8, u16, Address), Error>
where
    S: AsyncSocket,
{
    if !is_zero_aead_flow(flow) {
        return read_plain_request(stream).await;
    }

    let mut salt = [0u8; 8];
    read_exact(stream, &mut salt).await?;
    let mut buf = Vec::with_capacity(320);
    ensure_read(stream, &mut buf, 1).await?;

    let pad_len = buf[0] as usize;
    if pad_len > 31 {
        return Err(Error::Protocol("VLESS flow: invalid padding length"));
    }
    ensure_read(stream, &mut buf, 1 + pad_len + 2).await?;
    let plain_len = u16::from_be_bytes([buf[1 + pad_len], buf[2 + pad_len]]) as usize;
    if plain_len > 300 {
        return Err(Error::Protocol("VLESS flow: invalid plain block length"));
    }

    let total_payload = 1 + pad_len + 2 + plain_len + AEAD_TAG_LEN;
    ensure_read(stream, &mut buf, total_payload).await?;
    let mut encrypted = buf[..total_payload].to_vec();
    let key_bytes = derive_flow_key(uuid, &salt)?;
    let unbound = UnboundKey::new(&AES_128_GCM, &key_bytes)
        .map_err(|_| Error::Protocol("flow key init failed"))?;
    let key = LessSafeKey::new(unbound);
    let decrypted = key
        .open_in_place(flow_nonce(&salt), Aad::empty(), &mut encrypted)
        .map_err(|_| {
            Error::Protocol("VLESS flow decryption failed - wrong key or corrupted data")
        })?;

    let plain_offset = 1 + pad_len + 2;
    parse_plain_block(&decrypted[plain_offset..plain_offset + plain_len])
}

fn flow_nonce(salt: &[u8]) -> Nonce {
    let mut nonce_bytes = [0u8; AEAD_NONCE_LEN];
    let mut ctx = digest::Context::new(&digest::SHA256);
    ctx.update(salt);
    ctx.update(b"vless flow nonce");
    let hash = ctx.finish();
    nonce_bytes.copy_from_slice(&hash.as_ref()[..AEAD_NONCE_LEN]);
    Nonce::assume_unique_for_key(nonce_bytes)
}

async fn read_plain_request<S>(stream: &mut S) -> Result<(u8, u16, Address), Error>
where
    S: AsyncSocket,
{
    let mut command = [0u8; 1];
    read_exact(stream, &mut command).await?;
    let mut port = [0u8; 2];
    read_exact(stream, &mut port).await?;
    let port = u16::from_be_bytes(port);
    if port == 0 {
        return Err(Error::Protocol("VLESS target port must not be 0"));
    }
    let mut atyp = [0u8; 1];
    read_exact(stream, &mut atyp).await?;
    let target = read_address_from_stream(stream, atyp[0]).await?;
    Ok((command[0], port, target))
}

fn parse_plain_block(plain: &[u8]) -> Result<(u8, u16, Address), Error> {
    if plain.len() < 4 {
        return Err(Error::Protocol("VLESS flow: truncated plain block"));
    }
    let command = plain[0];
    let port = u16::from_be_bytes([plain[1], plain[2]]);
    if port == 0 {
        return Err(Error::Protocol("VLESS target port must not be 0"));
    }
    let target = read_address_from_bytes(plain[3], &plain[4..])?;
    Ok((command, port, target))
}

async fn ensure_read<S>(stream: &mut S, buf: &mut Vec<u8>, target_len: usize) -> Result<(), Error>
where
    S: AsyncSocket,
{
    while buf.len() < target_len {
        let mut chunk = [0u8; 512];
        let n = stream
            .read(&mut chunk)
            .await
            .map_err(|_| Error::Io("failed to read flow data"))?;
        if n == 0 {
            return Err(Error::Io("unexpected EOF during flow read"));
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    Ok(())
}

fn read_address_from_bytes(atyp: u8, data: &[u8]) -> Result<Address, Error> {
    match atyp {
        ATYP_IPV4 if data.len() >= 4 => Ok(Address::Ipv4(data[..4].try_into().unwrap())),
        ATYP_IPV6 if data.len() >= 16 => Ok(Address::Ipv6(data[..16].try_into().unwrap())),
        ATYP_DOMAIN if !data.is_empty() => {
            let len = data[0] as usize;
            if len == 0 || data.len() < 1 + len {
                return Err(Error::Protocol("VLESS flow: truncated domain address"));
            }
            let domain = alloc::string::String::from_utf8(data[1..1 + len].to_vec())
                .map_err(|_| Error::Protocol("VLESS domain is not valid UTF-8"))?;
            Ok(Address::Domain(domain))
        }
        ATYP_IPV4 | ATYP_IPV6 | ATYP_DOMAIN => {
            Err(Error::Protocol("VLESS flow: truncated address"))
        }
        _ => Err(Error::Unsupported("VLESS address type is not supported")),
    }
}

async fn read_address_from_stream<S>(stream: &mut S, atyp: u8) -> Result<Address, Error>
where
    S: AsyncSocket,
{
    match atyp {
        ATYP_IPV4 => {
            let mut bytes = [0u8; 4];
            read_exact(stream, &mut bytes).await?;
            Ok(Address::Ipv4(bytes))
        }
        ATYP_IPV6 => {
            let mut bytes = [0u8; 16];
            read_exact(stream, &mut bytes).await?;
            Ok(Address::Ipv6(bytes))
        }
        ATYP_DOMAIN => {
            let mut length = [0u8; 1];
            read_exact(stream, &mut length).await?;
            let len = length[0] as usize;
            if len == 0 {
                return Err(Error::Protocol("VLESS domain must not be empty"));
            }
            let mut domain = alloc::vec![0u8; len];
            read_exact(stream, &mut domain).await?;
            let domain = alloc::string::String::from_utf8(domain)
                .map_err(|_| Error::Protocol("VLESS domain is not valid UTF-8"))?;
            Ok(Address::Domain(domain))
        }
        _ => Err(Error::Unsupported("VLESS address type is not supported")),
    }
}
