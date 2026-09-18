use rand::Rng;
use zero_core::{Address, Error, Session};
use zero_traits::AsyncSocket;

use crate::crypto::{
    create_xray_auth_id, current_timestamp, derive_xray_cmd_key, seal_xray_aead_header,
};
use crate::VmessCipher;

pub const VERSION: u8 = 0x01;
pub const CMD_TCP: u8 = 0x01;
pub const CMD_UDP: u8 = 0x02;
pub const MUX_COOL_DOMAIN: &str = "v1.mux.cool";
pub const MUX_COOL_PORT: u16 = 666;

const ATYP_IPV4: u8 = 0x01;
const ATYP_DOMAIN: u8 = 0x02;
const ATYP_IPV6: u8 = 0x03;

pub const AUTH_ID_LEN: usize = 16;

pub struct VmessOutboundSession {
    pub upload_key: Vec<u8>,
    pub upload_nonce: Vec<u8>,
    pub download_key: Vec<u8>,
    pub download_nonce: Vec<u8>,
    pub cipher: VmessCipher,
    pub chunk_stream: bool,
    pub authenticated_length: bool,
    pub chunk_masking: bool,
    pub global_padding: bool,
    pub length_key_source: Vec<u8>,
    pub length_nonce_source: Vec<u8>,
    pub response_header: Option<u8>,
}

struct PendingVmessSession {
    request_len: usize,
    response_header: u8,
    request_key: Vec<u8>,
    request_nonce: Vec<u8>,
    response_key: Vec<u8>,
    response_nonce: Vec<u8>,
    cipher: VmessCipher,
    chunk_stream: bool,
    authenticated_length: bool,
    chunk_masking: bool,
    global_padding: bool,
}

impl PendingVmessSession {
    fn into_session(self) -> VmessOutboundSession {
        let length_key_source = self.request_key.clone();
        let length_nonce_source = self.request_nonce.clone();
        VmessOutboundSession {
            upload_key: self.request_key,
            upload_nonce: self.request_nonce,
            download_key: self.response_key,
            download_nonce: self.response_nonce,
            cipher: self.cipher,
            chunk_stream: self.chunk_stream,
            authenticated_length: self.authenticated_length,
            chunk_masking: self.chunk_masking,
            global_padding: self.global_padding,
            length_key_source,
            length_nonce_source,
            response_header: Some(self.response_header),
        }
    }
}

pub async fn read_exact<S: AsyncSocket>(stream: &mut S, buf: &mut [u8]) -> Result<(), Error> {
    let mut offset = 0;
    while offset < buf.len() {
        let n = stream
            .read(&mut buf[offset..])
            .await
            .map_err(|_| Error::Io("vmess: failed to read from socket"))?;
        if n == 0 {
            return Err(Error::Io("vmess: unexpected EOF while reading socket"));
        }
        offset += n;
    }
    Ok(())
}

pub fn write_address(buf: &mut Vec<u8>, address: &Address) -> Result<(), Error> {
    match address {
        Address::Ipv4(addr) => {
            buf.push(ATYP_IPV4);
            buf.extend_from_slice(addr);
        }
        Address::Domain(domain) => {
            let domain_bytes = domain.as_bytes();
            if domain_bytes.len() > 255 {
                return Err(Error::Protocol("vmess domain too long (>255)"));
            }
            buf.push(ATYP_DOMAIN);
            buf.push(domain_bytes.len() as u8);
            buf.extend_from_slice(domain_bytes);
        }
        Address::Ipv6(addr) => {
            buf.push(ATYP_IPV6);
            buf.extend_from_slice(addr);
        }
    }
    Ok(())
}

pub fn parse_address_from_bytes(atyp: u8, bytes: &[u8]) -> Result<Address, Error> {
    match atyp {
        ATYP_IPV4 => {
            if bytes.len() < 4 {
                return Err(Error::Protocol("vmess truncated ipv4"));
            }
            let addr: [u8; 4] = bytes[..4].try_into().unwrap();
            Ok(Address::Ipv4(addr))
        }
        ATYP_DOMAIN => {
            if bytes.is_empty() {
                return Err(Error::Protocol("vmess truncated domain length"));
            }
            let len = bytes[0] as usize;
            if bytes.len() < 1 + len {
                return Err(Error::Protocol("vmess truncated domain"));
            }
            let domain = std::str::from_utf8(&bytes[1..1 + len])
                .map_err(|_| Error::Protocol("vmess domain not utf-8"))?;
            Ok(Address::Domain(domain.to_owned()))
        }
        ATYP_IPV6 => {
            if bytes.len() < 16 {
                return Err(Error::Protocol("vmess truncated ipv6"));
            }
            let addr: [u8; 16] = bytes[..16].try_into().unwrap();
            Ok(Address::Ipv6(addr))
        }
        _ => Err(Error::Protocol("vmess unexpected address type")),
    }
}

pub(crate) async fn establish_outbound_session<S: AsyncSocket>(
    stream: &mut S,
    session: &Session,
    uuid: &[u8; 16],
    cipher: VmessCipher,
    command: u8,
) -> Result<VmessOutboundSession, Error> {
    establish_outbound_session_with_request_len(stream, session, uuid, cipher, command)
        .await
        .map(|(outbound_session, _request_len)| outbound_session)
}

pub(crate) async fn establish_outbound_session_with_request_len<S: AsyncSocket>(
    stream: &mut S,
    session: &Session,
    uuid: &[u8; 16],
    cipher: VmessCipher,
    command: u8,
) -> Result<(VmessOutboundSession, usize), Error> {
    let pending = send_request(stream, session, uuid, cipher, command).await?;
    let request_len = pending.request_len;
    Ok((pending.into_session(), request_len))
}

async fn send_request<S: AsyncSocket>(
    stream: &mut S,
    session: &Session,
    uuid: &[u8; 16],
    cipher: VmessCipher,
    command: u8,
) -> Result<PendingVmessSession, Error> {
    let cmd_key = derive_xray_cmd_key(uuid);
    let timestamp = current_timestamp();
    let request_body_key = rand::rng().random::<[u8; 16]>();
    let request_body_nonce = rand::rng().random::<[u8; 16]>();
    let response_header = rand::rng().random::<u8>();

    let mut header = Vec::new();
    header.push(VERSION);
    header.extend_from_slice(&request_body_nonce);
    header.extend_from_slice(&request_body_key);
    header.push(response_header);
    let chunk_masking = false;
    let global_padding = false;
    let chunk_stream = cipher != VmessCipher::Zero;
    let options = if chunk_stream { 0x01 } else { 0x00 };
    header.push(options);
    let security = security_byte(cipher);
    header.push(security);
    header.push(0x00);
    header.push(command);
    if command != 0x03 {
        write_address_port_xray(&mut header, &session.target, session.port)?;
    }
    let checksum = fnv1a32(&header);
    header.extend_from_slice(&checksum.to_be_bytes());

    let auth_id = create_xray_auth_id(&cmd_key, timestamp)?;
    let packet = seal_xray_aead_header(&cmd_key, &auth_id, &header)?;

    stream
        .write_all(&packet)
        .await
        .map_err(|_| Error::Io("vmess: failed to write to socket"))?;

    let response_key = sha256_16(&request_body_key);
    let response_nonce = sha256_16(&request_body_nonce);

    Ok(PendingVmessSession {
        request_len: packet.len(),
        response_header,
        request_key: request_body_key.to_vec(),
        request_nonce: request_body_nonce.to_vec(),
        response_key,
        response_nonce,
        cipher,
        chunk_stream,
        authenticated_length: false,
        chunk_masking,
        global_padding,
    })
}

fn security_byte(cipher: VmessCipher) -> u8 {
    match cipher {
        VmessCipher::Aes128Gcm => 0x03,
        VmessCipher::Chacha20Poly1305 => 0x04,
        VmessCipher::None => 0x05,
        VmessCipher::Zero => 0x05,
        VmessCipher::ZeroPlus => 0x06,
    }
}

fn write_address_port_xray(buf: &mut Vec<u8>, address: &Address, port: u16) -> Result<(), Error> {
    buf.extend_from_slice(&port.to_be_bytes());
    match address {
        Address::Ipv4(addr) => {
            buf.push(0x01);
            buf.extend_from_slice(addr);
        }
        Address::Domain(domain) => {
            let bytes = domain.as_bytes();
            if bytes.len() > 255 {
                return Err(Error::Protocol("vmess domain too long (>255)"));
            }
            buf.push(0x02);
            buf.push(bytes.len() as u8);
            buf.extend_from_slice(bytes);
        }
        Address::Ipv6(addr) => {
            buf.push(0x03);
            buf.extend_from_slice(addr);
        }
    }
    Ok(())
}

fn fnv1a32(bytes: &[u8]) -> u32 {
    let mut hash = 0x811c9dc5_u32;
    for byte in bytes {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x01000193);
    }
    hash
}

fn sha256_16(bytes: &[u8; 16]) -> Vec<u8> {
    let digest = ring::digest::digest(&ring::digest::SHA256, bytes);
    digest.as_ref()[..16].to_vec()
}
