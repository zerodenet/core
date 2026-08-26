use std::io;
use std::net::IpAddr;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use zero_core::{Address, Session, TargetHostSource};

use crate::transport::{RecordingStream, ReplayStream};

const TLS_SNI_SNIFF_TIMEOUT: Duration = Duration::from_millis(200);
const MAX_TLS_RECORD_LENGTH: usize = 18_432;
const MAX_CLIENT_HELLO_LENGTH: usize = 65_535;

pub(super) async fn sniff_tls_target<S>(
    mut session: Session,
    stream: S,
) -> (Session, ReplayStream<S>)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if !matches!(session.port, 443 | 8443)
        || !matches!(&session.target, Address::Ipv4(_) | Address::Ipv6(_))
    {
        return (session, ReplayStream::new(stream, Vec::new()));
    }

    let mut recording = RecordingStream::new(stream);
    let sniffed = tokio::time::timeout(TLS_SNI_SNIFF_TIMEOUT, peek_tls_sni(&mut recording)).await;
    let (stream, prefix) = recording.into_parts();

    match sniffed {
        Ok(Ok(Some(domain))) => {
            if let Some(domain) = normalize_sniffed_domain(domain) {
                tracing::debug!(
                    original_target = ?session.target,
                    sniffed_domain = %domain,
                    "TUN recovered domain from TLS ClientHello"
                );
                if session.original_target.is_none() {
                    session.original_target = Some(session.target.clone());
                }
                session.direct_target = Some(session.target.clone());
                session.sni = Some(domain.clone());
                session.target = Address::Domain(domain);
                session.target_host_source = Some(TargetHostSource::TlsSni);
            }
        }
        Ok(Err(error)) => {
            tracing::trace!(error = %error, "TUN TLS ClientHello sniff failed");
        }
        Err(_) => {
            tracing::trace!("TUN TLS ClientHello sniff timed out");
        }
        Ok(Ok(None)) => {}
    }

    (session, ReplayStream::new(stream, prefix))
}

async fn peek_tls_sni<R>(reader: &mut R) -> io::Result<Option<String>>
where
    R: AsyncRead + Unpin,
{
    let mut handshake = Vec::new();
    let handshake_length = loop {
        let mut record_header = [0_u8; 5];
        reader.read_exact(&mut record_header).await?;
        if record_header[0] != 0x16 {
            return Ok(None);
        }
        let record_length = u16::from_be_bytes([record_header[3], record_header[4]]) as usize;
        if record_length == 0 || record_length > MAX_TLS_RECORD_LENGTH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid TLS handshake record length",
            ));
        }
        let previous_length = handshake.len();
        handshake.resize(previous_length + record_length, 0);
        reader.read_exact(&mut handshake[previous_length..]).await?;

        if handshake.len() >= 4 {
            if handshake[0] != 0x01 {
                return Ok(None);
            }
            let length = ((handshake[1] as usize) << 16)
                | ((handshake[2] as usize) << 8)
                | handshake[3] as usize;
            if length > MAX_CLIENT_HELLO_LENGTH {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "TLS ClientHello exceeds sniffing limit",
                ));
            }
            if handshake.len() >= 4 + length {
                break length;
            }
        }
    };

    parse_client_hello_sni(&handshake[4..4 + handshake_length])
}

fn parse_client_hello_sni(client_hello: &[u8]) -> io::Result<Option<String>> {
    let mut offset = 34;
    let session_id_length = take_u8(client_hello, &mut offset)? as usize;
    take(client_hello, &mut offset, session_id_length)?;
    let cipher_suites_length = take_u16(client_hello, &mut offset)? as usize;
    take(client_hello, &mut offset, cipher_suites_length)?;
    let compression_methods_length = take_u8(client_hello, &mut offset)? as usize;
    take(client_hello, &mut offset, compression_methods_length)?;
    if offset == client_hello.len() {
        return Ok(None);
    }
    let extensions_length = take_u16(client_hello, &mut offset)? as usize;
    let extensions = take(client_hello, &mut offset, extensions_length)?;
    Ok(parse_sni_extension(extensions))
}

fn parse_sni_extension(extensions: &[u8]) -> Option<String> {
    let mut offset = 0;
    while offset + 4 <= extensions.len() {
        let extension_type = u16::from_be_bytes([extensions[offset], extensions[offset + 1]]);
        let extension_length =
            u16::from_be_bytes([extensions[offset + 2], extensions[offset + 3]]) as usize;
        offset += 4;
        if offset + extension_length > extensions.len() {
            return None;
        }
        let extension = &extensions[offset..offset + extension_length];
        if extension_type == 0 && extension.len() >= 5 && extension[2] == 0 {
            let name_length = u16::from_be_bytes([extension[3], extension[4]]) as usize;
            if 5 + name_length <= extension.len() {
                return std::str::from_utf8(&extension[5..5 + name_length])
                    .ok()
                    .map(ToOwned::to_owned);
            }
        }
        offset += extension_length;
    }
    None
}

fn take_u8(bytes: &[u8], offset: &mut usize) -> io::Result<u8> {
    Ok(take(bytes, offset, 1)?[0])
}

fn take_u16(bytes: &[u8], offset: &mut usize) -> io::Result<u16> {
    let bytes = take(bytes, offset, 2)?;
    Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
}

fn take<'a>(bytes: &'a [u8], offset: &mut usize, length: usize) -> io::Result<&'a [u8]> {
    let end = offset.checked_add(length).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "TLS ClientHello length overflow",
        )
    })?;
    let value = bytes
        .get(*offset..end)
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "truncated TLS ClientHello"))?;
    *offset = end;
    Ok(value)
}

fn normalize_sniffed_domain(domain: String) -> Option<String> {
    let domain = domain.trim_end_matches('.').to_ascii_lowercase();
    if domain.is_empty()
        || domain.len() > 253
        || domain.parse::<IpAddr>().is_ok()
        || domain.split('.').any(|label| {
            label.is_empty()
                || label.len() > 63
                || label.starts_with('-')
                || label.ends_with('-')
                || !label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
    {
        return None;
    }
    Some(domain)
}
