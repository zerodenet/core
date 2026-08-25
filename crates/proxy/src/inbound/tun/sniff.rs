use std::io;
use std::net::IpAddr;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use zero_core::{Address, Session, TargetHostSource};

use crate::transport::{RecordingStream, ReplayStream};

const TARGET_SNIFF_TIMEOUT: Duration = Duration::from_millis(200);
const MAX_TLS_RECORD_LENGTH: usize = 18_432;
const MAX_CLIENT_HELLO_LENGTH: usize = 65_535;
const MAX_HTTP_HEADER_LENGTH: usize = 32 * 1024;

pub(super) async fn sniff_tcp_target<S>(
    mut session: Session,
    stream: S,
) -> (Session, ReplayStream<S>)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if !matches!(&session.target, Address::Ipv4(_) | Address::Ipv6(_)) {
        return (session, ReplayStream::new(stream, Vec::new()));
    }

    let protocol = match session.port {
        443 | 8443 => SniffProtocol::Tls,
        80 | 8000 | 8080 | 8888 => SniffProtocol::Http,
        _ => return (session, ReplayStream::new(stream, Vec::new())),
    };

    let mut recording = RecordingStream::new(stream);
    let sniffed = match protocol {
        SniffProtocol::Tls => {
            tokio::time::timeout(TARGET_SNIFF_TIMEOUT, peek_tls_target(&mut recording)).await
        }
        SniffProtocol::Http => {
            tokio::time::timeout(TARGET_SNIFF_TIMEOUT, peek_http_target(&mut recording)).await
        }
    };
    let (stream, prefix) = recording.into_parts();

    match sniffed {
        Ok(Ok(SniffOutcome::Domain { domain, source })) => {
            if let Some(domain) = normalize_sniffed_domain(domain) {
                tracing::debug!(
                    original_target = ?session.target,
                    sniffed_domain = %domain,
                    source = source.as_str(),
                    "TUN recovered domain from application traffic"
                );
                if session.original_target.is_none() {
                    session.original_target = Some(session.target.clone());
                }
                session.direct_target = Some(session.target.clone());
                if source == TargetHostSource::TlsSni {
                    session.sni = Some(domain.clone());
                }
                session.target = Address::Domain(domain);
                session.target_host_source = Some(source);
            }
        }
        Ok(Ok(SniffOutcome::EncryptedClientHello)) => {
            tracing::debug!(
                original_target = ?session.target,
                "TUN observed ECH and retained deterministic DNS-reverse/IP fallback"
            );
        }
        Ok(Err(error)) => {
            tracing::trace!(error = %error, "TUN application target sniff failed");
        }
        Err(_) => {
            tracing::trace!("TUN application target sniff timed out");
        }
        Ok(Ok(SniffOutcome::None)) => {}
    }

    (session, ReplayStream::new(stream, prefix))
}

#[derive(Clone, Copy)]
enum SniffProtocol {
    Tls,
    Http,
}

enum SniffOutcome {
    Domain {
        domain: String,
        source: TargetHostSource,
    },
    EncryptedClientHello,
    None,
}

async fn peek_tls_target<R>(reader: &mut R) -> io::Result<SniffOutcome>
where
    R: AsyncRead + Unpin,
{
    let mut handshake = Vec::new();
    let handshake_length = loop {
        let mut record_header = [0_u8; 5];
        reader.read_exact(&mut record_header).await?;
        if record_header[0] != 0x16 {
            return Ok(SniffOutcome::None);
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
                return Ok(SniffOutcome::None);
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

    let parsed = parse_client_hello(&handshake[4..4 + handshake_length])?;
    if parsed.encrypted_client_hello {
        return Ok(SniffOutcome::EncryptedClientHello);
    }
    Ok(parsed
        .sni
        .map_or(SniffOutcome::None, |domain| SniffOutcome::Domain {
            domain,
            source: TargetHostSource::TlsSni,
        }))
}

struct ParsedTlsHello {
    sni: Option<String>,
    encrypted_client_hello: bool,
}

fn parse_client_hello(client_hello: &[u8]) -> io::Result<ParsedTlsHello> {
    let mut offset = 34;
    let session_id_length = take_u8(client_hello, &mut offset)? as usize;
    take(client_hello, &mut offset, session_id_length)?;
    let cipher_suites_length = take_u16(client_hello, &mut offset)? as usize;
    take(client_hello, &mut offset, cipher_suites_length)?;
    let compression_methods_length = take_u8(client_hello, &mut offset)? as usize;
    take(client_hello, &mut offset, compression_methods_length)?;
    if offset == client_hello.len() {
        return Ok(ParsedTlsHello {
            sni: None,
            encrypted_client_hello: false,
        });
    }
    let extensions_length = take_u16(client_hello, &mut offset)? as usize;
    let extensions = take(client_hello, &mut offset, extensions_length)?;
    Ok(parse_tls_extensions(extensions))
}

fn parse_tls_extensions(extensions: &[u8]) -> ParsedTlsHello {
    let mut offset = 0;
    let mut sni = None;
    let mut encrypted_client_hello = false;
    while offset + 4 <= extensions.len() {
        let extension_type = u16::from_be_bytes([extensions[offset], extensions[offset + 1]]);
        let extension_length =
            u16::from_be_bytes([extensions[offset + 2], extensions[offset + 3]]) as usize;
        offset += 4;
        if offset + extension_length > extensions.len() {
            break;
        }
        let extension = &extensions[offset..offset + extension_length];
        if extension_type == 0 && extension.len() >= 5 && extension[2] == 0 {
            let name_length = u16::from_be_bytes([extension[3], extension[4]]) as usize;
            if 5 + name_length <= extension.len() {
                sni = std::str::from_utf8(&extension[5..5 + name_length])
                    .ok()
                    .map(ToOwned::to_owned);
            }
        } else if extension_type == 0xfe0d {
            encrypted_client_hello = true;
        }
        offset += extension_length;
    }
    ParsedTlsHello {
        sni,
        encrypted_client_hello,
    }
}

async fn peek_http_target<R>(reader: &mut R) -> io::Result<SniffOutcome>
where
    R: AsyncRead + Unpin,
{
    let mut headers = Vec::with_capacity(1024);
    let mut chunk = [0_u8; 1024];
    loop {
        let size = reader.read(&mut chunk).await?;
        if size == 0 {
            return Ok(SniffOutcome::None);
        }
        headers.extend_from_slice(&chunk[..size]);
        if headers.len() > MAX_HTTP_HEADER_LENGTH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "HTTP request headers exceed sniffing limit",
            ));
        }
        if let Some(end) = find_http_header_end(&headers) {
            return Ok(
                parse_http_host(&headers[..end]).map_or(SniffOutcome::None, |domain| {
                    SniffOutcome::Domain {
                        domain,
                        source: TargetHostSource::HttpHost,
                    }
                }),
            );
        }
        if headers.len() >= 16 && !looks_like_http_request(&headers) {
            return Ok(SniffOutcome::None);
        }
    }
}

fn find_http_header_end(bytes: &[u8]) -> Option<usize> {
    bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|offset| offset + 4)
}

fn looks_like_http_request(bytes: &[u8]) -> bool {
    let Some(space) = bytes.iter().position(|byte| *byte == b' ') else {
        return false;
    };
    space > 0
        && bytes[..space]
            .iter()
            .all(|byte| byte.is_ascii_uppercase() || *byte == b'-')
}

fn parse_http_host(headers: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(headers).ok()?;
    let mut lines = text.split("\r\n");
    let request_line = lines.next()?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next()?;
    let target = request_parts.next()?;
    let version = request_parts.next()?;
    if request_parts.next().is_some()
        || method.is_empty()
        || !method
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte == b'-')
        || !matches!(version, "HTTP/1.0" | "HTTP/1.1")
    {
        return None;
    }
    let host = lines.find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("host").then(|| value.trim())
    });
    host.or_else(|| absolute_form_authority(target))
        .and_then(strip_http_port)
        .map(ToOwned::to_owned)
}

fn absolute_form_authority(target: &str) -> Option<&str> {
    let remainder = target
        .strip_prefix("http://")
        .or_else(|| target.strip_prefix("https://"))?;
    let authority = remainder.split(['/', '?', '#']).next()?;
    (!authority.is_empty() && !authority.contains('@')).then_some(authority)
}

fn strip_http_port(authority: &str) -> Option<&str> {
    if authority.starts_with('[') {
        return None;
    }
    match authority.rsplit_once(':') {
        Some((host, port)) if !host.is_empty() && port.parse::<u16>().is_ok() => Some(host),
        Some(_) => None,
        None => Some(authority),
    }
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

#[cfg(test)]
mod tests;
