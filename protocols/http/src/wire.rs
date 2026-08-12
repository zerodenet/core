use alloc::string::{String, ToString};
use alloc::vec::Vec;

use zero_core::Error;
use zero_traits::AsyncSocket;

pub(super) const MAX_HEAD_SIZE: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Header {
    pub(super) name: Vec<u8>,
    pub(super) value: Vec<u8>,
}

pub(super) async fn read_head<S>(stream: &mut S) -> Result<Option<Vec<u8>>, Error>
where
    S: AsyncSocket,
{
    let mut head = Vec::new();
    loop {
        if head.len() >= MAX_HEAD_SIZE {
            return Err(Error::Protocol("HTTP message head is too large"));
        }
        let mut byte = [0_u8; 1];
        let read = stream
            .read(&mut byte)
            .await
            .map_err(|_| Error::Io("failed to read HTTP message head"))?;
        if read == 0 {
            return if head.is_empty() {
                Ok(None)
            } else {
                Err(Error::Protocol("HTTP message head ended unexpectedly"))
            };
        }
        head.push(byte[0]);
        if head.ends_with(b"\r\n\r\n") {
            return Ok(Some(head));
        }
    }
}

pub(super) fn parse_head(head: &[u8]) -> Result<(&[u8], Vec<Header>), Error> {
    if !head.ends_with(b"\r\n\r\n") {
        return Err(Error::Protocol("HTTP message head is incomplete"));
    }
    let first_end = find_crlf(head).ok_or(Error::Protocol("HTTP start line is incomplete"))?;
    let first = &head[..first_end];
    if first.is_empty() {
        return Err(Error::Protocol("HTTP start line is missing"));
    }

    let mut headers = Vec::new();
    let mut offset = first_end + 2;
    while offset < head.len() - 2 {
        let relative =
            find_crlf(&head[offset..]).ok_or(Error::Protocol("HTTP header line is incomplete"))?;
        if relative == 0 {
            break;
        }
        let line = &head[offset..offset + relative];
        if matches!(line.first(), Some(b' ' | b'\t')) {
            return Err(Error::Protocol(
                "obsolete folded HTTP headers are not supported",
            ));
        }
        let colon = line
            .iter()
            .position(|byte| *byte == b':')
            .ok_or(Error::Protocol("HTTP header is missing a colon"))?;
        let name = &line[..colon];
        if name.is_empty() || !name.iter().all(|byte| is_tchar(*byte)) {
            return Err(Error::Protocol("HTTP header name is invalid"));
        }
        let value = trim_ows(&line[colon + 1..]);
        if value.iter().any(|byte| matches!(*byte, b'\r' | b'\n' | 0)) {
            return Err(Error::Protocol("HTTP header value is invalid"));
        }
        headers.push(Header {
            name: name.to_vec(),
            value: value.to_vec(),
        });
        offset += relative + 2;
    }
    Ok((first, headers))
}

pub(super) fn header_values<'a>(headers: &'a [Header], name: &[u8]) -> Vec<&'a [u8]> {
    headers
        .iter()
        .filter(|header| eq_ascii(&header.name, name))
        .map(|header| header.value.as_slice())
        .collect()
}

pub(super) fn connection_tokens(headers: &[Header]) -> Result<Vec<Vec<u8>>, Error> {
    let mut tokens = Vec::new();
    for value in header_values(headers, b"connection") {
        for token in value.split(|byte| *byte == b',') {
            let token = trim_ows(token);
            if token.is_empty() || !token.iter().all(|byte| is_tchar(*byte)) {
                return Err(Error::Protocol("HTTP Connection token is invalid"));
            }
            tokens.push(token.iter().map(u8::to_ascii_lowercase).collect());
        }
    }
    Ok(tokens)
}

pub(super) fn has_token(headers: &[Header], name: &[u8], expected: &[u8]) -> bool {
    header_values(headers, name).into_iter().any(|value| {
        value
            .split(|byte| *byte == b',')
            .map(trim_ows)
            .any(|token| eq_ascii(token, expected))
    })
}

pub(super) fn content_length(headers: &[Header]) -> Result<Option<u64>, Error> {
    let mut length = None;
    for value in header_values(headers, b"content-length") {
        for item in value.split(|byte| *byte == b',') {
            let item = trim_ows(item);
            let text = core::str::from_utf8(item)
                .map_err(|_| Error::Protocol("HTTP Content-Length is not ASCII"))?;
            let parsed = text
                .parse::<u64>()
                .map_err(|_| Error::Protocol("HTTP Content-Length is invalid"))?;
            if length.is_some_and(|existing| existing != parsed) {
                return Err(Error::Protocol("conflicting HTTP Content-Length values"));
            }
            length = Some(parsed);
        }
    }
    Ok(length)
}

pub(super) fn transfer_encoding(headers: &[Header]) -> Result<Option<String>, Error> {
    let values = header_values(headers, b"transfer-encoding");
    if values.is_empty() {
        return Ok(None);
    }
    let mut codings = Vec::new();
    for value in values {
        for coding in value.split(|byte| *byte == b',') {
            let coding = trim_ows(coding);
            if coding.is_empty() {
                return Err(Error::Protocol("HTTP Transfer-Encoding is invalid"));
            }
            let name = coding
                .split(|byte| *byte == b';')
                .next()
                .unwrap_or_default();
            if !name.iter().all(|byte| is_tchar(*byte)) {
                return Err(Error::Protocol("HTTP transfer coding is invalid"));
            }
            codings.push(
                core::str::from_utf8(coding)
                    .map_err(|_| Error::Protocol("HTTP Transfer-Encoding is not ASCII"))?
                    .to_string(),
            );
        }
    }
    if !codings.last().is_some_and(|coding| {
        coding
            .split(';')
            .next()
            .unwrap_or_default()
            .trim()
            .eq_ignore_ascii_case("chunked")
    }) {
        return Err(Error::Protocol(
            "final HTTP transfer coding must be chunked",
        ));
    }
    Ok(Some(codings.join(", ")))
}

pub(super) fn append_header(output: &mut Vec<u8>, name: &[u8], value: &[u8]) {
    output.extend_from_slice(name);
    output.extend_from_slice(b": ");
    output.extend_from_slice(value);
    output.extend_from_slice(b"\r\n");
}

pub(super) fn is_hop_header(name: &[u8]) -> bool {
    [
        b"connection".as_slice(),
        b"proxy-connection".as_slice(),
        b"keep-alive".as_slice(),
        b"te".as_slice(),
        b"trailer".as_slice(),
        b"transfer-encoding".as_slice(),
        b"upgrade".as_slice(),
        b"proxy-authorization".as_slice(),
        b"proxy-authenticate".as_slice(),
    ]
    .iter()
    .any(|candidate| eq_ascii(name, candidate))
}

pub(super) fn named_by_connection(name: &[u8], tokens: &[Vec<u8>]) -> bool {
    tokens.iter().any(|token| eq_ascii(name, token))
}

pub(super) fn eq_ascii(left: &[u8], right: &[u8]) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn trim_ows(mut value: &[u8]) -> &[u8] {
    while matches!(value.first(), Some(b' ' | b'\t')) {
        value = &value[1..];
    }
    while matches!(value.last(), Some(b' ' | b'\t')) {
        value = &value[..value.len() - 1];
    }
    value
}

fn find_crlf(bytes: &[u8]) -> Option<usize> {
    bytes.windows(2).position(|window| window == b"\r\n")
}

fn is_tchar(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}
