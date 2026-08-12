use alloc::format;
use alloc::vec;
use alloc::vec::Vec;

use zero_core::Error;
use zero_traits::AsyncSocket;

const MAX_CHUNK_LINE_SIZE: usize = 8192;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpBodyKind {
    None,
    ContentLength(u64),
    Chunked,
    UntilClose,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HttpTransferCount {
    pub read: u64,
    pub written: u64,
}

pub async fn relay_http_body<R, W>(
    reader: &mut R,
    writer: &mut W,
    kind: HttpBodyKind,
) -> Result<HttpTransferCount, Error>
where
    R: AsyncSocket,
    W: AsyncSocket,
{
    match kind {
        HttpBodyKind::None => Ok(HttpTransferCount::default()),
        HttpBodyKind::ContentLength(length) => relay_exact(reader, writer, length).await,
        HttpBodyKind::Chunked => relay_chunked(reader, writer).await,
        HttpBodyKind::UntilClose => relay_until_close(reader, writer).await,
    }
}

pub async fn relay_close_delimited_as_chunked<R, W>(
    reader: &mut R,
    writer: &mut W,
) -> Result<HttpTransferCount, Error>
where
    R: AsyncSocket,
    W: AsyncSocket,
{
    let mut count = HttpTransferCount::default();
    let mut buffer = vec![0_u8; 16 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .await
            .map_err(|_| Error::Io("failed to read close-delimited HTTP body"))?;
        if read == 0 {
            writer
                .write_all(b"0\r\n\r\n")
                .await
                .map_err(|_| Error::Io("failed to finish chunked HTTP body"))?;
            count.written = count.written.saturating_add(5);
            return Ok(count);
        }

        let prefix = format!("{read:X}\r\n");
        writer
            .write_all(prefix.as_bytes())
            .await
            .map_err(|_| Error::Io("failed to write chunked HTTP body"))?;
        writer
            .write_all(&buffer[..read])
            .await
            .map_err(|_| Error::Io("failed to write chunked HTTP body"))?;
        writer
            .write_all(b"\r\n")
            .await
            .map_err(|_| Error::Io("failed to write chunked HTTP body"))?;
        count.read = count.read.saturating_add(read as u64);
        count.written = count
            .written
            .saturating_add(prefix.len() as u64)
            .saturating_add(read as u64)
            .saturating_add(2);
    }
}

async fn relay_exact<R, W>(
    reader: &mut R,
    writer: &mut W,
    mut remaining: u64,
) -> Result<HttpTransferCount, Error>
where
    R: AsyncSocket,
    W: AsyncSocket,
{
    let mut count = HttpTransferCount::default();
    let mut buffer = vec![0_u8; 16 * 1024];
    while remaining > 0 {
        let wanted = usize::try_from(remaining.min(buffer.len() as u64)).unwrap_or(buffer.len());
        let read = reader
            .read(&mut buffer[..wanted])
            .await
            .map_err(|_| Error::Io("failed to read fixed-length HTTP body"))?;
        if read == 0 {
            return Err(Error::Protocol("HTTP body ended before Content-Length"));
        }
        writer
            .write_all(&buffer[..read])
            .await
            .map_err(|_| Error::Io("failed to write fixed-length HTTP body"))?;
        remaining -= read as u64;
        count.read = count.read.saturating_add(read as u64);
        count.written = count.written.saturating_add(read as u64);
    }
    Ok(count)
}

async fn relay_until_close<R, W>(reader: &mut R, writer: &mut W) -> Result<HttpTransferCount, Error>
where
    R: AsyncSocket,
    W: AsyncSocket,
{
    let mut count = HttpTransferCount::default();
    let mut buffer = vec![0_u8; 16 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .await
            .map_err(|_| Error::Io("failed to read close-delimited HTTP body"))?;
        if read == 0 {
            return Ok(count);
        }
        writer
            .write_all(&buffer[..read])
            .await
            .map_err(|_| Error::Io("failed to write close-delimited HTTP body"))?;
        count.read = count.read.saturating_add(read as u64);
        count.written = count.written.saturating_add(read as u64);
    }
}

async fn relay_chunked<R, W>(reader: &mut R, writer: &mut W) -> Result<HttpTransferCount, Error>
where
    R: AsyncSocket,
    W: AsyncSocket,
{
    let mut count = HttpTransferCount::default();
    loop {
        let line = read_line(reader).await?;
        let size = parse_chunk_size(&line)?;
        write_counted(writer, &line, &mut count).await?;
        if size == 0 {
            let mut trailers = Vec::new();
            loop {
                let trailer = read_line(reader).await?;
                count.read = count.read.saturating_add(trailer.len() as u64);
                if trailer == b"\r\n" {
                    break;
                }
                trailers.push(parse_trailer(trailer)?);
            }
            let connection_tokens = trailers
                .iter()
                .filter(|trailer| trailer.name.eq_ignore_ascii_case(b"connection"))
                .flat_map(|trailer| trailer.value.split(|byte| *byte == b','))
                .map(trim_ows)
                .filter(|token| !token.is_empty())
                .map(|token| token.iter().map(u8::to_ascii_lowercase).collect::<Vec<_>>())
                .collect::<Vec<_>>();
            for trailer in trailers {
                if forbidden_trailer(&trailer.name, &connection_tokens) {
                    continue;
                }
                writer
                    .write_all(&trailer.raw)
                    .await
                    .map_err(|_| Error::Io("failed to write HTTP trailer"))?;
                count.written = count.written.saturating_add(trailer.raw.len() as u64);
            }
            writer
                .write_all(b"\r\n")
                .await
                .map_err(|_| Error::Io("failed to finish HTTP trailers"))?;
            count.written = count.written.saturating_add(2);
            return Ok(count);
        }

        let payload = relay_exact(reader, writer, size).await?;
        count.read = count.read.saturating_add(payload.read);
        count.written = count.written.saturating_add(payload.written);
        let ending = read_exact_bytes(reader, 2).await?;
        if ending != b"\r\n" {
            return Err(Error::Protocol("HTTP chunk payload is missing CRLF"));
        }
        write_counted(writer, &ending, &mut count).await?;
    }
}

async fn write_counted<W>(
    writer: &mut W,
    bytes: &[u8],
    count: &mut HttpTransferCount,
) -> Result<(), Error>
where
    W: AsyncSocket,
{
    writer
        .write_all(bytes)
        .await
        .map_err(|_| Error::Io("failed to write chunked HTTP body"))?;
    count.read = count.read.saturating_add(bytes.len() as u64);
    count.written = count.written.saturating_add(bytes.len() as u64);
    Ok(())
}

async fn read_line<R>(reader: &mut R) -> Result<Vec<u8>, Error>
where
    R: AsyncSocket,
{
    let mut line = Vec::new();
    loop {
        if line.len() >= MAX_CHUNK_LINE_SIZE {
            return Err(Error::Protocol("HTTP chunk line is too large"));
        }
        let mut byte = [0_u8; 1];
        let read = reader
            .read(&mut byte)
            .await
            .map_err(|_| Error::Io("failed to read HTTP chunk line"))?;
        if read == 0 {
            return Err(Error::Protocol("HTTP chunked body ended unexpectedly"));
        }
        line.push(byte[0]);
        if line.ends_with(b"\r\n") {
            return Ok(line);
        }
    }
}

async fn read_exact_bytes<R>(reader: &mut R, length: usize) -> Result<Vec<u8>, Error>
where
    R: AsyncSocket,
{
    let mut bytes = vec![0_u8; length];
    let mut offset = 0;
    while offset < length {
        let read = reader
            .read(&mut bytes[offset..])
            .await
            .map_err(|_| Error::Io("failed to read HTTP body framing"))?;
        if read == 0 {
            return Err(Error::Protocol("HTTP body framing ended unexpectedly"));
        }
        offset += read;
    }
    Ok(bytes)
}

fn parse_chunk_size(line: &[u8]) -> Result<u64, Error> {
    let text =
        core::str::from_utf8(line).map_err(|_| Error::Protocol("HTTP chunk size is not ASCII"))?;
    let size = text
        .trim_end_matches("\r\n")
        .split(';')
        .next()
        .unwrap_or_default()
        .trim();
    if size.is_empty() {
        return Err(Error::Protocol("HTTP chunk size is missing"));
    }
    u64::from_str_radix(size, 16).map_err(|_| Error::Protocol("HTTP chunk size is invalid"))
}

struct Trailer {
    raw: Vec<u8>,
    name: Vec<u8>,
    value: Vec<u8>,
}

fn parse_trailer(raw: Vec<u8>) -> Result<Trailer, Error> {
    let line = &raw[..raw.len().saturating_sub(2)];
    let colon = line
        .iter()
        .position(|byte| *byte == b':')
        .ok_or(Error::Protocol("HTTP trailer is missing a colon"))?;
    let name = &line[..colon];
    if name.is_empty() || !name.iter().all(|byte| is_tchar(*byte)) {
        return Err(Error::Protocol("HTTP trailer name is invalid"));
    }
    let value = trim_ows(&line[colon + 1..]);
    if value.iter().any(|byte| matches!(*byte, b'\r' | b'\n' | 0)) {
        return Err(Error::Protocol("HTTP trailer value is invalid"));
    }
    let name = name.to_vec();
    let value = value.to_vec();
    Ok(Trailer { raw, name, value })
}

fn forbidden_trailer(name: &[u8], connection_tokens: &[Vec<u8>]) -> bool {
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
        b"content-length".as_slice(),
        b"host".as_slice(),
    ]
    .iter()
    .any(|candidate| name.eq_ignore_ascii_case(candidate))
        || connection_tokens
            .iter()
            .any(|token| name.eq_ignore_ascii_case(token))
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
