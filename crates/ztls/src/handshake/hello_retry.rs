//! TLS 1.3 ServerHello validation and transcript-preserving retry construction.
use crate::{buf_reader::BufReader, fingerprint::key_share::KeyShares};
use std::io;

const RETRY_RANDOM: [u8; 32] = [
    0xcf, 0x21, 0xad, 0x74, 0xe5, 0x9a, 0x61, 0x11, 0xbe, 0x1d, 0x8c, 0x02, 0x1e, 0x65, 0xb8, 0x91,
    0xc2, 0xa2, 0x11, 0x16, 0x7a, 0xbb, 0x8c, 0x5e, 0x07, 0x9e, 0x09, 0xe2, 0xc8, 0xa8, 0x33, 0x9c,
];

pub(super) struct ServerHello {
    pub retry: bool,
    pub suite: u16,
    pub group: Option<u16>,
    pub cookie: Option<Vec<u8>>,
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

pub(super) fn parse(record: &[u8], offered: &[u8]) -> io::Result<ServerHello> {
    let mut r = BufReader::new(record);
    if r.read_u8()? != 22 || r.read_u16_be()? != 0x0303 {
        return Err(invalid("invalid ServerHello record"));
    }
    let n = r.read_u16_be()? as usize;
    let mut hello = BufReader::new(r.read_slice(n)?);
    if !r.is_consumed() || hello.read_u8()? != 2 {
        return Err(invalid("expected ServerHello"));
    }
    let n = hello.read_u24_be()? as usize;
    if n != hello.remaining() || hello.read_u16_be()? != 0x0303 {
        return Err(invalid("invalid ServerHello length or legacy version"));
    }
    let retry = hello.read_slice(32)? == RETRY_RANDOM;
    let n = hello.read_u8()? as usize;
    let session = hello.read_slice(n)?;
    let mut client = BufReader::new(offered);
    client.skip(38)?;
    let n = client.read_u8()? as usize;
    if session != client.read_slice(n)? {
        return Err(invalid("ServerHello session ID mismatch"));
    }
    let suite = hello.read_u16_be()?;
    let (suites, _) = crate::fingerprint::wire::parts(offered)?;
    if !matches!(suite, 0x1301..=0x1303) || !suites.contains(&suite) || hello.read_u8()? != 0 {
        return Err(invalid("invalid ServerHello suite or compression"));
    }
    let n = hello.read_u16_be()? as usize;
    let mut extensions = BufReader::new(hello.read_slice(n)?);
    if !hello.is_consumed() {
        return Err(invalid("trailing ServerHello data"));
    }
    let mut seen = std::collections::HashSet::new();
    let (mut version, mut group, mut cookie) = (false, None, None);
    while !extensions.is_consumed() {
        let kind = extensions.read_u16_be()?;
        let n = extensions.read_u16_be()? as usize;
        let bytes = extensions.read_slice(n)?;
        if !seen.insert(kind) {
            return Err(invalid("duplicate ServerHello extension"));
        }
        match kind {
            43 if bytes == [3, 4] => version = true,
            51 if retry && bytes.len() == 2 => {
                group = Some(u16::from_be_bytes([bytes[0], bytes[1]]));
            }
            51 if !retry && bytes.len() >= 4 => {}
            44 if retry => {
                let mut r = BufReader::new(bytes);
                let n = r.read_u16_be()? as usize;
                if n == 0 || n != r.remaining() {
                    return Err(invalid("invalid HelloRetryRequest cookie"));
                }
                cookie = Some(bytes.to_vec());
            }
            _ => return Err(invalid("unexpected ServerHello extension")),
        }
    }
    if !version || (!retry && !seen.contains(&51)) || (retry && group.is_none() && cookie.is_none())
    {
        return Err(invalid("incomplete ServerHello or HelloRetryRequest"));
    }
    Ok(ServerHello {
        retry,
        suite,
        group,
        cookie,
    })
}

pub(super) fn rebuild(
    original: &[u8],
    retry: &ServerHello,
    keys: &mut KeyShares,
    x25519: &[u8],
) -> io::Result<Vec<u8>> {
    let (_, mut extensions) = crate::fingerprint::wire::parts(original)?;
    if let Some(group) = retry.group {
        let supported = extensions
            .iter()
            .find(|(k, _)| *k == 10)
            .ok_or_else(|| invalid("missing supported groups"))?;
        let mut r = BufReader::new(&supported.1);
        let n = r.read_u16_be()? as usize;
        if n != r.remaining()
            || !n.is_multiple_of(2)
            || !r
                .read_slice(n)?
                .as_chunks::<2>()
                .0
                .iter()
                .any(|g| u16::from_be_bytes([g[0], g[1]]) == group)
        {
            return Err(invalid("retry selected an unoffered group"));
        }
        let share = extensions
            .iter_mut()
            .find(|(k, _)| *k == 51)
            .ok_or_else(|| invalid("missing ClientHello key shares"))?;
        let mut r = BufReader::new(&share.1);
        let n = r.read_u16_be()? as usize;
        if n != r.remaining() {
            return Err(invalid("invalid ClientHello key shares"));
        }
        while !r.is_consumed() {
            let previous = r.read_u16_be()?;
            let n = r.read_u16_be()? as usize;
            r.skip(n)?;
            if previous == group {
                return Err(invalid("retry selected an already offered key share"));
            }
        }
        keys.reset_for_retry();
        let public = keys
            .offer(group, x25519)?
            .ok_or_else(|| invalid("retry group unavailable"))?;
        let mut data = Vec::new();
        data.extend_from_slice(&((public.len() + 4) as u16).to_be_bytes());
        data.extend_from_slice(&group.to_be_bytes());
        data.extend_from_slice(&(public.len() as u16).to_be_bytes());
        data.extend_from_slice(&public);
        share.1 = data;
    }
    extensions.retain(|(kind, _)| *kind != 42 && *kind != 44);
    if let Some(cookie) = &retry.cookie {
        extensions.push((44, cookie.clone()));
    }
    let mut prefix = BufReader::new(original);
    prefix.skip(38)?;
    let n = prefix.read_u8()? as usize;
    prefix.skip(n)?;
    let n = prefix.read_u16_be()? as usize;
    prefix.skip(n)?;
    let n = prefix.read_u8()? as usize;
    prefix.skip(n)?;
    let mut wire = original[..prefix.position()].to_vec();
    let mut encoded = Vec::new();
    for (kind, data) in extensions {
        encoded.extend_from_slice(&kind.to_be_bytes());
        encoded.extend_from_slice(&(data.len() as u16).to_be_bytes());
        encoded.extend_from_slice(&data);
    }
    let n = u16::try_from(encoded.len()).map_err(|_| invalid("retry ClientHello too large"))?;
    wire.extend_from_slice(&n.to_be_bytes());
    wire.extend_from_slice(&encoded);
    let n = (wire.len() - 4) as u32;
    wire[1..4].copy_from_slice(&n.to_be_bytes()[1..]);
    Ok(wire)
}

pub(super) fn offered_share(hello: &[u8], selected: u16) -> io::Result<bool> {
    let (_, extensions) = crate::fingerprint::wire::parts(hello)?;
    let (_, share) = extensions
        .iter()
        .find(|(kind, _)| *kind == 51)
        .ok_or_else(|| invalid("missing ClientHello key shares"))?;
    let mut r = BufReader::new(share);
    let n = r.read_u16_be()? as usize;
    if n != r.remaining() {
        return Err(invalid("invalid ClientHello key shares"));
    }
    while !r.is_consumed() {
        let group = r.read_u16_be()?;
        let n = r.read_u16_be()? as usize;
        r.skip(n)?;
        if group == selected {
            return Ok(true);
        }
    }
    Ok(false)
}
