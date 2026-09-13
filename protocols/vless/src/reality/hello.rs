// SPDX-License-Identifier: MPL-2.0
// REALITY behavior follows XTLS/REALITY 9234c772ba8f, as pinned by Xray-core v26.3.27.
//! Bounded wire inspection shared by direct and target-assisted REALITY handshakes.
use super::{
    reality_auth::{decrypt_session_id, derive_auth_key, perform_ecdh},
    reality_server_connection::RealityServerConfig,
};
use std::io;
use ztls::buf_reader::BufReader;

pub(super) fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid REALITY TLS hello")
}

pub(super) fn assembled(input: &[u8]) -> io::Result<Option<(Vec<u8>, usize)>> {
    let mut consumed = 0;
    let mut hello = Vec::new();
    while input.len() >= consumed + 5 {
        let header = &input[consumed..consumed + 5];
        let len = u16::from_be_bytes([header[3], header[4]]) as usize;
        if header[0] != 22 || len > 18432 || consumed + len > 128 * 1024 {
            return Err(invalid());
        }
        if input.len() < consumed + 5 + len {
            return Ok(None);
        }
        hello.extend_from_slice(&input[consumed + 5..consumed + 5 + len]);
        consumed += 5 + len;
        if hello.len() < 4 {
            continue;
        }
        let size = ((hello[1] as usize) << 16) | ((hello[2] as usize) << 8) | hello[3] as usize;
        if hello[0] != 1 || size > 65531 {
            return Err(invalid());
        }
        if hello.len() < size + 4 {
            continue;
        }
        if hello.len() != size + 4 {
            return Err(invalid());
        }
        let mut record = ztls::messages::write_record_header(22, hello.len() as u16);
        record.extend(hello);
        return Ok(Some((record, consumed)));
    }
    Ok(None)
}

pub(super) fn extensions(record: &[u8], client: bool) -> io::Result<Vec<(u16, &[u8])>> {
    let mut body = BufReader::new(record);
    body.skip(5 + 4 + 2 + 32)?;
    let sid = body.read_u8()? as usize;
    body.skip(sid)?;
    if client {
        let ciphers = body.read_u16_be()? as usize;
        body.skip(ciphers)?;
        let compression = body.read_u8()? as usize;
        body.skip(compression)?;
    } else {
        body.skip(3)?;
    }
    let size = body.read_u16_be()? as usize;
    let mut extensions = BufReader::new(body.read_slice(size)?);
    if !body.is_consumed() {
        return Err(invalid());
    }
    let mut result = Vec::new();
    while !extensions.is_consumed() {
        let kind = extensions.read_u16_be()?;
        let size = extensions.read_u16_be()? as usize;
        if result.iter().any(|(previous, _)| *previous == kind) {
            return Err(invalid());
        }
        result.push((kind, extensions.read_slice(size)?));
    }
    Ok(result)
}

pub(super) fn shares(record: &[u8], client: bool) -> io::Result<Vec<(u16, &[u8])>> {
    let data = extensions(record, client)?
        .into_iter()
        .find(|(kind, _)| *kind == 51)
        .ok_or_else(invalid)?
        .1;
    let mut reader = BufReader::new(data);
    if client {
        let size = reader.read_u16_be()? as usize;
        if size != reader.remaining() {
            return Err(invalid());
        }
    }
    let mut shares = Vec::new();
    while !reader.is_consumed() {
        let group = reader.read_u16_be()?;
        let size = reader.read_u16_be()? as usize;
        shares.push((group, reader.read_slice(size)?));
    }
    Ok(shares)
}

pub(super) fn authenticate(
    config: &RealityServerConfig,
    record: &[u8],
) -> io::Result<([u8; 32], [u8; 32])> {
    let ext = extensions(record, true)?;
    let versions = ext
        .iter()
        .find(|(kind, _)| *kind == 43)
        .ok_or_else(invalid)?
        .1;
    if versions.is_empty()
        || versions[0] as usize != versions.len() - 1
        || !(versions.len() - 1).is_multiple_of(2)
        || !versions[1..].chunks_exact(2).any(|v| v == [3, 4])
    {
        return Err(invalid());
    }
    let shares = shares(record, true)?;
    let public = shares
        .iter()
        .find(|(group, data)| *group == 29 && data.len() == 32)
        .map(|(_, data)| *data)
        .or_else(|| {
            shares
                .iter()
                .find(|(group, data)| *group == 4588 && data.len() == 1216)
                .map(|(_, data)| &data[1184..])
        })
        .ok_or_else(invalid)?;
    let public: [u8; 32] = public.try_into().map_err(|_| invalid())?;
    let random = ztls::util::extract_client_random(record)?;
    let session: [u8; 32] = ztls::util::extract_session_id_slice(record)?
        .try_into()
        .map_err(|_| invalid())?;
    let auth = derive_auth_key(
        &perform_ecdh(&config.private_key, &public)?,
        &random[..20],
        b"REALITY",
    )?;
    let mut aad = record[5..].to_vec();
    aad.get_mut(39..71).ok_or_else(invalid)?.fill(0);
    let session = decrypt_session_id(&session, &auth, &random[20..], &aad)?;
    let short_id: [u8; 8] = session[8..16].try_into().unwrap();
    let metadata = ztls::hello::client_hello_metadata(record)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(io::Error::other)?
        .as_millis();
    if !config.short_ids.contains(&short_id)
        || !config
            .policy
            .accepts(metadata.server_name.as_deref(), &session, now)
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "REALITY client admission rejected",
        ));
    }
    Ok((auth, public))
}
