use alloc::vec::Vec;

use zero_core::Error;

use super::{is_vision_flow, is_zero_aead_flow, parse_flow, FLOW_XTLS_RPRX_VISION};

/// Encodes the standard VLESS Addons protobuf, including its one-byte length.
pub fn encode_addons(flow: Option<&str>) -> Result<Vec<u8>, Error> {
    let mut encoded = Vec::new();
    if is_vision_flow(flow) {
        encoded.push(0x0a); // field 1 (Flow), wire type 2
        encoded.push(FLOW_XTLS_RPRX_VISION.len() as u8);
        encoded.extend_from_slice(FLOW_XTLS_RPRX_VISION.as_bytes());
    } else if flow.is_some() && !is_zero_aead_flow(flow) {
        return Err(Error::Unsupported("VLESS flow is not supported"));
    }
    if encoded.len() > u8::MAX as usize {
        return Err(Error::Protocol("VLESS addons are too large"));
    }
    let mut framed = Vec::with_capacity(encoded.len() + 1);
    framed.push(encoded.len() as u8);
    framed.extend_from_slice(&encoded);
    Ok(framed)
}

pub fn decode_addons(encoded: &[u8]) -> Result<Option<&'static str>, Error> {
    let mut offset = 0;
    let mut flow = None;
    while offset < encoded.len() {
        let key = read_varint(encoded, &mut offset)?;
        let field = key >> 3;
        let wire = key & 0x07;
        match (field, wire) {
            (1, 2) => {
                let len = read_varint(encoded, &mut offset)? as usize;
                let end = checked_field_end(encoded, offset, len)?;
                let value = core::str::from_utf8(&encoded[offset..end])
                    .map_err(|_| Error::Protocol("VLESS addons flow is not UTF-8"))?;
                flow = Some(parse_flow(value)?);
                offset = end;
            }
            (_, 0) => {
                let _ = read_varint(encoded, &mut offset)?;
            }
            (_, 1) => offset = checked_field_end(encoded, offset, 8)?,
            (_, 2) => {
                let len = read_varint(encoded, &mut offset)? as usize;
                offset = checked_field_end(encoded, offset, len)?;
            }
            (_, 5) => offset = checked_field_end(encoded, offset, 4)?,
            _ => return Err(Error::Protocol("invalid VLESS addons protobuf wire type")),
        }
    }
    Ok(flow)
}

fn checked_field_end(input: &[u8], offset: usize, len: usize) -> Result<usize, Error> {
    offset
        .checked_add(len)
        .filter(|end| *end <= input.len())
        .ok_or(Error::Protocol("truncated VLESS addons field"))
}

fn read_varint(input: &[u8], offset: &mut usize) -> Result<u64, Error> {
    let mut value = 0_u64;
    for shift in (0..70).step_by(7) {
        let byte = *input
            .get(*offset)
            .ok_or(Error::Protocol("truncated VLESS addons varint"))?;
        *offset += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(Error::Protocol("VLESS addons varint is too large"))
}
