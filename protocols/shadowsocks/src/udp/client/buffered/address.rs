use super::*;
pub(super) fn encoded_length(target: &Address) -> core::result::Result<usize, Error> {
    match target {
        Address::Ipv4(_) => Ok(5),
        Address::Ipv6(_) => Ok(17),
        Address::Domain(name) if !name.is_empty() && name.len() <= 255 => Ok(2 + name.len()),
        _ => Err(Error::Protocol("ss: invalid domain length")),
    }
}
pub(super) fn encode(
    target: &Address,
    port: u16,
    payload: &[u8],
    out: &mut [u8],
) -> core::result::Result<usize, Error> {
    let size = encoded_length(target)?;
    match target {
        Address::Ipv4(bytes) => {
            out[0] = 1;
            out[1..5].copy_from_slice(bytes);
        }
        Address::Ipv6(bytes) => {
            out[0] = 4;
            out[1..17].copy_from_slice(bytes);
        }
        Address::Domain(name) => {
            out[0] = 3;
            out[1] = name.len() as u8;
            out[2..size].copy_from_slice(name.as_bytes());
        }
    }
    out[size..size + 2].copy_from_slice(&port.to_be_bytes());
    out[size + 2..size + 2 + payload.len()].copy_from_slice(payload);
    Ok(size + 2 + payload.len())
}
pub(super) fn decode(data: &[u8], domain: &mut PacketBuffer) -> Result<(Address, u16, usize)> {
    let invalid = || protocol(Error::Protocol("ss: truncated UDP target"));
    let (target, end) = match data.first().copied() {
        Some(1) => {
            let mut ip = [0; 4];
            ip.copy_from_slice(data.get(1..5).ok_or_else(invalid)?);
            (Address::Ipv4(ip), 5)
        }
        Some(4) => {
            let mut ip = [0; 16];
            ip.copy_from_slice(data.get(1..17).ok_or_else(invalid)?);
            (Address::Ipv6(ip), 17)
        }
        Some(3) => {
            let n = *data.get(1).ok_or_else(invalid)? as usize;
            let bytes = data.get(2..2 + n).ok_or_else(invalid)?;
            core::str::from_utf8(bytes)
                .map_err(|_| protocol(Error::Protocol("ss: invalid domain UTF-8")))?;
            // Move the admitted unique allocation; never allocate a second
            // String backing store while its workspace remains allocated.
            let mut owned = domain
                .take_vec_allocation()
                .ok_or(Failure::Storage(PacketDeriveError::Unreserved))?;
            owned[..n].copy_from_slice(bytes);
            owned.truncate(n);
            let name = String::from_utf8(owned)
                .map_err(|_| protocol(Error::Protocol("ss: invalid domain UTF-8")))?;
            (Address::Domain(name), 2 + n)
        }
        _ => return Err(protocol(Error::Unsupported("ss: unknown address type"))),
    };
    let port = data.get(end..end + 2).ok_or_else(invalid)?;
    Ok((target, u16::from_be_bytes([port[0], port[1]]), end + 2))
}
