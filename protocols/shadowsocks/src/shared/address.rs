use super::*;
// Address encode / decode.

pub fn encode_address(addr: &Address) -> Result<Vec<u8>, Error> {
    match addr {
        Address::Ipv4(bytes) => {
            let mut buf = Vec::with_capacity(5);
            buf.push(ADDR_TYPE_IPV4);
            buf.extend_from_slice(bytes);
            Ok(buf)
        }
        Address::Ipv6(bytes) => {
            let mut buf = Vec::with_capacity(17);
            buf.push(ADDR_TYPE_IPV6);
            buf.extend_from_slice(bytes);
            Ok(buf)
        }
        Address::Domain(domain) => {
            let b = domain.as_bytes();
            if b.is_empty() || b.len() > u8::MAX as usize {
                return Err(Error::Protocol("ss: invalid domain length"));
            }
            let mut buf = Vec::with_capacity(2 + b.len());
            buf.push(ADDR_TYPE_DOMAIN);
            buf.push(b.len() as u8);
            buf.extend_from_slice(b);
            Ok(buf)
        }
    }
}

pub fn decode_address(data: &[u8]) -> Result<(Address, usize), Error> {
    if data.is_empty() {
        return Err(Error::Protocol("ss: empty address data"));
    }
    match data[0] {
        ADDR_TYPE_IPV4 => {
            if data.len() < 5 {
                return Err(Error::Protocol("ss: truncated IPv4"));
            }
            let mut bytes = [0u8; 4];
            bytes.copy_from_slice(&data[1..5]);
            Ok((Address::Ipv4(bytes), 5))
        }
        ADDR_TYPE_IPV6 => {
            if data.len() < 17 {
                return Err(Error::Protocol("ss: truncated IPv6"));
            }
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(&data[1..17]);
            Ok((Address::Ipv6(bytes), 17))
        }
        ADDR_TYPE_DOMAIN => {
            let len = *data
                .get(1)
                .ok_or(Error::Protocol("ss: truncated domain length"))?
                as usize;
            if data.len() < 2 + len {
                return Err(Error::Protocol("ss: truncated domain"));
            }
            let domain = String::from_utf8(data[2..2 + len].to_vec())
                .map_err(|_| Error::Protocol("ss: invalid domain UTF-8"))?;
            Ok((Address::Domain(domain), 2 + len))
        }
        _ => Err(Error::Unsupported("ss: unknown address type")),
    }
}

/// Build target + payload bytes. Format: [addr][port:2][payload]
pub fn build_target_data(addr: &Address, port: u16, payload: &[u8]) -> Result<Vec<u8>, Error> {
    let addr_bytes = encode_address(addr)?;
    let mut buf = Vec::with_capacity(addr_bytes.len() + 2 + payload.len());
    buf.extend_from_slice(&addr_bytes);
    buf.extend_from_slice(&port.to_be_bytes());
    buf.extend_from_slice(payload);
    Ok(buf)
}

/// Parse target + payload bytes. Returns (address, port, remaining payload offset).
pub fn parse_target_data(data: &[u8]) -> Result<(Address, u16, usize), Error> {
    let (addr, addr_end) = decode_address(data)?;
    if data.len() < addr_end + 2 {
        return Err(Error::Protocol("ss: truncated port"));
    }
    let port = u16::from_be_bytes([data[addr_end], data[addr_end + 1]]);
    Ok((addr, port, addr_end + 2))
}

/// Read exact number of bytes from stream.
pub async fn read_exact<S: AsyncSocket>(stream: &mut S, buf: &mut [u8]) -> Result<(), Error> {
    let mut offset = 0;
    while offset < buf.len() {
        let n = stream
            .read(&mut buf[offset..])
            .await
            .map_err(|_| Error::Io("ss: read failed"))?;
        if n == 0 {
            return Err(Error::Io("ss: unexpected EOF"));
        }
        offset += n;
    }
    Ok(())
}
