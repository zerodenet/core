use std::io;

pub(crate) fn normalize_domain(domain: &str) -> io::Result<String> {
    let domain = domain.trim().trim_end_matches('.');
    if domain.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "DNS name must not be empty",
        ));
    }
    let ascii = idna::domain_to_ascii(domain).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid internationalized DNS name `{domain}`: {error}"),
        )
    })?;
    let normalized = ascii.to_ascii_lowercase();
    if normalized.len() > 253
        || normalized.split('.').any(|label| {
            label.is_empty()
                || label.len() > 63
                || label.starts_with('-')
                || label.ends_with('-')
                || !label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        })
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid DNS name `{domain}`"),
        ));
    }
    Ok(normalized)
}

pub(super) fn encode_name(domain: &str, output: &mut Vec<u8>) -> io::Result<()> {
    let normalized = normalize_domain(domain)?;
    for label in normalized.split('.') {
        output.push(label.len() as u8);
        output.extend_from_slice(label.as_bytes());
    }
    output.push(0);
    Ok(())
}

pub(super) fn decode_name(data: &[u8], start: usize) -> io::Result<(String, usize)> {
    let mut labels = Vec::new();
    let mut offset = start;
    let mut next = None;
    let mut hops = 0_usize;
    loop {
        let length = *data
            .get(offset)
            .ok_or_else(|| invalid("truncated DNS name"))?;
        if length & 0xc0 == 0xc0 {
            let low = *data
                .get(offset + 1)
                .ok_or_else(|| invalid("truncated DNS compression pointer"))?;
            let pointer = (((length & 0x3f) as usize) << 8) | low as usize;
            if pointer >= data.len() {
                return Err(invalid("DNS compression pointer is out of bounds"));
            }
            next.get_or_insert(offset + 2);
            offset = pointer;
            hops += 1;
            if hops > 128 {
                return Err(invalid("DNS compression pointer loop"));
            }
            continue;
        }
        if length & 0xc0 != 0 {
            return Err(invalid("invalid DNS label length"));
        }
        offset += 1;
        if length == 0 {
            let name = labels.join(".");
            return Ok((name, next.unwrap_or(offset)));
        }
        let end = offset
            .checked_add(length as usize)
            .ok_or_else(|| invalid("DNS label length overflow"))?;
        let label = data
            .get(offset..end)
            .ok_or_else(|| invalid("truncated DNS label"))?;
        let label = std::str::from_utf8(label)
            .map_err(|_| invalid("DNS label is not valid ASCII/UTF-8"))?;
        labels.push(label.to_owned());
        offset = end;
    }
}

pub(super) fn skip_name(data: &[u8], offset: usize) -> io::Result<usize> {
    decode_name(data, offset).map(|(_, next)| next)
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
