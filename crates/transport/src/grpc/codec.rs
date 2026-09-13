use std::io;
pub(super) fn encode_grpc_hunk(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + varint_len(payload.len() as u64) + payload.len());
    out.push(0x0a);
    write_varint(payload.len() as u64, &mut out);
    out.extend_from_slice(payload);
    out
}

pub(super) fn decode_grpc_hunk(mut input: &[u8], multi: bool) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    while !input.is_empty() {
        let (field, wire_type) = tag(&mut input)?;
        if field == 1 && wire_type == 2 {
            let payload = bytes(&mut input)?;
            if !multi {
                out.clear();
            }
            out.extend_from_slice(payload);
        } else {
            skip(&mut input, field, wire_type)?;
        }
    }
    Ok(out)
}

fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "invalid or truncated gRPC protobuf field",
    )
}
fn tag(input: &mut &[u8]) -> io::Result<(u64, u64)> {
    let key = read_varint(input)?;
    let field = key >> 3;
    if field == 0 || field > (1 << 29) - 1 {
        return Err(invalid());
    }
    Ok((field, key & 7))
}
fn take<'a>(input: &mut &'a [u8], size: usize) -> io::Result<&'a [u8]> {
    let (value, rest) = input.split_at_checked(size).ok_or_else(invalid)?;
    *input = rest;
    Ok(value)
}
fn bytes<'a>(input: &mut &'a [u8]) -> io::Result<&'a [u8]> {
    let size = usize::try_from(read_varint(input)?).map_err(|_| invalid())?;
    take(input, size)
}
fn scalar(input: &mut &[u8], wire: u64) -> io::Result<()> {
    match wire {
        0 => {
            read_varint(input)?;
        }
        1 => {
            take(input, 8)?;
        }
        2 => {
            bytes(input)?;
        }
        5 => {
            take(input, 4)?;
        }
        _ => return Err(invalid()),
    }
    Ok(())
}
fn skip(input: &mut &[u8], field: u64, wire: u64) -> io::Result<()> {
    if wire != 3 {
        return scalar(input, wire);
    }
    // Unknown protobuf groups may contain nested groups. Keep this iterative so
    // untrusted nesting cannot exhaust a runtime worker's call stack.
    let mut groups = vec![field];
    while !groups.is_empty() {
        let (field, wire) = tag(input)?;
        match wire {
            3 => {
                if groups.len() >= 10_000 {
                    return Err(invalid());
                }
                groups.push(field);
            }
            4 => {
                if groups.pop() != Some(field) {
                    return Err(invalid());
                }
            }
            _ => scalar(input, wire)?,
        }
    }
    Ok(())
}

pub(super) fn write_varint(mut value: u64, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

pub(super) fn read_varint(input: &mut &[u8]) -> io::Result<u64> {
    let mut value = 0_u64;
    for shift in (0..64).step_by(7) {
        let Some((&byte, rest)) = input.split_first() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "grpc protobuf varint truncated",
            ));
        };
        *input = rest;
        if shift == 63 && byte > 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "protobuf varint overflow",
            ));
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "grpc protobuf varint too long",
    ))
}

pub(super) fn varint_len(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}
