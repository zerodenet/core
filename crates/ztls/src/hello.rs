//! Bounded ClientHello extension inspection shared by custom TLS handshakes.
use crate::buf_reader::BufReader;
use std::io;

#[derive(Debug, Default)]
pub struct ClientHelloMetadata {
    pub server_name: Option<String>,
    pub alpn: Vec<String>,
}

pub fn client_hello_metadata(record: &[u8]) -> io::Result<ClientHelloMetadata> {
    let mut reader = BufReader::new(record);
    if reader.read_u8()? != 22 {
        return Err(invalid());
    }
    reader.skip(2)?;
    let record_len = reader.read_u16_be()? as usize;
    let mut hello = BufReader::new(reader.read_slice(record_len)?);
    if hello.read_u8()? != 1 {
        return Err(invalid());
    }
    let length = hello.read_u24_be()? as usize;
    let mut hello = BufReader::new(hello.read_slice(length)?);
    hello.skip(34)?;
    let length = hello.read_u8()? as usize;
    hello.skip(length)?;
    let length = hello.read_u16_be()? as usize;
    hello.skip(length)?;
    let length = hello.read_u8()? as usize;
    hello.skip(length)?;
    if hello.is_consumed() {
        return Ok(ClientHelloMetadata::default());
    }
    let length = hello.read_u16_be()? as usize;
    let mut extensions = BufReader::new(hello.read_slice(length)?);
    let mut metadata = ClientHelloMetadata::default();
    while !extensions.is_consumed() {
        let kind = extensions.read_u16_be()?;
        let length = extensions.read_u16_be()? as usize;
        let mut extension = BufReader::new(extensions.read_slice(length)?);
        if kind != 0 && kind != 16 {
            continue;
        }
        let length = extension.read_u16_be()? as usize;
        if length != extension.remaining() {
            return Err(invalid());
        }
        while !extension.is_consumed() {
            if kind == 0 {
                let name_kind = extension.read_u8()?;
                let length = extension.read_u16_be()? as usize;
                let value = extension.read_slice(length)?;
                if name_kind == 0 {
                    if metadata.server_name.is_some() || value.is_empty() {
                        return Err(invalid());
                    }
                    metadata.server_name = Some(
                        std::str::from_utf8(value)
                            .map_err(|_| invalid())?
                            .to_owned(),
                    );
                }
            } else {
                let length = extension.read_u8()? as usize;
                if length == 0 {
                    return Err(invalid());
                }
                metadata.alpn.push(extension.read_str(length)?.to_owned());
            }
        }
    }
    Ok(metadata)
}
fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "invalid TLS ClientHello metadata",
    )
}
