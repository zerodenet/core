use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;

use zero_core::{Address, Error, Session};
use zero_traits::AsyncSocket;

pub const VLESS_VERSION: u8 = 0x00;

pub(crate) const CMD_TCP: u8 = 0x01;
pub(crate) const CMD_UDP: u8 = 0x02;
pub(crate) const CMD_MUX: u8 = 0x03;

pub(crate) const ATYP_IPV4: u8 = 0x01;
pub(crate) const ATYP_DOMAIN: u8 = 0x02;
pub(crate) const ATYP_IPV6: u8 = 0x03;

pub(crate) async fn read_exact<S>(stream: &mut S, buf: &mut [u8]) -> Result<(), Error>
where
    S: AsyncSocket,
{
    let mut offset = 0;

    while offset < buf.len() {
        let read = stream
            .read(&mut buf[offset..])
            .await
            .map_err(|_| Error::Io("failed to read from socket"))?;

        if read == 0 {
            return Err(Error::Io("unexpected EOF while reading socket"));
        }

        offset += read;
    }

    Ok(())
}

#[cfg(not(feature = "reality"))]
pub(crate) async fn read_addon<S>(stream: &mut S) -> Result<(), Error>
where
    S: AsyncSocket,
{
    read_addon_len(stream).await.map(|_| ())
}

pub(crate) async fn read_addon_len<S>(stream: &mut S) -> Result<usize, Error>
where
    S: AsyncSocket,
{
    let mut length = [0_u8; 1];
    read_exact(stream, &mut length).await?;
    let length = length[0] as usize;
    if length == 0 {
        return Ok(1);
    }

    let mut addon = vec![0_u8; length];
    read_exact(stream, &mut addon).await?;
    Ok(1 + length)
}

pub(crate) async fn read_address<S>(stream: &mut S, atyp: u8) -> Result<Address, Error>
where
    S: AsyncSocket,
{
    match atyp {
        ATYP_IPV4 => {
            let mut bytes = [0_u8; 4];
            read_exact(stream, &mut bytes).await?;
            Ok(Address::Ipv4(bytes))
        }
        ATYP_DOMAIN => {
            let mut length = [0_u8; 1];
            read_exact(stream, &mut length).await?;

            let domain_length = length[0] as usize;
            if domain_length == 0 {
                return Err(Error::Protocol("VLESS domain must not be empty"));
            }

            let mut domain = vec![0_u8; domain_length];
            read_exact(stream, &mut domain).await?;

            let domain = String::from_utf8(domain)
                .map_err(|_| Error::Protocol("VLESS domain is not valid UTF-8"))?;
            Ok(Address::Domain(domain))
        }
        ATYP_IPV6 => {
            let mut bytes = [0_u8; 16];
            read_exact(stream, &mut bytes).await?;
            Ok(Address::Ipv6(bytes))
        }
        _ => Err(Error::Unsupported("VLESS address type is not supported")),
    }
}

pub(crate) fn write_address(buf: &mut Vec<u8>, address: &Address) -> Result<(), Error> {
    match address {
        Address::Ipv4(bytes) => {
            buf.push(ATYP_IPV4);
            buf.extend_from_slice(bytes);
        }
        Address::Ipv6(bytes) => {
            buf.push(ATYP_IPV6);
            buf.extend_from_slice(bytes);
        }
        Address::Domain(domain) => {
            let bytes = domain.as_bytes();
            if bytes.is_empty() {
                return Err(Error::Protocol("VLESS domain must not be empty"));
            }
            if bytes.len() > u8::MAX as usize {
                return Err(Error::Unsupported("VLESS domain is too long"));
            }

            buf.push(ATYP_DOMAIN);
            buf.push(bytes.len() as u8);
            buf.extend_from_slice(bytes);
        }
    }

    Ok(())
}

pub(crate) fn build_request(
    session: &Session,
    id: &[u8; 16],
    command: u8,
) -> Result<Vec<u8>, Error> {
    build_request_to(&session.target, session.port, id, command)
}

pub(crate) fn build_request_to(
    target: &Address,
    port: u16,
    id: &[u8; 16],
    command: u8,
) -> Result<Vec<u8>, Error> {
    let mut request = Vec::with_capacity(24);
    request.push(VLESS_VERSION);
    request.extend_from_slice(id);
    request.push(0x00);
    request.push(command);
    request.extend_from_slice(&port.to_be_bytes());
    write_address(&mut request, target)?;
    Ok(request)
}

pub(crate) async fn read_response<S>(stream: &mut S) -> Result<(), Error>
where
    S: AsyncSocket,
{
    read_response_len(stream).await.map(|_| ())
}

pub(crate) async fn read_response_len<S>(stream: &mut S) -> Result<usize, Error>
where
    S: AsyncSocket,
{
    let mut version = [0_u8; 1];
    read_exact(stream, &mut version).await?;
    if version[0] != VLESS_VERSION {
        return Err(Error::Protocol("unsupported VLESS response version"));
    }

    Ok(1 + read_addon_len(stream).await?)
}

pub(crate) use crate::uuid::parse_uuid;
