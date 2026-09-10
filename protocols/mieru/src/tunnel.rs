use alloc::string::String;
use alloc::vec::Vec;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use zero_core::{Address, Error, Network, ProtocolType, Session};

#[derive(Debug, Clone, PartialEq, Eq)]
enum MieruTunnelRequest {
    Tcp { target: Address, port: u16 },
    UdpAssociate { target: Address, port: u16 },
}

impl MieruTunnelRequest {
    fn into_session(self) -> Session {
        match self {
            Self::Tcp { target, port } => {
                Session::new(0, target, port, Network::Tcp, ProtocolType::new("mieru"))
            }
            Self::UdpAssociate { target, port } => {
                Session::new(0, target, port, Network::Udp, ProtocolType::new("mieru"))
            }
        }
    }
}

pub(crate) async fn accept_tunneled_session<S>(stream: &mut S) -> Result<Session, Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let request = read_request(stream).await?;
    write_success_response(stream).await?;
    Ok(request.into_session())
}

pub(crate) async fn request_tcp_connect<S>(
    stream: &mut S,
    target: &Address,
    port: u16,
) -> Result<(), Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    write_request(stream, 0x01, target, port).await?;
    read_success_response(stream).await
}

pub(crate) fn build_udp_associate_request() -> Result<Vec<u8>, Error> {
    build_request(0x03, &Address::Ipv4([0, 0, 0, 0]), 0)
}

pub(crate) fn validate_success_response(response: &[u8]) -> Result<(), Error> {
    if response.len() < 4 {
        return Err(Error::Protocol("mieru socks5: reply is too short"));
    }
    if response[0] != 0x05 {
        return Err(Error::Protocol("mieru socks5: bad reply version"));
    }
    if response[1] != 0x00 {
        return Err(Error::Protocol("mieru socks5: connect rejected"));
    }
    let mut offset = 4usize;
    offset += match response[3] {
        0x01 => 4,
        0x04 => 16,
        0x03 => {
            if response.len() < offset + 1 {
                return Err(Error::Protocol("mieru socks5: truncated domain length"));
            }
            1 + response[offset] as usize
        }
        _ => return Err(Error::Protocol("mieru socks5: bad BND address type")),
    };
    if response.len() < offset + 2 {
        return Err(Error::Protocol("mieru socks5: truncated BND response"));
    }
    Ok(())
}

async fn read_request<S>(stream: &mut S) -> Result<MieruTunnelRequest, Error>
where
    S: AsyncRead + Unpin,
{
    let mut head = [0u8; 4];
    stream
        .read_exact(&mut head)
        .await
        .map_err(|_| Error::Io("mieru socks5: read request header"))?;

    if head[0] != 0x05 {
        return Err(Error::Protocol("mieru socks5: bad request version"));
    }

    let target = read_address(stream, head[3]).await?;

    let mut port_bytes = [0u8; 2];
    stream
        .read_exact(&mut port_bytes)
        .await
        .map_err(|_| Error::Io("mieru socks5: read request port"))?;
    let port = u16::from_be_bytes(port_bytes);

    match head[1] {
        0x01 => Ok(MieruTunnelRequest::Tcp { target, port }),
        0x03 => Ok(MieruTunnelRequest::UdpAssociate { target, port }),
        _ => Err(Error::Unsupported("mieru socks5: unsupported command")),
    }
}

async fn write_request<S>(
    stream: &mut S,
    command: u8,
    target: &Address,
    port: u16,
) -> Result<(), Error>
where
    S: AsyncWrite + Unpin,
{
    let req = build_request(command, target, port)?;
    stream
        .write_all(&req)
        .await
        .map_err(|_| Error::Io("mieru socks5: write request"))?;
    stream
        .flush()
        .await
        .map_err(|_| Error::Io("mieru socks5: flush request"))?;
    Ok(())
}

fn build_request(command: u8, target: &Address, port: u16) -> Result<Vec<u8>, Error> {
    let mut req = Vec::from([0x05, command, 0x00]);
    encode_address(target, &mut req)?;
    req.extend_from_slice(&port.to_be_bytes());
    Ok(req)
}

async fn read_success_response<S>(stream: &mut S) -> Result<(), Error>
where
    S: AsyncRead + Unpin,
{
    let mut head = [0u8; 4];
    stream
        .read_exact(&mut head)
        .await
        .map_err(|_| Error::Io("mieru socks5: read response"))?;
    if head[0] != 0x05 {
        return Err(Error::Protocol("mieru socks5: bad reply version"));
    }
    if head[1] != 0x00 {
        return Err(Error::Protocol("mieru socks5: connect rejected"));
    }

    discard_address(stream, head[3]).await?;

    let mut bind_port = [0u8; 2];
    stream
        .read_exact(&mut bind_port)
        .await
        .map_err(|_| Error::Io("mieru socks5: read BND port"))?;
    Ok(())
}

async fn write_success_response<S>(stream: &mut S) -> Result<(), Error>
where
    S: AsyncWrite + Unpin,
{
    stream
        .write_all(&[0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
        .await
        .map_err(|_| Error::Io("mieru socks5: write success"))?;
    stream
        .flush()
        .await
        .map_err(|_| Error::Io("mieru socks5: flush success"))?;
    Ok(())
}

async fn read_address<S>(stream: &mut S, atyp: u8) -> Result<Address, Error>
where
    S: AsyncRead + Unpin,
{
    match atyp {
        0x01 => {
            let mut ip = [0u8; 4];
            stream
                .read_exact(&mut ip)
                .await
                .map_err(|_| Error::Io("mieru socks5: read ipv4 address"))?;
            Ok(Address::Ipv4(ip))
        }
        0x04 => {
            let mut ip = [0u8; 16];
            stream
                .read_exact(&mut ip)
                .await
                .map_err(|_| Error::Io("mieru socks5: read ipv6 address"))?;
            Ok(Address::Ipv6(ip))
        }
        0x03 => {
            let mut len = [0u8; 1];
            stream
                .read_exact(&mut len)
                .await
                .map_err(|_| Error::Io("mieru socks5: read domain length"))?;
            let mut domain = vec![0u8; len[0] as usize];
            stream
                .read_exact(&mut domain)
                .await
                .map_err(|_| Error::Io("mieru socks5: read domain"))?;
            let domain = String::from_utf8(domain)
                .map_err(|_| Error::Protocol("mieru socks5: invalid domain"))?;
            Ok(Address::Domain(domain))
        }
        _ => Err(Error::Protocol("mieru socks5: bad address type")),
    }
}

fn encode_address(address: &Address, buf: &mut Vec<u8>) -> Result<(), Error> {
    match address {
        Address::Ipv4(ip) => {
            buf.push(0x01);
            buf.extend_from_slice(ip);
        }
        Address::Ipv6(ip) => {
            buf.push(0x04);
            buf.extend_from_slice(ip);
        }
        Address::Domain(domain) => {
            let bytes = domain.as_bytes();
            if bytes.len() > 255 {
                return Err(Error::Protocol("mieru socks5: domain too long"));
            }
            buf.push(0x03);
            buf.push(bytes.len() as u8);
            buf.extend_from_slice(bytes);
        }
    }
    Ok(())
}

async fn discard_address<S>(stream: &mut S, atyp: u8) -> Result<(), Error>
where
    S: AsyncRead + Unpin,
{
    let len = match atyp {
        0x01 => 4,
        0x04 => 16,
        0x03 => {
            let mut len = [0u8; 1];
            stream
                .read_exact(&mut len)
                .await
                .map_err(|_| Error::Io("mieru socks5: read domain length"))?;
            len[0] as usize
        }
        _ => return Err(Error::Protocol("mieru socks5: bad BND address type")),
    };
    let mut addr = vec![0u8; len];
    stream
        .read_exact(&mut addr)
        .await
        .map_err(|_| Error::Io("mieru socks5: read BND address"))?;
    Ok(())
}

pub(crate) async fn request_udp_associate<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
) -> Result<(), Error> {
    write_request(stream, 3, &Address::Ipv4([0; 4]), 0).await?;
    read_success_response(stream).await
}
