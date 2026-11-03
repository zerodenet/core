//! HAProxy PROXY protocol stream preambles. No proxy-protocol payload framing.
use std::{io, net::SocketAddr};
mod read;
pub use read::accept;
pub fn encode(
    version: u8,
    source: Option<SocketAddr>,
    destination: Option<SocketAddr>,
) -> io::Result<Vec<u8>> {
    match version {
        0 => Ok(Vec::new()),
        1 => Ok(match (source, destination) {
            (Some(SocketAddr::V4(a)), Some(SocketAddr::V4(b))) => format!(
                "PROXY TCP4 {} {} {} {}\r\n",
                a.ip(),
                b.ip(),
                a.port(),
                b.port()
            )
            .into_bytes(),
            (Some(SocketAddr::V6(a)), Some(SocketAddr::V6(b))) => format!(
                "PROXY TCP6 {} {} {} {}\r\n",
                a.ip(),
                b.ip(),
                a.port(),
                b.port()
            )
            .into_bytes(),
            _ => b"PROXY UNKNOWN\r\n".to_vec(),
        }),
        2 => {
            let mut header = b"\r\n\r\n\0\r\nQUIT\n".to_vec();
            header.push(0x21);
            match (source, destination) {
                (Some(SocketAddr::V4(a)), Some(SocketAddr::V4(b))) => {
                    header.extend_from_slice(&[0x11, 0, 12]);
                    header.extend_from_slice(&a.ip().octets());
                    header.extend_from_slice(&b.ip().octets());
                    header.extend_from_slice(&a.port().to_be_bytes());
                    header.extend_from_slice(&b.port().to_be_bytes());
                }
                (Some(SocketAddr::V6(a)), Some(SocketAddr::V6(b))) => {
                    header.extend_from_slice(&[0x21, 0, 36]);
                    header.extend_from_slice(&a.ip().octets());
                    header.extend_from_slice(&b.ip().octets());
                    header.extend_from_slice(&a.port().to_be_bytes());
                    header.extend_from_slice(&b.port().to_be_bytes());
                }
                _ => {
                    header[12] = 0x20;
                    header.extend_from_slice(&[0, 0, 0]);
                }
            }
            Ok(header)
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "PROXY protocol version must be 0, 1 or 2",
        )),
    }
}

/// Execute the required neutral prelude before TLS/carrier preparation. The
/// returned facts retain the actual physical peer independently of its trusted
/// effective source; only the host may apply those facts to admission.
pub async fn prepare(
    socket: &mut zero_platform_tokio::TokioSocket,
) -> io::Result<zero_platform_tokio::contract::context::InboundConnectionFacts> {
    let mut facts =
        zero_platform_tokio::contract::context::InboundConnectionFacts::from_stream(socket);
    let addresses = tokio::time::timeout(std::time::Duration::from_secs(10), accept(socket))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "PROXY protocol header timeout"))??;
    if let Some((source, destination)) = addresses {
        socket.set_effective_addresses(source, destination);
        facts.effective_source = Some(source);
        facts.local_endpoint = Some(destination);
    }
    Ok(facts)
}
