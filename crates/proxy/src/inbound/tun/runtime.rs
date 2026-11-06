//! Shared TUN TCP DNS behavior; production Source tasks use the original host.
use std::{io, sync::Arc};
use zero_stack::UserTcpStream;
pub(super) const MAX_CONCURRENT_DNS_CONNECTIONS: usize = 256;
#[cfg(any(test, not(feature = "raw-ip-runtime")))]
mod legacy;
#[cfg(not(feature = "raw-ip-runtime"))]
pub(super) use legacy::{run, TunIngressConfig};

pub(crate) async fn serve_dns_tcp(
    resolver: Arc<zero_dns::DnsSystem>,
    mut stream: UserTcpStream,
) -> io::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    loop {
        let mut length = [0_u8; 2];
        match stream.read_exact(&mut length).await {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(error) => return Err(error),
        }
        let length = u16::from_be_bytes(length) as usize;
        if length == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "zero-length DNS-over-TCP query",
            ));
        }
        let mut query = vec![0_u8; length];
        stream.read_exact(&mut query).await?;
        let response = resolver.answer_tcp_query(&query).await?;
        let response_length: u16 = response.len().try_into().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "DNS-over-TCP response is too large",
            )
        })?;
        stream.write_all(&response_length.to_be_bytes()).await?;
        stream.write_all(&response).await?;
        stream.flush().await?;
    }
}
