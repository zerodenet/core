//! Shared TUN TCP DNS behavior; production Source tasks use the original host.
use std::{io, sync::Arc};
use zero_stack::UserTcpStream;
pub(super) const MAX_CONCURRENT_DNS_CONNECTIONS: usize = 256;
#[cfg(any(test, not(feature = "raw-ip-runtime")))]
mod legacy;
#[cfg(not(feature = "raw-ip-runtime"))]
pub(super) use legacy::{run, TunIngressConfig};

#[cfg(any(test, not(feature = "raw-ip-runtime")))]
pub(crate) async fn serve_dns_tcp(
    resolver: Arc<zero_dns::DnsSystem>,
    stream: UserTcpStream,
) -> io::Result<()> {
    serve_dns_tcp_inner(
        resolver,
        stream,
        #[cfg(feature = "raw-ip-runtime")]
        None,
    )
    .await
}

#[cfg(feature = "raw-ip-runtime")]
pub(crate) async fn serve_dns_tcp_owned(
    resolver: Arc<zero_dns::DnsSystem>,
    stream: UserTcpStream,
    native: crate::runtime::inbound_operation::raw_ip::native_obligations::NativeObligations,
    caller: zero_platform_tokio::contract::scope::TaskJoinWitness,
) -> io::Result<()> {
    serve_dns_tcp_inner(resolver, stream, Some((native, caller))).await
}

async fn serve_dns_tcp_inner(
    resolver: Arc<zero_dns::DnsSystem>,
    mut stream: UserTcpStream,
    #[cfg(feature = "raw-ip-runtime")] owned: Option<(
        crate::runtime::inbound_operation::raw_ip::native_obligations::NativeObligations,
        zero_platform_tokio::contract::scope::TaskJoinWitness,
    )>,
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
        let (call, work) = resolver.prepare_tcp_query(&query);
        #[cfg(feature = "raw-ip-runtime")]
        let _original = match &owned {
            Some((owner, caller)) => {
                let mut original = owner.reserve_reclaimed(
                    crate::runtime::inbound_operation::raw_ip::native_obligations::ChildKind::Dns,
                    tokio::time::Instant::now() + std::time::Duration::from_secs(5),
                ).await.map_err(io::Error::other)?;
                original.bind_dns_call(call,
                    crate::runtime::inbound_operation::raw_ip::native_obligations::DnsSourceCaller::TcpConnection(caller.clone()),
                ).map_err(io::Error::other)?;
                Some(original)
            }
            None => None, // Explicit stack library/test executor only.
        };
        #[cfg(not(feature = "raw-ip-runtime"))]
        let _ = call;
        let response = work.await?;
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
