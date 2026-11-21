//! TUN TCP DNS runs in the original Source host and retains each DNS call.
#[cfg(feature = "raw-ip-runtime")]
use std::{io, sync::Arc};
#[cfg(feature = "raw-ip-runtime")]
use zero_stack::UserTcpStream;
#[cfg(test)]
#[path = "runtime/tests/stack_fixture.rs"]
mod stack_fixture;

#[cfg(feature = "raw-ip-runtime")]
pub(crate) async fn serve_dns_tcp_owned(
    resolver: Arc<zero_dns::DnsSystem>,
    mut stream: UserTcpStream,
    native: crate::runtime::inbound_operation::raw_ip::native_obligations::NativeObligations,
    caller: zero_platform_tokio::contract::scope::TaskJoinWitness,
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
        let mut query = resolver.prepare_wire_input(length)?;
        stream.read_exact(query.as_mut_slice()).await?;
        let (call, work) = resolver.prepare_tcp_query(&query);
        let mut at_capacity = false;
        let _original = match native
            .reserve_reclaimed(
                crate::runtime::inbound_operation::raw_ip::native_obligations::ChildKind::Dns,
                tokio::time::Instant::now() + std::time::Duration::from_secs(5),
            )
            .await
        {
            Ok(mut original) => {
                original.bind_dns_call(call,
                    crate::runtime::inbound_operation::raw_ip::native_obligations::DnsSourceCaller::TcpConnection(caller.clone()),
                ).map_err(io::Error::other)?;
                Some(original)
            }
            Err(source) if crate::runtime::raw_ip::tasks::capacity::before_work(&source) => {
                at_capacity = true;
                None
            }
            Err(source) => return Err(io::Error::other(source)),
        };
        let response = if at_capacity {
            drop(work); // This exact answer future was never polled/submitted.
            resolver.busy_response(&query)?
        } else {
            work.await?
        };
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
