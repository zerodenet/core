//! Runtime-provided target connections for transport handshakes and decoy relays.
use std::{future::Future, io, pin::Pin, sync::Arc};
use zero_platform_tokio::TcpRelayStream;
use zero_traits::FallbackEndpoint;

pub type ConnectFuture = Pin<Box<dyn Future<Output = io::Result<TcpRelayStream>> + Send>>;
#[derive(Clone)]
pub struct Connector(Arc<dyn Fn(FallbackEndpoint) -> ConnectFuture + Send + Sync>);
impl std::fmt::Debug for Connector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HandshakeTargetConnector")
    }
}
impl Connector {
    pub fn new(
        connect: impl Fn(FallbackEndpoint) -> ConnectFuture + Send + Sync + 'static,
    ) -> Self {
        Self(Arc::new(connect))
    }
    pub async fn connect(&self, endpoint: FallbackEndpoint) -> io::Result<TcpRelayStream> {
        (self.0)(endpoint).await
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RateLimit {
    pub after_bytes: u64,
    pub bytes_per_sec: u64,
    pub burst_bytes: u64,
}

pub async fn relay<A, B>(
    client: &mut A,
    target: &mut B,
    upload: RateLimit,
    download: RateLimit,
) -> io::Result<()>
where
    A: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    B: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (client_read, client_write) = tokio::io::split(client);
    let (target_read, target_write) = tokio::io::split(target);
    tokio::try_join!(
        copy(client_read, target_write, upload),
        copy(target_read, client_write, download)
    )?;
    Ok(())
}
async fn copy<R: tokio::io::AsyncRead + Unpin, W: tokio::io::AsyncWrite + Unpin>(
    mut read: R,
    mut write: W,
    limit: RateLimit,
) -> io::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut buffer = [0; 8192];
    let mut exempt = limit.after_bytes;
    let mut credit = limit.burst_bytes.max(limit.bytes_per_sec) as f64;
    let mut last = tokio::time::Instant::now();
    loop {
        let size = read.read(&mut buffer).await?;
        if size == 0 {
            return write.shutdown().await;
        }
        // Match the official read boundary: the read crossing after_bytes is exempt.
        if exempt > 0 {
            exempt = exempt.saturating_sub(size as u64);
        } else if limit.bytes_per_sec > 0 {
            let now = tokio::time::Instant::now();
            let elapsed = now.saturating_duration_since(last).as_secs_f64();
            credit = (credit + elapsed * limit.bytes_per_sec as f64)
                .min(limit.burst_bytes.max(limit.bytes_per_sec) as f64);
            credit -= size as f64;
            last = now;
            if credit < 0.0 {
                let delay =
                    std::time::Duration::from_secs_f64(-credit / limit.bytes_per_sec as f64);
                tokio::time::sleep(delay).await;
                last = tokio::time::Instant::now();
                credit = 0.0;
            }
        }
        write.write_all(&buffer[..size]).await?;
    }
}

/// A prepared transport continuation. Runtime polls it in the listener's task.
pub fn forward_session(
    client: TcpRelayStream,
    target: TcpRelayStream,
    prefix: Vec<u8>,
    upload: RateLimit,
    download: RateLimit,
) -> Box<dyn zero_core::inbound::InboundControlSession> {
    Box::new(Forward {
        client,
        target,
        prefix,
        upload,
        download,
    })
}
struct Forward {
    client: TcpRelayStream,
    target: TcpRelayStream,
    prefix: Vec<u8>,
    upload: RateLimit,
    download: RateLimit,
}
impl zero_core::inbound::InboundControlSession for Forward {
    fn auth(&self) -> Option<&zero_core::SessionAuth> {
        None
    }
    fn run(self: Box<Self>) -> Pin<Box<dyn Future<Output = Result<(), zero_core::Error>> + Send>> {
        Box::pin(async move {
            use tokio::io::AsyncWriteExt;
            let Self {
                mut client,
                mut target,
                prefix,
                upload,
                download,
            } = *self;
            client
                .write_all(&prefix)
                .await
                .map_err(|_| zero_core::Error::Io("target response write failed"))?;
            relay(&mut client, &mut target, upload, download)
                .await
                .map_err(|_| zero_core::Error::Io("target forwarding failed"))
        })
    }
}
