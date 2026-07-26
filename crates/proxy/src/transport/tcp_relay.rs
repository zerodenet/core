use std::io;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::rate_limit::SharedRateLimiter;

// ── Bidirectional relay ───────────────────────────────────────────────

pub(crate) async fn relay_bidirectional_metered<L, R, F1, F2>(
    left: L,
    right: R,
    left_to_right: F1,
    right_to_left: F2,
) -> io::Result<(u64, u64)>
where
    L: AsyncRead + AsyncWrite + Send + Unpin,
    R: AsyncRead + AsyncWrite + Send + Unpin,
    F1: FnMut(u64),
    F2: FnMut(u64),
{
    relay_bidirectional_metered_throttled(left, right, left_to_right, right_to_left, None, None)
        .await
}

/// Bidirectional metered relay with optional rate limiting.
///
/// `upload_limiter` limits left→right (client upload).
/// `download_limiter` limits right→left (client download).
pub(crate) async fn relay_bidirectional_metered_throttled<L, R, F1, F2>(
    left: L,
    right: R,
    left_to_right: F1,
    right_to_left: F2,
    upload_limiter: Option<SharedRateLimiter>,
    download_limiter: Option<SharedRateLimiter>,
) -> io::Result<(u64, u64)>
where
    L: AsyncRead + AsyncWrite + Send + Unpin,
    R: AsyncRead + AsyncWrite + Send + Unpin,
    F1: FnMut(u64),
    F2: FnMut(u64),
{
    let (left_read, left_write) = tokio::io::split(left);
    let (right_read, right_write) = tokio::io::split(right);

    tokio::try_join!(
        copy_one_way(left_read, right_write, left_to_right, upload_limiter),
        copy_one_way(right_read, left_write, right_to_left, download_limiter)
    )
}

/// Uni-directional byte copy with optional rate limiting.
pub(crate) async fn copy_one_way<R, W, F>(
    mut reader: R,
    mut writer: W,
    mut on_bytes: F,
    limiter: Option<SharedRateLimiter>,
) -> io::Result<u64>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
    F: FnMut(u64),
{
    let mut buf = [0_u8; 16 * 1024];
    let mut total = 0_u64;

    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            shutdown_writer(&mut writer).await?;
            return Ok(total);
        }
        if let Some(limiter) = limiter.as_ref() {
            limiter.throttle(n).await;
        }
        writer.write_all(&buf[..n]).await?;
        writer.flush().await?;
        total = total.saturating_add(n as u64);
        on_bytes(n as u64);
    }
}

async fn shutdown_writer(writer: &mut (impl AsyncWrite + Unpin)) -> io::Result<()> {
    match writer.shutdown().await {
        Ok(()) => Ok(()),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::NotConnected | io::ErrorKind::BrokenPipe
            ) =>
        {
            Ok(())
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests;
