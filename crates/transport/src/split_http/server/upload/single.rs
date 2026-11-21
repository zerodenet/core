//! A returned stream-one download does not truncate the actual HTTP upload.
use super::*;
use crate::split_http::io::LifetimeRef;

pub(in crate::split_http::server) async fn stream_one_upload<B, W>(
    mut body: B,
    writer: &mut W,
    life: &LifetimeRef,
) -> io::Result<()>
where
    B: hyper::body::Body<Data = Bytes> + Unpin,
    B::Error: std::error::Error + Send + Sync + 'static,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut returning = false;
    loop {
        let frame = tokio::select! { biased;
            _ = life.cancelled(), if !returning => {
                life.error()?;
                returning = true;
                continue;
            },
            frame = body.frame() => frame,
        };
        let Some(frame) = frame else { break };
        if let Ok(bytes) = frame.map_err(io::Error::other)?.into_data() {
            if returning {
                continue;
            }
            let written = tokio::select! { biased;
                _ = life.cancelled() => {
                    life.error()?;
                    returning = true;
                    continue;
                },
                result = writer.write_all(&bytes) => result,
            };
            if let Err(error) = written {
                life.error()?;
                if life.is_closed() {
                    returning = true;
                    continue;
                }
                return Err(error);
            }
        }
    }
    writer.shutdown().await
}
