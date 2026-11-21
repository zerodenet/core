//! Return the logical upload without truncating its actual HTTP request body.
use super::*;
mod single;
pub(super) use single::stream_one_upload;

pub(super) async fn stream_upload<B>(mut body: B, session: &Session) -> io::Result<()>
where
    B: hyper::body::Body<Data = Bytes> + Unpin,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    let mut sequence = 0u64;
    let mut returning = false;
    loop {
        let frame = tokio::select! { biased;
            _ = session.life.cancelled(), if !returning => {
                session.life.error()?;
                returning = true;
                continue;
            },
            frame = body.frame() => frame,
        };
        let Some(frame) = frame else { break };
        // Real wire errors remain errors even after the logical user returns.
        if let Ok(bytes) = frame.map_err(io::Error::other)?.into_data() {
            for chunk in bytes.chunks(64 * 1024) {
                if returning {
                    continue;
                }
                let pushed = tokio::select! { biased;
                    _ = session.life.cancelled() => {
                        session.life.error()?;
                        returning = true;
                        continue;
                    },
                    result = session.push_stream(sequence, Bytes::copy_from_slice(chunk)) => result,
                };
                if let Err(error) = pushed {
                    session.life.error()?;
                    if error
                        .get_ref()
                        .is_some_and(|source| source.is::<super::super::sessions::SessionClosed>())
                    {
                        returning = true;
                        continue;
                    }
                    return Err(error);
                }
                sequence = sequence
                    .checked_add(1)
                    .ok_or_else(|| io::Error::other("xhttp sequence exhausted"))?;
            }
        }
    }
    // Only the actual HTTP END_STREAM/last chunk can end its response worker.
    session.finish();
    Ok(())
}

#[cfg(test)]
#[path = "../../../tests/xhttp_server/upload_return.rs"]
mod tests;

#[cfg(test)]
#[path = "../../../tests/xhttp_server/stream_one_return.rs"]
mod stream_one_tests;
