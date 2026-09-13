//! Packet ownership outlives its HTTP acknowledgement connection.
use super::*;
use tokio::sync::OwnedSemaphorePermit;

pub(super) async fn upload<B>(
    request: http::Request<B>,
    profile: Profile,
    session: Arc<Session>,
    sequence: u64,
    permit: OwnedSemaphorePermit,
) -> io::Result<http::Response<Body>>
where
    B: hyper::body::Body<Data = Bytes> + Unpin + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    let http1 = request.version() == http::Version::HTTP_11;
    if http1 {
        if let Some(policy) = request.extensions().get::<super::http1::ReplyPolicy>() {
            policy.set_packet(true);
        }
    }
    let read = async {
        let mut data = profile.packet_prefix(&request)?;
        let mut budget = session.reserve_upload(data.len())?;
        let prefix_len = data.len();
        if matches!(
            profile.options.uplink_data_placement.as_str(),
            "body" | "auto"
        ) {
            let mut body = request.into_body();
            while let Some(frame) = body.frame().await {
                if let Ok(bytes) = frame.map_err(io::Error::other)?.into_data() {
                    if data.len() + bytes.len() > profile.options.sc_max_each_post_bytes.to as usize
                    {
                        return Err(io::Error::new(
                            io::ErrorKind::FileTooLarge,
                            "xhttp POST too large",
                        ));
                    }
                    budget.merge(session.reserve_upload(bytes.len())?);
                    data.extend_from_slice(&bytes);
                }
            }
        }
        Ok::<_, io::Error>((data.len() == prefix_len, Bytes::from(data), budget))
    };
    let (body_empty, data, budget) = match tokio::time::timeout(Duration::from_secs(30), read).await
    {
        Ok(Ok(packet)) => packet,
        Ok(Err(error)) => {
            return Ok(status(if error.kind() == io::ErrorKind::FileTooLarge {
                413
            } else {
                400
            }))
        }
        Err(_) => return Ok(status(408)),
    };
    // Once the complete packet has arrived, HTTP cancellation cannot undo it.
    // Go's synchronous handler also finishes its queue Push after peer closure.
    // The task retains its request permit, queue/byte limits and deadline.
    let admission = admit(session, sequence, data, permit, budget);
    match admission.await {
        Ok(()) => {
            let mut response = status(200);
            if body_empty {
                response
                    .headers_mut()
                    .insert("cache-control", http::HeaderValue::from_static("no-store"));
            }
            Ok(response)
        }
        Err(error) => {
            tracing::debug!(?error, "XHTTP packet admission rejected");
            Ok(status(if error.kind() == io::ErrorKind::AlreadyExists {
                409
            } else {
                500
            }))
        }
    }
}
async fn admit(
    session: Arc<Session>,
    sequence: u64,
    data: Bytes,
    permit: OwnedSemaphorePermit,
    budget: OwnedSemaphorePermit,
) -> io::Result<()> {
    use std::{future::Future, task::Poll};
    let mut admission = Box::pin(async move {
        let _permit = permit;
        match tokio::time::timeout(
            Duration::from_secs(30),
            session.push_reserved(sequence, data, false, budget),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                session.life.fail("xhttp packet admission timed out");
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "xhttp packet admission timed out",
                ))
            }
        }
    });
    // Admit in the receiving task first. Spawning every packet adds scheduler
    // reordering before the FIFO writer lock, even when the queue has room.
    // If backpressured, transfer the already-registered lock waiter intact.
    let ready = std::future::poll_fn(|cx| {
        // A depleted Tokio cooperative budget can return Pending before Mutex
        // registers its waiter, even when the queue is empty. Complete this one
        // bounded reservation poll before transferring ownership to another task.
        let first = tokio::task::unconstrained(admission.as_mut());
        tokio::pin!(first);
        Poll::Ready(match first.as_mut().poll(cx) {
            Poll::Ready(result) => Some(result),
            Poll::Pending => None,
        })
    })
    .await;
    match ready {
        Some(result) => result,
        None => tokio::spawn(admission).await.map_err(io::Error::other)?,
    }
}
#[cfg(test)]
#[path = "../../../tests/xhttp_server/packet_admission.rs"]
mod tests;
