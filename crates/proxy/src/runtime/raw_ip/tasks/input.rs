//! Seal a real input poll through installation in its existing child owner.
use std::{future::Future, sync::Arc, task::Poll};
use zero_platform_tokio::contract::{
    packet::{PacketSourceAcquisition, PacketSourcePause},
    ChannelError,
};

/// The returned guard stays with the dequeued input until its caller installs
/// the original peer/native/task records. An empty read holds no acquisition:
/// Source pause can then reach the same main/wire checkpoints. A sealed Source
/// waits for resume without polling or consuming that same input future.
pub(crate) async fn admitted<T>(
    input: impl Future<Output = T>,
    source: &Arc<PacketSourcePause>,
) -> Result<(T, PacketSourceAcquisition), ChannelError> {
    tokio::pin!(input);
    let mut entering = Box::pin(source.enter());
    std::future::poll_fn(|cx| {
        let acquisition = match entering.as_mut().poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(source)) => return Poll::Ready(Err(source)),
            Poll::Ready(Ok(acquisition)) => acquisition,
        };
        match input.as_mut().poll(cx) {
            Poll::Ready(value) => Poll::Ready(Ok((value, acquisition))),
            Poll::Pending => {
                drop(acquisition);
                entering = Box::pin(source.enter());
                Poll::Pending
            }
        }
    })
    .await
}

#[cfg(test)]
mod tests;
