//! Cancelled logical END remains in this same writer until FIFO delivery.
use super::*;
impl TrojanInboundMuxWriter {
    pub(super) fn request_end(&self, sid: u16) {
        if self.end(sid).is_err() {
            self.pending_ends
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(sid);
            // Same original writer task, including the empty-queue race.
            self.end_changed.notify_one();
        }
    }
    pub(super) fn retry_end_requests(&self) {
        // The wire SID is finite. These are the same still-owned SIDs, not
        // native receipts or a new executor/association allocation.
        let pending = self
            .pending_ends
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .copied()
            .collect::<Vec<_>>();
        for sid in pending {
            if self.end(sid).is_ok() {
                self.pending_ends
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&sid);
            }
        }
    }
}

/// No queued or reserved DATA can follow receiver None. Complete the retained
/// endings on this same original wire; EOF itself is never END delivery.
pub(super) async fn finish_last_sender<W: tokio::io::AsyncWrite + Unpin>(
    pending: &std::sync::Arc<std::sync::Mutex<std::collections::BTreeSet<u16>>>,
    writer: &mut W,
) -> Result<(), zero_platform_tokio::contract::ChannelError> {
    let endings = pending
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .copied()
        .collect::<Vec<_>>();
    for sid in endings {
        let frame = encode_end_stream(sid)
            .map_err(|source| standard::write_error(io::Error::other(source)))?;
        writer
            .write_all(&frame)
            .await
            .map_err(standard::write_error)?;
        writer.flush().await.map_err(standard::write_error)?;
        pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&sid);
    }
    Ok(())
}
