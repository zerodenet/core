//! Per-frame storage is charged before allocation and released with its owner.
use super::*;
use tokio::io::AsyncBufRead;

pub(super) struct RequestFrame {
    bytes: Vec<u8>,
    _storage: ControlBudgetPermit,
}
impl RequestFrame {
    pub(super) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    /// The decoded envelope may coexist with its original wire bytes. Its
    /// independent reservation follows that envelope through command join.
    pub(super) fn reserve_decoded(
        &self,
        budget: &Arc<ControlBudget>,
    ) -> io::Result<ControlBudgetPermit> {
        budget
            .reserve(ControlCharge::RequestBytes(self.bytes.len()))
            .map_err(input::budget)
    }
}
pub(super) async fn next_request(
    reader: &mut (impl AsyncBufRead + Unpin),
    budget: &Arc<ControlBudget>,
    commands: &mut ConcurrentCommandTasks,
) -> io::Result<Option<RequestFrame>> {
    // Pin this exact read once. A command join must not discard bytes already
    // consumed by a partial frame, nor wait for another client request to free
    // that completed command's original join/credits.
    let reading = read_request_frame(reader, budget);
    tokio::pin!(reading);
    loop {
        let commands_pending = commands.has_tasks();
        tokio::select! {
            result=&mut reading=>return result,
            _=commands.receive_next(),if commands_pending=>{},
        }
    }
}
pub(super) async fn read_request_frame(
    reader: &mut (impl AsyncBufRead + Unpin),
    budget: &Arc<ControlBudget>,
) -> io::Result<Option<RequestFrame>> {
    let mut storage = budget
        .reserve(ControlCharge::RequestBytes(0))
        .map_err(input::budget)?;
    let mut bytes = Vec::new();
    loop {
        let input = reader.fill_buf().await?;
        if input.is_empty() {
            return Ok((!bytes.is_empty()).then_some(RequestFrame {
                bytes,
                _storage: storage,
            }));
        }
        let complete = input.iter().position(|byte| *byte == b'\n');
        let take = complete.map_or(input.len(), |index| index + 1);
        let length = bytes.len().checked_add(take).ok_or_else(input::too_large)?;
        if length > budget.limits().messages.ipc_frame_bytes {
            return Err(input::too_large());
        }
        // Exact reserve avoids geometric idle capacity. This entire buffer is
        // dropped after its frame, rather than clear() retaining an uncharged allocation.
        if length > bytes.capacity() {
            storage
                .grow(length - bytes.capacity())
                .map_err(input::budget)?;
            bytes
                .try_reserve_exact(length - bytes.len())
                .map_err(io::Error::other)?;
        }
        bytes.extend_from_slice(&input[..take]);
        reader.consume(take);
        if complete.is_some() {
            return Ok(Some(RequestFrame {
                bytes,
                _storage: storage,
            }));
        }
    }
}

pub(super) async fn write_budgeted_response(
    writer: &mut (impl AsyncWriteExt + Unpin),
    response: &impl Serialize,
    budget: &Arc<ControlBudget>,
) -> io::Result<()> {
    let bytes = super::super::owner::serialized_len(response)?
        .checked_add(1)
        .ok_or_else(|| io::Error::other("control response envelope length overflow"))?;
    if bytes > budget.limits().messages.response_bytes {
        return Err(input::too_large());
    }
    let _storage = budget
        .reserve(ControlCharge::ResponseBytes(bytes))
        .map_err(input::budget)?;
    let frame = serialize_measured(response, bytes)?;
    writer.write_all(&frame).await?;
    writer.flush().await
}

pub(super) fn serialize_measured(response: &impl Serialize, bytes: usize) -> io::Result<Vec<u8>> {
    let mut frame = Vec::new();
    frame.try_reserve_exact(bytes).map_err(io::Error::other)?;
    serde_json::to_writer(&mut frame, response).map_err(io::Error::other)?;
    frame.push(b'\n');
    Ok(frame)
}

#[cfg(test)]
mod tests;
