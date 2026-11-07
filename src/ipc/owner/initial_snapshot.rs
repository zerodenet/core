//! One final original event buffer, prepaid before any snapshot DTO can exist.
use std::{io, sync::Arc};
use zero_runtime::configuration::{ControlBudget, ControlBudgetPermit, ControlCharge};
pub(crate) struct EncodedEvent {
    pub(crate) bytes: Vec<u8>,
    pub(crate) credit: ControlBudgetPermit,
}
#[derive(Clone, Copy)]
pub(crate) enum EventFraming {
    Ipc,
    Sse,
}
pub(crate) fn prepare_initial_snapshot(
    engine: &zero_engine::EngineHandle,
    budget: &Arc<ControlBudget>,
    framing: EventFraming,
    ordered_work_bytes: usize,
) -> io::Result<(EncodedEvent, Option<ControlBudgetPermit>)> {
    // Both tickets are outside every actual Engine guard, on error/unwind too.
    let mut workspace = budget.reserve(ControlCharge::ResponseBytes(0))?;
    let mut output = None;
    let prepared = (|| -> io::Result<Vec<u8>> {
        let guard = engine.inner().control_flow_snapshot();
        let view = guard.freeze(&mut workspace)?;
        let json = super::serialized_len(&view)?;
        if json > budget.limits().messages.response_bytes {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "initial snapshot exceeds the configured response limit",
            ));
        }
        let size = match framing {
            EventFraming::Ipc => json.checked_add(1),
            EventFraming::Sse => json.checked_add(
                5 + (view.sequence().max(1).ilog10() as usize + 1)
                    + 8
                    + view.event_type().len()
                    + 6
                    + 2,
            ),
        }
        .ok_or_else(|| io::Error::other("initial snapshot framing overflow"))?;
        let total = size
            .checked_add(ordered_work_bytes)
            .ok_or_else(|| io::Error::other("ordered snapshot operation overflow"))?;
        output = Some(budget.reserve(ControlCharge::QueuedEventBytes(total))?);
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(size).map_err(io::Error::other)?;
        use io::Write;
        if matches!(framing, EventFraming::Sse) {
            writeln!(&mut bytes, "id: {}", view.sequence())?;
            writeln!(&mut bytes, "event: {}", view.event_type())?;
            bytes.extend_from_slice(b"data: ");
        }
        serde_json::to_writer(&mut bytes, &view).map_err(io::Error::other)?;
        match framing {
            EventFraming::Ipc => bytes.push(b'\n'),
            EventFraming::Sse => bytes.extend_from_slice(b"\n\n"),
        }
        if bytes.len() != size {
            return Err(io::Error::other(
                "same original snapshot changed during encoding",
            ));
        }
        Ok(bytes)
    })();
    drop(workspace);
    let bytes = prepared?;
    let mut credit = output.take().expect("same original snapshot credit");
    let ordered = if ordered_work_bytes == 0 {
        None
    } else {
        Some(credit.split_queued_event_bytes(ordered_work_bytes)?)
    };
    Ok((EncodedEvent { bytes, credit }, ordered))
}
#[cfg(test)]
mod tests;
