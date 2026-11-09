//! One finite output drain on its original registered blocking actor.
use super::*;
use zero_platform_tokio::contract::lifecycle::ComponentError;
pub(super) fn run(entry: file::Output) -> Result<(), ComponentError> {
    entry.gate.wait();
    if !entry.recover_closed() {
        return Ok(());
    }
    if !entry.initialized() {
        if entry.stopping() && entry.queue_empty() {
            entry.close();
            return Ok(());
        }
        if let Err(error) = entry.open() {
            let source = entry.receive_open_failure(error);
            return Err(zero_platform_tokio::contract::ChannelError::retained(
                source.diagnostic(),
            ));
        }
        entry.mark_ready();
    }
    loop {
        if entry.has_pending_io() || entry.is_unknown() || !entry.ensure_slot(0) {
            break;
        }
        let frame = entry.queue.lock().unwrap_or_else(|e| e.into_inner()).pop();
        let Some(frame) = frame else { break };
        *entry.working.lock().unwrap_or_else(|e| e.into_inner()) = Some(frame);
        #[cfg(test)]
        if entry
            .panic_next_work
            .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            panic!("original logging actor failed with its working chunk installed");
        }
        if !entry.write_working() {
            #[cfg(test)]
            if entry
                .panic_after_write_failure
                .swap(false, std::sync::atomic::Ordering::AcqRel)
            {
                panic!("original logger panic after independently retained OS error");
            }
            break;
        }
        // Source is durably retained before an actual failed chunk can return.
        let frame = entry
            .working
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        drop(frame);
        entry.book.wake.notify();
    }
    // This output's next job is never started until this real exit is joined
    // and ACKed. Concurrent enqueue after the empty check stays in its original
    // queue; main observes both that queue and the original completion wake.
    entry.flush();
    // All admissions were fenced before stop. Read preparing FIRST, then
    // recheck the actual queue under its lock so the last admitted enqueue
    // cannot race this close. Potentially blocking close stays on this actor.
    if entry.stopping()
        && entry.book.preparing() == 0
        && entry.queue_empty()
        && !entry.has_pending_io()
        && !entry.is_unknown()
    {
        entry.close();
    }
    entry.book.wake.notify();
    Ok(())
}
