//! Native close failure keeps the same unknown original; never retry/use it.
use super::*;
impl OwnedWintunReadControl {
    pub(in crate::windows::owned) fn close(&self) -> Result<(), Arc<io::Error>> {
        let mut state = self.state();
        if let Some(source) = &state.close_unknown {
            return Err(source.clone());
        }
        if !state.stopped {
            return Err(Arc::new(io::Error::new(
                io::ErrorKind::WouldBlock,
                "same original reader has not stopped",
            )));
        }
        let Some(event) = state.event else {
            state.closed = true;
            return Ok(());
        };
        // SAFETY: the owner calls only after the original reader body returned;
        // runtime additionally requires its actual blocking TaskJoinWitness.
        if unsafe { CloseHandle(event as HANDLE) } == 0 {
            let source = Arc::new(io::Error::last_os_error());
            state.close_unknown = Some(source.clone());
            return Err(source);
        }
        state.event = None;
        state.closed = true;
        Ok(())
    }
    pub(in crate::windows::owned) fn is_open(&self) -> bool {
        self.state().event.is_some()
    }
}
