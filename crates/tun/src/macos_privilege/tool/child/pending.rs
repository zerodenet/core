//! Test-only rendezvous after the real original stdio poll returns Pending.
use super::*;
impl ChildOriginal {
    pub(super) fn observe_pending(&mut self, descriptors: &mut [libc::pollfd]) -> io::Result<()> {
        if self.pending.is_some()
            && (!self.pending_after_stdio || self.pipes.iter().flatten().all(Original::closed))
        {
            let count = unsafe { libc::poll(descriptors.as_mut_ptr(), descriptors.len() as _, 0) };
            if count < 0 {
                return Err(io::Error::last_os_error());
            }
            if count == 0 {
                let pending = self.pending.take().unwrap();
                pending
                    .send(())
                    .map_err(|_| io::Error::other("actual IO Pending receiver expired"))?;
            }
        }
        Ok(())
    }
}
