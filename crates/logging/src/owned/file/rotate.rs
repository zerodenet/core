use super::*;
impl Entry {
    pub(super) fn rotate_locked(
        &self,
        state: &mut Native,
    ) -> Result<(), (fault::IoPhase, io::Error)> {
        state.rotating = true;
        if state.closed.is_none() {
            let original = state.original.as_mut().ok_or_else(|| {
                (
                    fault::IoPhase::Rotate,
                    io::Error::other("rotation lacks its original"),
                )
            })?;
            original
                .flush()
                .map_err(|error| (fault::IoPhase::Flush, error))?;
            let receipt = original
                .close()
                .map_err(|error| (fault::IoPhase::Close, error))?;
            if !original.matches_closed(&receipt) {
                return Err((
                    fault::IoPhase::Close,
                    io::Error::other("rotation close receipt mismatch"),
                ));
            }
            state.closed = Some(receipt);
            self.native_closed
                .store(true, std::sync::atomic::Ordering::Release);
        }
        // A rename error leaves this same old graph positively closed. A later
        // admitted attempt resumes rotation, never writes through a closed FD.
        for index in (1..self.max_files).rev() {
            let source = if index == 1 {
                self.path.clone()
            } else {
                self.path.with_extension(format!("{}", index - 1))
            };
            let destination = self.path.with_extension(index.to_string());
            if source.exists() {
                fs::rename(source, destination).map_err(|error| (fault::IoPhase::Rotate, error))?;
            }
        }
        state.acquiring = true;
        self.native_closed
            .store(false, std::sync::atomic::Ordering::Release);
        let next = match fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)
        {
            Ok(file) => file,
            Err(error) => {
                state.acquiring = false;
                self.native_closed
                    .store(true, std::sync::atomic::Ordering::Release);
                return Err((fault::IoPhase::Open, error));
            }
        };
        let next = OriginalFile::adopt(next);
        // Replacing a positively closed controller does not create a native
        // overlap; no Drop observation is substituted for its stored receipt.
        state.original = Some(next);
        state.acquiring = false;
        state.closed = None;
        state.written = 0;
        state.rotating = false;
        Ok(())
    }
}
