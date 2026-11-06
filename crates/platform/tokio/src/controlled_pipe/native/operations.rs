use super::*;
impl Native {
    pub(super) fn start_read(&mut self, shared: &Shared) -> io::Result<()> {
        if self.read.pending
            || lock(&shared.state).eof
            || lock(&shared.state).read.len() == READ_SLOTS
        {
            return Ok(());
        }
        self.read.bytes.resize(CHUNK, 0);
        self.read.offset = 0;
        unsafe {
            ResetEvent(self.read.overlapped.hEvent);
        }
        self.read.pending = true;
        let mut size = 0;
        let accepted = unsafe {
            ReadFile(
                handle(self.pipe),
                self.read.bytes.as_mut_ptr(),
                self.read.bytes.len() as u32,
                &mut size,
                &mut *self.read.overlapped,
            )
        };
        if accepted != 0 {
            self.read.pending = false;
            self.publish_read(shared, size as usize);
        } else {
            let error = io::Error::last_os_error();
            match error.raw_os_error().map(|code| code as u32) {
                Some(ERROR_IO_PENDING) => {
                    shared.pending.fetch_or(2, Ordering::AcqRel);
                }
                Some(ERROR_BROKEN_PIPE | ERROR_HANDLE_EOF) => {
                    self.read.pending = false;
                    lock(&shared.state).eof = true;
                    shared.notify_io();
                }
                _ => {
                    self.read.pending = false;
                    return Err(error);
                }
            }
        }
        Ok(())
    }
    pub(super) fn finish_read(&mut self, shared: &Shared) -> io::Result<()> {
        let result = Self::complete(self.pipe, &mut self.read, false);
        if !self.read.pending {
            shared.pending.fetch_and(!2, Ordering::AcqRel);
        }
        match result {
            Ok(size) => self.publish_read(shared, size),
            Err(error)
                if matches!(
                    error.raw_os_error().map(|code| code as u32),
                    Some(ERROR_BROKEN_PIPE | ERROR_HANDLE_EOF)
                ) =>
            {
                lock(&shared.state).eof = true;
                shared.notify_io();
            }
            Err(error) => return Err(error),
        }
        Ok(())
    }
    pub(super) fn publish_read(&mut self, shared: &Shared, size: usize) {
        let mut state = lock(&shared.state);
        if size == 0 {
            state.eof = true;
        } else {
            state.read.push_back(self.read.bytes[..size].to_vec());
        }
        drop(state);
        shared.notify_io();
    }
    pub(super) fn start_write(&mut self, shared: &Shared) -> io::Result<()> {
        if self.write.pending {
            return Ok(());
        }
        if self.write.offset == self.write.bytes.len() {
            let mut state = lock(&shared.state);
            let Some(bytes) = state.write.pop_front() else {
                state.writing = false;
                drop(state);
                shared.notify_io();
                return Ok(());
            };
            self.write.bytes = bytes;
            self.write.offset = 0;
            state.writing = true;
            drop(state);
            shared.notify_io();
        }
        unsafe {
            ResetEvent(self.write.overlapped.hEvent);
        }
        self.write.pending = true;
        let mut size = 0;
        let remaining = &self.write.bytes[self.write.offset..];
        let accepted = unsafe {
            WriteFile(
                handle(self.pipe),
                remaining.as_ptr(),
                remaining.len() as u32,
                &mut size,
                &mut *self.write.overlapped,
            )
        };
        if accepted != 0 {
            self.write.pending = false;
            self.written(shared, size as usize)?;
        } else {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(ERROR_IO_PENDING as i32) {
                shared.pending.fetch_or(4, Ordering::AcqRel);
            } else {
                self.write.pending = false;
                return Err(error);
            }
        }
        Ok(())
    }
    pub(super) fn finish_write(&mut self, shared: &Shared) -> io::Result<()> {
        let result = Self::complete(self.pipe, &mut self.write, false);
        if !self.write.pending {
            shared.pending.fetch_and(!4, Ordering::AcqRel);
        }
        let size = result?;
        self.written(shared, size)
    }
    pub(super) fn written(&mut self, shared: &Shared, size: usize) -> io::Result<()> {
        if size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "original pipe write completed without progress",
            ));
        }
        self.write.offset += size;
        if self.write.offset == self.write.bytes.len() {
            lock(&shared.state).writing = false;
        }
        shared.notify_io();
        Ok(())
    }
    pub(super) fn complete(
        pipe: usize,
        operation: &mut Operation,
        wait: bool,
    ) -> io::Result<usize> {
        let mut size = 0;
        let completed = unsafe {
            GetOverlappedResult(
                handle(pipe),
                &*operation.overlapped,
                &mut size,
                i32::from(wait),
            )
        };
        if completed != 0 {
            operation.pending = false;
            return Ok(size as usize);
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(ERROR_IO_INCOMPLETE as i32) {
            operation.pending = false;
        }
        Err(error)
    }
    pub(super) fn drain(&mut self) -> io::Result<()> {
        for operation in [&mut self.connect, &mut self.read, &mut self.write] {
            if !operation.pending {
                continue;
            }
            if unsafe { CancelIoEx(handle(self.pipe), &*operation.overlapped) } == 0 {
                let error = io::Error::last_os_error();
                if error.raw_os_error() != Some(ERROR_NOT_FOUND as i32) {
                    self.faults.push(Arc::new(error));
                }
            }
            if let Err(error) = Self::complete(self.pipe, operation, true) {
                if error.raw_os_error() != Some(ERROR_OPERATION_ABORTED as i32) {
                    self.faults.push(Arc::new(error));
                }
            }
        }
        self.fault_result()
    }
}
