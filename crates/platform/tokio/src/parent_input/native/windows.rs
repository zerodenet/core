use super::*;
use std::os::windows::io::AsRawHandle;
use windows_sys::Win32::{
    Foundation::{
        CloseHandle, DUPLICATE_SAME_ACCESS, DuplicateHandle, ERROR_BROKEN_PIPE, ERROR_HANDLE_EOF,
        ERROR_NOT_FOUND, ERROR_OPERATION_ABORTED, HANDLE,
    },
    Storage::FileSystem::ReadFile,
    System::{IO::CancelSynchronousIo, Threading::GetCurrentProcess},
};
#[derive(Default)]
pub(crate) struct Input {
    original: Option<usize>,
    unknown: Option<Arc<io::Error>>,
}
pub(crate) fn duplicate_stdin() -> io::Result<Input> {
    duplicate(std::io::stdin().as_raw_handle() as usize)
}
pub(crate) fn duplicate(original: usize) -> io::Result<Input> {
    let process = unsafe { GetCurrentProcess() };
    let mut handle = std::ptr::null_mut();
    if unsafe {
        DuplicateHandle(
            process,
            original as HANDLE,
            process,
            &mut handle,
            0,
            0,
            DUPLICATE_SAME_ACCESS,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(Input {
        original: Some(handle as usize),
        unknown: None,
    })
}
impl Input {
    pub(crate) fn raw(&self) -> usize {
        self.original.expect("acquired original input")
    }
    pub(crate) fn close(&mut self) -> io::Result<()> {
        if let Some(error) = &self.unknown {
            return Err(retained(error));
        }
        if let Some(original) = self.original {
            if unsafe { CloseHandle(original as HANDLE) } == 0 {
                let error = Arc::new(io::Error::last_os_error());
                self.unknown = Some(error.clone());
                return Err(retained(&error));
            }
            self.original = None;
        }
        Ok(())
    }
}
pub(crate) fn interrupt(worker: &std::thread::JoinHandle<io::Result<()>>) -> io::Result<()> {
    if unsafe { CancelSynchronousIo(worker.as_raw_handle()) } != 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(ERROR_NOT_FOUND as i32) {
        Ok(())
    } else {
        Err(error)
    }
}
pub(crate) fn read_to_end(raw: usize, stop: &AtomicBool) -> io::Result<()> {
    let mut buffer = [0u8; 64];
    loop {
        if stop.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut count = 0;
        if unsafe {
            ReadFile(
                raw as HANDLE,
                buffer.as_mut_ptr(),
                buffer.len() as u32,
                &mut count,
                std::ptr::null_mut(),
            )
        } == 0
        {
            let error = io::Error::last_os_error();
            match error.raw_os_error().map(|code| code as u32) {
                Some(ERROR_BROKEN_PIPE | ERROR_HANDLE_EOF) => return Ok(()),
                Some(ERROR_OPERATION_ABORTED) if stop.load(Ordering::Acquire) => return Ok(()),
                _ => return Err(error),
            }
        }
        if count == 0 {
            return Ok(());
        }
    }
}
