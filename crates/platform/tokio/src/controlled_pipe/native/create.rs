//! Same retained graph, with explicit server/client Win32 acquisition.
use super::*;
use std::{os::windows::ffi::OsStrExt, ptr};
use windows_sys::Win32::{
    Foundation::{GetLastError, SetLastError, ERROR_NO_DATA, ERROR_PIPE_CONNECTED, ERROR_SUCCESS},
    Storage::FileSystem::{
        CreateFileW, GetFileType, FILE_FLAG_OVERLAPPED, FILE_GENERIC_READ, FILE_GENERIC_WRITE,
        FILE_TYPE_PIPE, FILE_TYPE_UNKNOWN, OPEN_EXISTING, PIPE_ACCESS_DUPLEX,
    },
    System::{
        Pipes::{
            ConnectNamedPipe, CreateNamedPipeW, PIPE_READMODE_BYTE, PIPE_REJECT_REMOTE_CLIENTS,
            PIPE_TYPE_BYTE, PIPE_UNLIMITED_INSTANCES,
        },
        Threading::CreateEventW,
    },
};
impl Native {
    pub(super) fn create(&mut self, shared: &Shared) -> io::Result<()> {
        for index in 0..4 {
            let event = unsafe { CreateEventW(ptr::null(), 1, 0, ptr::null()) };
            if event.is_null() {
                return Err(io::Error::last_os_error());
            }
            self.events[index] = event as usize;
            self.ever_acquired = true;
        }
        self.connect.overlapped.hEvent = handle(self.events[0]);
        self.read.overlapped.hEvent = handle(self.events[1]);
        self.write.overlapped.hEvent = handle(self.events[2]);
        *lock(&shared.wake) = Some(self.events[3]);
        let mut path = shared.path.as_os_str().encode_wide().collect::<Vec<_>>();
        if path.contains(&0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "pipe path contains NUL",
            ));
        }
        path.push(0);
        let pipe = if shared.client {
            validate_path(&path)?;
            unsafe {
                CreateFileW(
                    path.as_ptr(),
                    FILE_GENERIC_READ | FILE_GENERIC_WRITE,
                    0,
                    ptr::null(),
                    OPEN_EXISTING,
                    FILE_FLAG_OVERLAPPED,
                    ptr::null_mut(),
                )
            }
        } else {
            unsafe {
                CreateNamedPipeW(
                    path.as_ptr(),
                    PIPE_ACCESS_DUPLEX | FILE_FLAG_OVERLAPPED,
                    PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_REJECT_REMOTE_CLIENTS,
                    PIPE_UNLIMITED_INSTANCES,
                    65536,
                    65536,
                    0,
                    ptr::null(),
                )
            }
        };
        if pipe == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        self.pipe = pipe as usize;
        self.ever_acquired = true;
        if shared.client {
            let kind = unsafe {
                SetLastError(ERROR_SUCCESS);
                GetFileType(pipe)
            };
            if kind == FILE_TYPE_UNKNOWN {
                let code = unsafe { GetLastError() };
                if code != ERROR_SUCCESS {
                    return Err(io::Error::from_raw_os_error(code as i32));
                }
            }
            if kind != FILE_TYPE_PIPE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "client original is not a named pipe",
                ));
            }
        }
        Ok(())
    }
    pub(super) fn start_connect(&mut self, shared: &Shared) -> io::Result<()> {
        if shared.client {
            lock(&shared.state).connected = true;
            return Ok(());
        }
        self.connect.pending = true;
        let connected =
            unsafe { ConnectNamedPipe(handle(self.pipe), &mut *self.connect.overlapped) };
        if connected != 0 {
            self.connect.pending = false;
            lock(&shared.state).connected = true;
        } else {
            let error = io::Error::last_os_error();
            match error.raw_os_error().map(|code| code as u32) {
                Some(ERROR_PIPE_CONNECTED | ERROR_NO_DATA) => {
                    self.connect.pending = false;
                    lock(&shared.state).connected = true;
                }
                Some(ERROR_IO_PENDING) => {
                    shared.pending.fetch_or(1, Ordering::AcqRel);
                }
                _ => {
                    self.connect.pending = false;
                    return Err(error);
                }
            }
        }
        Ok(())
    }
}
fn ascii(actual: &[u16], expected: &[u8]) -> bool {
    actual.len() == expected.len()
        && actual.iter().zip(expected).all(|(a, b)| {
            *a == u16::from(b.to_ascii_lowercase()) || *a == u16::from(b.to_ascii_uppercase())
        })
}
fn validate_path(path: &[u16]) -> io::Result<()> {
    let slash = u16::from(b'\\');
    let invalid = || {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "client requires an explicit named-pipe namespace",
        )
    };
    if !path.starts_with(&[slash, slash]) {
        return Err(invalid());
    }
    let mut parts = path[2..].split(|unit| *unit == slash);
    let server = parts.next().ok_or_else(invalid)?;
    let mut share = parts.next().ok_or_else(invalid)?;
    if server == [u16::from(b'?')] && ascii(share, b"UNC") {
        if parts.next().is_none_or(|server| server.is_empty()) {
            return Err(invalid());
        }
        share = parts.next().ok_or_else(invalid)?;
    }
    if server.is_empty()
        || !ascii(share, b"pipe")
        || parts
            .next()
            .is_none_or(|name| name.is_empty() || name[0] == 0)
    {
        return Err(invalid());
    }
    Ok(())
}
