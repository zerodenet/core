//! Synchronous local IPC with one retained pre-connect owner and explicit close.
use crate::file::OriginalFile;
use std::{
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
#[cfg(unix)]
enum Native {
    Pending(socket2::Socket),
    Connected(std::os::unix::net::UnixStream),
}
#[cfg(windows)]
type Native = OriginalFile;
#[cfg(not(any(unix, windows)))]
enum Native {}

#[derive(Clone, Debug)]
pub struct OriginalLocalStreamClosed {
    identity: Arc<()>,
    native: bool,
}
impl OriginalLocalStreamClosed {
    pub fn has_native_original(&self) -> bool {
        self.native
    }
}
/// Install this concrete owner before calling connect. It never duplicates or
/// exports an OS handle. A failed/interrupted connect leaves the original here;
/// only close's actual native result permits construction of a replacement.
pub struct OriginalLocalStream {
    path: PathBuf,
    attempted: bool,
    native: Option<Native>,
    closing: Option<OriginalFile>,
    identity: Arc<()>,
    acquired: bool,
    receipt: Option<OriginalLocalStreamClosed>,
}
impl OriginalLocalStream {
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        #[cfg(unix)]
        std::os::unix::net::SocketAddr::from_pathname(path.as_ref())?;
        Ok(Self {
            path: path.as_ref().to_owned(),
            attempted: false,
            native: None,
            closing: None,
            identity: Arc::new(()),
            acquired: false,
            receipt: None,
        })
    }
    pub fn connect(&mut self) -> io::Result<()> {
        if self.attempted || self.receipt.is_some() {
            return Err(io::Error::other(
                "original local connect already attempted or retired",
            ));
        }
        self.attempted = true;
        #[cfg(unix)]
        {
            let address = socket2::SockAddr::unix(&self.path)?;
            let socket = socket2::Socket::new(socket2::Domain::UNIX, socket2::Type::STREAM, None)?;
            self.native = Some(Native::Pending(socket));
            self.acquired = true;
            let Some(Native::Pending(socket)) = &self.native else {
                unreachable!()
            };
            socket.connect(&address)?;
            let Some(Native::Pending(socket)) = self.native.take() else {
                unreachable!()
            };
            self.native = Some(Native::Connected(socket.into()));
            Ok(())
        }
        #[cfg(windows)]
        {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&self.path)?;
            self.native = Some(OriginalFile::adopt(file));
            self.acquired = true;
            Ok(())
        }
        #[cfg(not(any(unix, windows)))]
        {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "local IPC unavailable on this platform",
            ))
        }
    }
    pub fn set_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        #[cfg(unix)]
        match &self.native {
            Some(Native::Connected(stream)) => {
                stream.set_read_timeout(timeout)?;
                stream.set_write_timeout(timeout)
            }
            _ => Err(unavailable()),
        }
        // Preserve the existing synchronous named-pipe timeout semantics.
        #[cfg(windows)]
        {
            let _ = timeout;
            if self.native.is_some() {
                Ok(())
            } else {
                Err(unavailable())
            }
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = timeout;
            Err(unavailable())
        }
    }
    pub fn close(&mut self) -> io::Result<OriginalLocalStreamClosed> {
        if let Some(receipt) = &self.receipt {
            return Ok(receipt.clone());
        }
        if self.acquired && self.native.is_none() && self.closing.is_none() {
            return Err(io::Error::other(
                "original local stream transfer disposition unknown",
            ));
        }
        if let Some(native) = self.native.take() {
            #[cfg(unix)]
            {
                use std::os::fd::{FromRawFd, IntoRawFd};
                let descriptor = match native {
                    Native::Pending(socket) => socket.into_raw_fd(),
                    Native::Connected(stream) => stream.into_raw_fd(),
                };
                // SAFETY: exclusive transfer of this sole original, never dup.
                let file = unsafe { std::fs::File::from_raw_fd(descriptor) };
                self.closing = Some(OriginalFile::adopt(file));
            }
            #[cfg(windows)]
            {
                self.closing = Some(native);
            }
        }
        if let Some(original) = &mut self.closing {
            let receipt = original.close()?;
            if !original.matches_closed(&receipt) {
                return Err(io::Error::other("local stream original close mismatched"));
            }
        }
        let receipt = OriginalLocalStreamClosed {
            identity: self.identity.clone(),
            native: self.acquired,
        };
        self.receipt = Some(receipt.clone());
        Ok(receipt)
    }
    pub fn matches_closed(&self, receipt: &OriginalLocalStreamClosed) -> bool {
        self.receipt.as_ref().is_some_and(|actual| {
            Arc::ptr_eq(&actual.identity, &receipt.identity) && actual.native == receipt.native
        })
    }
}
fn unavailable() -> io::Error {
    io::Error::new(
        io::ErrorKind::NotConnected,
        "original local stream unavailable or retired",
    )
}
impl Read for OriginalLocalStream {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        match &mut self.native {
            #[cfg(unix)]
            Some(Native::Connected(stream)) => stream.read(buffer),
            #[cfg(windows)]
            Some(file) => file.read(buffer),
            _ => Err(unavailable()),
        }
    }
}
impl Write for OriginalLocalStream {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        match &mut self.native {
            #[cfg(unix)]
            Some(Native::Connected(stream)) => stream.write(buffer),
            #[cfg(windows)]
            Some(file) => file.write(buffer),
            _ => Err(unavailable()),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match &mut self.native {
            #[cfg(unix)]
            Some(Native::Connected(stream)) => stream.flush(),
            #[cfg(windows)]
            Some(file) => file.flush(),
            _ => Err(unavailable()),
        }
    }
}
#[cfg(all(test, unix))]
#[path = "../tests/local_stream/original.rs"]
mod tests;
