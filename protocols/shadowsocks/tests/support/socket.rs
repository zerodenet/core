use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_traits::AsyncSocket;
pub struct Socket {
    pub input: io::Cursor<Vec<u8>>,
    pub output: Arc<Mutex<Vec<u8>>>,
    reads: usize,
    pub fragment: bool,
    pub slow: bool,
    pub fragmented_io: bool,
    read_pending: bool,
    write_pending: bool,
}
impl Socket {
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            input: io::Cursor::new(data),
            output: Default::default(),
            reads: 0,
            fragment: false,
            slow: false,
            fragmented_io: false,
            read_pending: false,
            write_pending: false,
        }
    }
}
impl AsyncSocket for Socket {
    type Error = io::Error;
    async fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        self.reads += 1;
        if self.slow {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        let n = if self.fragment && self.reads == 2 {
            1
        } else {
            b.len()
        };
        io::Read::read(&mut self.input, &mut b[..n])
    }
    async fn write_all(&mut self, b: &[u8]) -> io::Result<()> {
        self.output.lock().unwrap().extend_from_slice(b);
        Ok(())
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl AsyncRead for Socket {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        b: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.fragmented_io && self.read_pending {
            self.read_pending = false;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        self.read_pending = true;
        let size = if self.fragmented_io {
            b.remaining().min(1)
        } else {
            b.remaining()
        };
        let mut bytes = vec![0; size];
        let n = io::Read::read(&mut self.input, &mut bytes)?;
        b.put_slice(&bytes[..n]);
        Poll::Ready(Ok(()))
    }
}
impl AsyncWrite for Socket {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        b: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.fragmented_io && self.write_pending {
            self.write_pending = false;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        self.write_pending = true;
        let size = if self.fragmented_io {
            b.len().min(1)
        } else {
            b.len()
        };
        self.output.lock().unwrap().extend_from_slice(&b[..size]);
        Poll::Ready(Ok(size))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
